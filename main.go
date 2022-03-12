package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-pg/pg/v10"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"
)

var dbConnect pg.DB

var log = logrus.New()

var sendNotificationsIsWorking bool

func main() {
	setLogParam()
	readConfig()

	dbInit()

	defer func() {
		err := dbConnect.Close()
		if err != nil {
			fmt.Printf("Error close postgres connection: %v\n", err)
			log.Fatalf("dbConnect.Close fatal error : %v", err)
		}
	}()

	go func() {
		telegramBot()
	}()

	for {
		sendNotifications() // mutex если в данный момент еще в работе
		time.Sleep(10 * time.Minute)
	}
}

func setLogParam() {
	log.Out = os.Stdout

	file, err := os.OpenFile("./logs/logrus.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		log.Out = file
	} else {
		log.Info("Failed to log to file, using default stderr")
	}
}

func telegramBot() {
	bot, err := tgbotapi.NewBotAPI(appConfig.TelegramBot)
	if err != nil {
		log.Panic(err)
	}

	bot.Debug = false //!!!!

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates, _ := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil { // ignore any non-Message updates
			continue
		}

		sub := Subscriber{}
		_, err := sub.addNew(update.Message.Chat) //subscriber

		if err != nil {
			fmt.Printf("can't add a new file db record : %v\n", err)
			log.Warnf("can't subscriber create : %v", err)
			continue
		}

		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "")

		if update.Message.IsCommand() { // ignore any non-command Messages

			//report - Report
			//status - Status

			// Extract the command from the Message.
			switch update.Message.Command() {
			case "report":
				msg.Text = "report"
			case "status":
				msg.Text = "I'm ok."
			default:
				msg.Text = "I don't know that command"
			}

			if _, err := bot.Send(msg); err != nil {
				log.Panic(err)
			}

			continue
		}

		rate, err := getActualExchangeRate(update.Message.Text)

		if err == nil {
			s, _ := json.MarshalIndent(rate, "", "\t")
			msg.Text = string(s)
			if _, err := bot.Send(msg); err != nil {
				log.Warnf("can't send bot message getActualExchangeRate: %v", err)
			}
		} else {
			msg.Text = err.Error()
			if _, err := bot.Send(msg); err != nil {
				log.Warnf("can't send bot message getActualExchangeRate: %v", err)
			}
		}

	}
}

func dbInit() {
	dbConnect = *pg.Connect(&pg.Options{
		Addr:     appConfig.Db.Host + ":" + strconv.Itoa(appConfig.Db.Port),
		User:     appConfig.Db.User,
		Password: appConfig.Db.Pass,
		Database: appConfig.Db.Dbname,
	})

	ctx := context.Background()

	_, err := dbConnect.ExecContext(ctx, "SELECT 1; SET timezone = 'UTC';")
	if err != nil {
		log.Panic(err)
		panic(err)
	}
}

func getPercentCoins(coins *[]PercentCoin) (err error) {
	_, err = dbConnect.Query(coins, `

WITH coin_pairs_24_hours AS (
    SELECT k.coin_pair_id, c.id as coin_id, c.code, k.open, k.close, k.high, k.low, k.close_time, k.open_time, c.rank
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.couple = 'BUSD'
      AND c.is_enabled = 1
      AND cp.is_enabled = 1
      AND k.close_time >= NOW() - INTERVAL '1 DAY'
    ORDER BY c.rank
)

SELECT DISTINCT ON (t.coin_id) t.coin_id,
                               t.code,
                               minute10.percent AS minute10,
                               hour.percent     AS hour,
                               hour4.percent    AS hour4,
                               hour12.percent   AS hour12,
                               hour24.percent   AS hour24,
                               minute10.max_open   AS minute10_max_open,
                               minute10.max_close   AS minute10_max_close,
                               hour.max_open   AS hour_max_open,
                               hour.max_close   AS hour_max_close,
                               hour4.max_open   AS hour4_max_open,
                               hour4.max_close   AS hour4_max_close,
                               hour12.max_open   AS hour12_max_open,
                               hour12.max_close   AS hour12_max_close,
                               hour24.max_open   AS hour24_max_open,
                               hour24.max_close   AS hour24_max_close
FROM coin_pairs_24_hours AS t
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '10 MINUTE' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '1 HOUR' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour ON t.coin_pair_id = hour.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '4 HOUR' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '12 HOUR' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '1 DAY' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
WHERE (hour.percent >= 2 OR hour.percent <= -2)
   OR (hour4.percent >= 2 OR hour4.percent <= -2)
   OR (hour12.percent >= 2 OR hour12.percent <= -2)
   OR (hour24.percent >= 2 OR hour24.percent <= -2)
ORDER BY t.coin_id, t.rank
LIMIT 10;
`)

	if err != nil {
		log.Panic("can't get percent pairs: %v", err)
		return err
	}

	return nil
}

func sendNotifications() {
	if sendNotificationsIsWorking == true {
		return
	}

	var coins []PercentCoin
	err := getPercentCoins(&coins)

	if err != nil {
		panic(err)
	}

	countCoins := len(coins)

	if countCoins == 0 {
		fmt.Println("countCoins is zero")
		sendNotificationsIsWorking = false
		return
	}

	sendNotificationsIsWorking = true

	var subscribers []Subscriber
	err = dbConnect.Model(&subscribers).
		Where("is_enabled = ?", 1).
		Select()

	if err != nil {
		log.Panic("can't get subscribers: %v", err)
		panic(err)
	}

	bot, err := tgbotapi.NewBotAPI(appConfig.TelegramBot)
	if err != nil {
		log.Panic(err)
	}

	bot.Debug = false //!!!!

	for _, subscriber := range subscribers {
		s, _ := json.MarshalIndent(coins, "", "\t")

		msg := tgbotapi.NewMessage(subscriber.TelegramId, string(s))
		if _, err := bot.Send(msg); err != nil {
			log.Panic(err)
		}
	}

	sendNotificationsIsWorking = false
}

func getActualExchangeRate(message string) (PercentCoin, error) {
	message = strings.ToUpper(strings.TrimSpace(message))

	var rate PercentCoin

	if !strings.Contains(message, "?") {
		return rate, errors.New("no correct coin")
	}

	coin := strings.Replace(message, "?", "", 100)

	if len(coin) >= 10 {
		return rate, errors.New("no correct coin")
	}

	res, err := dbConnect.Query(&rate, `

WITH coin_pairs_24_hours AS (
    SELECT k.coin_pair_id, c.id as coin_id, c.code, k.open, k.close, k.high, k.low, k.close_time, k.open_time, c.rank
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.couple = 'BUSD'
      AND c.is_enabled = 1
      AND cp.is_enabled = 1
      AND k.close_time >= NOW() - INTERVAL '1 DAY'
      AND c.code = ?
)

SELECT DISTINCT ON (t.coin_id) t.coin_id,
                               t.code,
                               minute10.percent AS minute10,
                               hour.percent     AS hour,
                               hour4.percent    AS hour4,
                               hour12.percent   AS hour12,
                               hour24.percent   AS hour24,
                               minute10.max_open   AS minute10_max_open,
                               minute10.max_close   AS minute10_max_close,
                               hour.max_open   AS hour_max_open,
                               hour.max_close   AS hour_max_close,
                               hour4.max_open   AS hour4_max_open,
                               hour4.max_close   AS hour4_max_close,
                               hour12.max_open   AS hour12_max_open,
                               hour12.max_close   AS hour12_max_close,
                               hour24.max_open   AS hour24_max_open,
                               hour24.max_close   AS hour24_max_close
FROM coin_pairs_24_hours AS t
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '10 MINUTE' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '1 HOUR' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour ON t.coin_pair_id = hour.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '4 HOUR' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '12 HOUR' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.open) AS max_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MAX(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.open_time >= NOW() - INTERVAL '1 DAY' AND t.close_time <= NOW()
    GROUP BY t.coin_pair_id
) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
`, coin)

	if err != nil {
		log.Panic("can't get get actual exchange rate: %v", err)
		return rate, err
	}

	if res.RowsAffected() == 0 {
		return rate, errors.New("coin not found")
	}

	return rate, nil
}
