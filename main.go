package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-pg/pg/v10"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/olekukonko/tablewriter"
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

	sendNotifications() // mutex если в данный момент еще в работе

	for {
		t := time.Now()

		if t.Hour() >= 2 && t.Hour() < 7 {
			time.Sleep(1 * time.Hour) // temp
		}

		if t.Second() == 0 {
			sendNotifications() // mutex если в данный момент еще в работе
			time.Sleep(30 * time.Minute)
		}
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
			msg.Text = rate
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

func getPercentCoins(coins *[]PercentCoinShort) (err error) {
	_, err = dbConnect.Query(coins, `

WITH coin_pairs_24_hours AS (
    SELECT k.coin_pair_id,
           c.id as coin_id,
           c.code,
           k.open,
           k.close,
           k.high,
           k.low,
           k.open_time,
           k.close_time,
           c.rank
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.couple = 'BUSD'
      AND c.is_enabled = 1
      AND cp.is_enabled = 1
      AND k.open_time >= NOW() - INTERVAL '1 DAY'
    ORDER BY c.rank
)

SELECT t.*
FROM (
         SELECT DISTINCT ON (t.coin_id) t.coin_id,
                                        t.code,
                                        t.rank,
                                        minute10.percent AS minute10,
                                        hour.percent     AS hour,
                                        hour4.percent    AS hour4,
                                        hour12.percent   AS hour12,
                                        hour24.percent   AS hour24
         FROM coin_pairs_24_hours AS t
                  LEFT JOIN (
             SELECT t.coin_pair_id,
                    MIN(t.open)                             AS min_open,
                    MAX(t.close)                            AS max_close,
                    CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
             FROM coin_pairs_24_hours AS t
             WHERE t.open_time >= date_round_down(NOW(), '10 MINUTE')
             GROUP BY t.coin_pair_id
         ) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
                  LEFT JOIN (
             SELECT t.coin_pair_id,
                    MIN(t.open)                             AS min_open,
                    MAX(t.close)                            AS max_close,
                    CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
             FROM coin_pairs_24_hours AS t
             WHERE t.open_time >= date_round_down(NOW(), '1 HOUR')
             GROUP BY t.coin_pair_id
         ) as hour ON t.coin_pair_id = hour.coin_pair_id
                  LEFT JOIN (
             SELECT t.coin_pair_id,
                    MIN(t.open)                             AS min_open,
                    MAX(t.close)                            AS max_close,
                    CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
             FROM coin_pairs_24_hours AS t
             WHERE t.open_time >= date_round_down(NOW(), '4 HOUR')
             GROUP BY t.coin_pair_id
         ) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
                  LEFT JOIN (
             SELECT t.coin_pair_id,
                    MIN(t.open)                             AS min_open,
                    MAX(t.close)                            AS max_close,
                    CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
             FROM coin_pairs_24_hours AS t
             WHERE t.open_time >= date_round_down(NOW(), '12 HOUR')
             GROUP BY t.coin_pair_id
         ) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
                  LEFT JOIN (
             SELECT t.coin_pair_id,
                    MIN(t.open)                             AS min_open,
                    MAX(t.close)                            AS max_close,
                    CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
             FROM coin_pairs_24_hours AS t
             GROUP BY t.coin_pair_id
         ) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
         WHERE (
                       (minute10.percent >= 2 OR minute10.percent <= -2)
                       OR (hour.percent >= 3 OR hour.percent <= -3)
                       OR (hour4.percent >= 4 OR hour4.percent <= -4)
                       OR (hour12.percent >= 8 OR hour12.percent <= -8)
                       OR (hour24.percent >= 10 OR hour24.percent <= -10))
         ORDER BY t.coin_id
         LIMIT 45
     ) AS t
ORDER BY t.rank ASC;
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

	fmt.Println("Send notifications start work")

	var coins []PercentCoinShort
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

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Name", "Rank", "10m", "1h", "4h", "12h", "24h"})

	for _, coin := range coins {
		table.Append([]string{
			coin.Code,
			IntToStr(coin.Rank),
			FloatToStr(coin.Minute10),
			FloatToStr(coin.Hour),
			FloatToStr(coin.Hour4),
			FloatToStr(coin.Hour12),
			FloatToStr(coin.Hour24),
		})
	}

	table.Render()

	for _, subscriber := range subscribers {
		msg := tgbotapi.NewMessage(subscriber.TelegramId, tableString.String())
		if _, err := bot.Send(msg); err != nil {
			log.Panic(err)
		}
	}

	sendNotificationsIsWorking = false
}

func getActualExchangeRate(message string) (string, error) {
	message = strings.ToUpper(strings.TrimSpace(message))

	var rate PercentCoin

	if !strings.Contains(message, "?") {
		return "", errors.New("no correct coin")
	}

	coin := strings.Replace(message, "?", "", 100)

	if len(coin) >= 10 {
		return "", errors.New("no correct coin")
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
      AND k.open_time >= date_round_down(now(), '1 DAY')
      AND c.code = ?
)

SELECT DISTINCT ON (t.coin_id) t.coin_id,
                               t.code,
                               t.rank,
                               minute10.percent AS minute10,
                               hour.percent     AS hour,
                               hour4.percent    AS hour4,
                               hour12.percent   AS hour12,
                               hour24.percent   AS hour24,
                               minute10.min_open   AS minute10_min_open,
                               minute10.max_close   AS minute10_max_close,
                               hour.min_open   AS hour_min_open,
                               hour.max_close   AS hour_max_close,
                               hour4.min_open   AS hour4_min_open,
                               hour4.max_close   AS hour4_max_close,
                               hour12.min_open   AS hour12_min_open,
                               hour12.max_close   AS hour12_max_close,
                               hour24.min_open   AS hour24_min_open,
                               hour24.max_close   AS hour24_max_close
FROM coin_pairs_24_hours AS t
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW(), '10 MINUTE')
    GROUP BY t.coin_pair_id
) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW(), '1 HOUR')
    GROUP BY t.coin_pair_id
) as hour ON t.coin_pair_id = hour.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW(), '4 HOUR')
    GROUP BY t.coin_pair_id
) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW(), '12 HOUR')
    GROUP BY t.coin_pair_id
) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW(), '1 DAY')
    GROUP BY t.coin_pair_id
) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
`, coin)

	if err != nil {
		log.Panic("can't get get actual exchange rate: %v", err)
		return "", err
	}

	if res.RowsAffected() == 0 {
		return "", errors.New("coin not found")
	}

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Name", "Value"})

	table.Append([]string{"Coin id", IntToStr(int(rate.CoinId))})
	table.Append([]string{"Coin", rate.Code})
	table.Append([]string{"Rank", IntToStr(rate.Rank)})
	table.Append([]string{"10 Minute", FloatToStr(rate.Minute10)})
	table.Append([]string{"Hour", FloatToStr(rate.Hour)})
	table.Append([]string{"4 Hour", FloatToStr(rate.Hour4)})
	table.Append([]string{"12 Hour", FloatToStr(rate.Hour12)})
	table.Append([]string{"24 Hour", FloatToStr(rate.Hour24)})
	table.Append([]string{"10 Min open", FloatToStr(rate.Minute10MinOpen)})
	table.Append([]string{"10 Max close", FloatToStr(rate.Minute10MaxClose)})
	table.Append([]string{"Hour min open", FloatToStr(rate.HourMinOpen)})
	table.Append([]string{"Hour max close", FloatToStr(rate.HourMaxClose)})
	table.Append([]string{"4 Hour min open", FloatToStr(rate.Hour4MinOpen)})
	table.Append([]string{"4 Hour max close", FloatToStr(rate.Hour4MaxClose)})
	table.Append([]string{"12 Hour open", FloatToStr(rate.Hour12MinOpen)})
	table.Append([]string{"12 Hour max close", FloatToStr(rate.Hour12MaxClose)})
	table.Append([]string{"24 Hour min open", FloatToStr(rate.Hour24MinOpen)})
	table.Append([]string{"24 Hour max close", FloatToStr(rate.Hour24MaxClose)})

	table.Render()

	return tableString.String(), nil
}
