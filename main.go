package main

//GOOS=linux GOARCH=amd64 go build -o ./notifications -a

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-pg/pg/v10"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
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

		if !update.Message.IsCommand() { // ignore any non-command Messages
			continue
		}

		sub := Subscriber{}
		_, err := sub.addNew(update.Message.Chat) //subscriber

		if err != nil {
			fmt.Printf("can't add a new file db record : %v\n", err)
			log.Warnf("can't subscriber create : %v", err)
		}

		//report - Report
		//status - Status

		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "")

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
    SELECT k.coin_pair_id,
           c.id as coin_id,
           c.code,
           k.open,
           k.close,
           k.close_time,
           c.rank
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
                               hour.min_value   AS hour_min_value,
                               hour.max_value   AS hour_max_value,
                               hour4.min_value  AS hour4_min_value,
                               hour4.max_value  AS hour4_max_value,
                               hour12.min_value AS hour12_min_value,
                               hour12.max_value AS hour12_max_value,
                               hour24.min_value AS hour24_min_value,
                               hour24.max_value AS hour24_max_value
FROM coin_pairs_24_hours as t
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.close) AS max_value,
           MIN(t.open) AS min_value,
           ROUND(CAST(((MIN(t.open) - MAX(t.close)) / MAX(t.close)) * 100 AS NUMERIC), 3) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.close_time >= NOW() - INTERVAL '10 MINUTE'
    GROUP BY t.coin_pair_id
) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.close) AS max_value,
           MIN(t.open) AS min_value,
           ROUND(CAST(((MIN(t.open) - MAX(t.close)) / MAX(t.close)) * 100 AS NUMERIC), 3) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.close_time >= NOW() - INTERVAL '1 HOUR'
    GROUP BY t.coin_pair_id
) as hour ON t.coin_pair_id = hour.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.close) AS max_value,
           MIN(t.open) AS min_value,
           ROUND(CAST(((MIN(t.open) - MAX(t.close)) / MAX(t.close)) * 100 AS NUMERIC), 3) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.close_time >= NOW() - INTERVAL '4 HOUR'
    GROUP BY t.coin_pair_id
) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.close) AS max_value,
           MIN(t.open) AS min_value,
           ROUND(CAST(((MIN(t.open) - MAX(t.close)) / MAX(t.close)) * 100 AS NUMERIC), 3) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.close_time >= NOW() - INTERVAL '12 HOUR'
    GROUP BY t.coin_pair_id
) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MAX(t.close) AS max_value,
           MIN(t.open) AS min_value,
           ROUND(CAST(((MIN(t.open) - MAX(t.close)) / MAX(t.close)) * 100 AS NUMERIC), 3) AS percent
    FROM coin_pairs_24_hours AS t
    WHERE t.close_time >= NOW() - INTERVAL '1 DAY'
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
