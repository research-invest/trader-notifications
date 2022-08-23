package main

//GOOS=linux GOARCH=amd64 go build -o ./notifications -a

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/go-pg/pg/v10"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/olekukonko/tablewriter"
	"github.com/sirupsen/logrus"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
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

	go func() {
		for {
			t := time.Now()
			if t.Hour() == 10 {
				sendConsolidationPeriod()
				time.Sleep(24 * time.Hour)
			}

			time.Sleep(30 * time.Second)
		}
	}()

	//sendNotifications() // mutex если в данный момент еще в работе

	for {
		t := time.Now()

		if t.Hour() >= 2 && t.Hour() < 7 {
			time.Sleep(1 * time.Hour) // temp
		}

		if t.Minute() == 0 || t.Minute() == 30 {
			sendNotifications() // mutex если в данный момент еще в работе
		}

		time.Sleep(45 * time.Second)
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

	var replyMarkup = tgbotapi.NewReplyKeyboard(
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Btc ❤️"),
			tgbotapi.NewKeyboardButton("Btc ❤️ 10m"),
			tgbotapi.NewKeyboardButton("Btc ❤️ 1H"),
		),
		tgbotapi.NewKeyboardButtonRow(
			tgbotapi.NewKeyboardButton("Есь че? 😘"),
		),
	)

	bot.Debug = false //!!!!

	log.Printf("Authorized on account %s", bot.Self.UserName)

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60

	updates := bot.GetUpdatesChan(u)

	for update := range updates {
		if update.Message == nil { // ignore any non-Message updates
			continue
		}

		sub := Subscriber{}
		subscriber, err := sub.addNew(update.Message.Chat) //subscriber

		if err != nil {
			fmt.Printf("can't add a new file db record : %v\n", err)
			log.Warnf("can't subscriber create : %v", err)
			continue
		}

		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "")
		msg.ParseMode = "MarkdownV2"
		msg.ReplyMarkup = replyMarkup

		if update.Message.IsCommand() { // ignore any non-command Messages

			//report - Report
			//status - Status

			// Extract the command from the Message.
			switch update.Message.Command() {
			case "start":
				msg.Text = "Привет " + update.Message.Chat.FirstName + " я буду присылать тебе уведомления о движениях монет"
			case "status":
				msg.Text = "I m ok"
			default:
				msg.Text = "I don't know that command"
			}
		}

		switch update.Message.Text {
		case "Btc ❤️":
			msg.Text = ""
			sendCoinGraph(subscriber.TelegramId, "BTC", "")
		case "Btc ❤️ 10m":
			msg.Text = ""
			sendCoinGraph(subscriber.TelegramId, "BTC", "10m")
		case "Btc ❤️ 1H":
			msg.Text = ""
			sendCoinGraph(subscriber.TelegramId, "BTC", "1H")
		case "Есь че? 😘":
			msg.Text = "```" + getNotificationText() + "```"
		default:
			rate, err := getActualExchangeRate(update.Message.Text)
			if err == nil {
				msg.Text = "```" + rate + "```"
			} else {
				msg.Text = err.Error()
			}

			if rate != "" {
				coin := strings.ToUpper(strings.TrimSpace(update.Message.Text))
				coin = strings.Replace(coin, "?", "", 100)
				sendCoinGraph(subscriber.TelegramId, coin, "1H")
			}
		}

		if _, err := bot.Send(msg); err != nil {
			log.Warnf("can't send bot message telegramBot: %v", err)
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
		log.Warn(err)
	}
}

func getPercentCoins(coins *[]PercentCoinShort) (err error) {
	_, err = dbConnect.Query(coins, `

WITH coin_pairs_24_hours AS (
    SELECT k.coin_pair_id, c.id as coin_id, c.code, k.open, k.close, k.high, k.low, k.open_time, k.close_time, c.rank
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.couple = 'BUSD' AND c.is_enabled = 1 AND cp.is_enabled = 1 AND k.open_time >= NOW() - INTERVAL '1 DAY'
    ORDER BY c.rank
)

SELECT *
FROM (
         SELECT t.*,
                ROUND(CAST((COALESCE(t.minute10, 0) + COALESCE(t.hour, 0) +
                            COALESCE(t.hour4, 0) + COALESCE(t.hour12, 0) +
                            COALESCE(t.hour24, 0)) AS NUMERIC), 3) AS percent_sum
         FROM (

                           SELECT DISTINCT ON (t.coin_id) t.coin_id,
                                                          t.code,
                                                          t.rank,
                                                          CAlC_PERCENT(MIN(COALESCE(minute10.first_open, 0)), MIN(COALESCE(minute10.last_close, 0))) AS minute10,
                                                          CAlC_PERCENT(MIN(COALESCE(hour.first_open, 0)), MIN(COALESCE(hour.last_close, 0)))         AS hour,
                                                          CAlC_PERCENT(MIN(COALESCE(hour4.first_open, 0)), MIN(COALESCE(hour4.last_close, 0)))       AS hour4,
                                                          CAlC_PERCENT(MIN(COALESCE(hour12.first_open, 0)), MIN(COALESCE(hour12.last_close, 0)))     AS hour12,
                                                          CAlC_PERCENT(MIN(COALESCE(hour24.first_open, 0)), MIN(COALESCE(hour24.last_close, 0)))     AS hour24

                           FROM coin_pairs_24_hours AS t
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               WHERE t.open_time >= date_round_down(NOW() - interval '10 MINUTE', '10 MINUTE')
                                  OR (t.open_time <= date_round_down(NOW() - interval '10 MINUTE', '10 MINUTE') AND
                                      t.close_time >= NOW())
                               GROUP BY t.coin_pair_id
                           ) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               WHERE t.open_time >= date_round_down(NOW() - interval '1 HOUR', '1 HOUR')
                               GROUP BY t.coin_pair_id
                           ) as hour ON t.coin_pair_id = hour.coin_pair_id
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               WHERE t.open_time >= date_round_down(NOW() - interval '4 HOUR', '1 HOUR')
                               GROUP BY t.coin_pair_id
                           ) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               WHERE t.open_time >= date_round_down(NOW() - interval '12 HOUR', '1 HOUR')
                               GROUP BY t.coin_pair_id
                           ) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
                                    LEFT JOIN (
                               SELECT t.coin_pair_id,
                                      MIN(t.open)                                       AS min_open,
                                      MAX(t.close)                                      AS max_close,
                                      (array_agg(t.open order by t.open_time asc))[1]   as first_open,
                                      (array_agg(t.close order by t.open_time desc))[1] as last_close
                               FROM coin_pairs_24_hours AS t
                               GROUP BY t.coin_pair_id
                           ) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
                           GROUP BY t.coin_id, t.code, t.rank
                           ORDER BY t.coin_id
                           LIMIT 45
                       ) AS t
                        WHERE (
                                (t.minute10 >= 2 OR t.minute10 <= -2)
                                OR (t.hour >= 3 OR t.hour <= -3)
                                OR (t.hour4 >= 4 OR t.hour4 <= -4)
                                OR (t.hour12 >= 8 OR t.hour12 <= -8)
                                OR (t.hour24 >= 10 OR t.hour24 <= -10))
     ) AS t
WHERE percent_sum >= 2
ORDER BY percent_sum DESC;
`)

	if err != nil {
		log.Error("can't get percent pairs: %v", err)
		return err
	}

	return nil
}

func getNotificationText() string {

	var coins []PercentCoinShort
	err := getPercentCoins(&coins)

	if err != nil {
		return "Возникла ошибка №435/1"
	}

	countCoins := len(coins)

	if countCoins == 0 {
		return ""
	}

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Name", "10m", "1h", "4h", "12h", "24h"})
	table.SetCaption(true, "Coins.")

	for _, coin := range coins {
		table.Append([]string{
			coin.Code + " [" + IntToStr(coin.Rank) + "]",
			FloatToStr(coin.Minute10),
			FloatToStr(coin.Hour),
			FloatToStr(coin.Hour4),
			FloatToStr(coin.Hour12),
			FloatToStr(coin.Hour24),
		})
	}

	table.Render()

	return tableString.String()
}

func sendNotifications() {
	if sendNotificationsIsWorking == true {
		return
	}

	fmt.Println("Send notifications start work")

	notificationText := getNotificationText()

	if notificationText == "" {
		fmt.Println("countCoins is zero")
		sendNotificationsIsWorking = false
		return
	}

	sendNotificationsIsWorking = true

	var subscribers []Subscriber
	err := dbConnect.Model(&subscribers).
		Where("is_enabled = ?", 1).
		Select()

	if err != nil {
		log.Warn("can't get subscribers: %v", err)
		sendNotificationsIsWorking = false
		return
	}

	bot, err := tgbotapi.NewBotAPI(appConfig.TelegramBot)
	if err != nil {
		log.Warn(err)
		return
	}

	defer func() {
		subscribers = nil
		bot = nil
	}()

	bot.Debug = false //!!!!

	for _, subscriber := range subscribers {
		msg := tgbotapi.NewMessage(subscriber.TelegramId, "```"+notificationText+"```")
		msg.ParseMode = "MarkdownV2"
		if _, err := bot.Send(msg); err != nil {
			if strings.Contains(err.Error(), "Forbidden: bot was blocked by the user") { // to const error text
				err := subscriber.enabledFalse()
				if err != nil {
					log.Warnf("Error disable subscriber: %v", err)
					continue
				}
			} else {
				log.Error(err)
			}
		}

		sendCoinGraph(subscriber.TelegramId, "", "")
	}

	sendNotificationsIsWorking = false
}

func getConsolidationPeriodCoins(coins *[]ConsolidationPeriodCoin) (err error) {
	_, err = dbConnect.Query(coins, `

WITH coins_last_prices AS (
    SELECT DISTINCT ON (k.coin_pair_id) k.coin_pair_id, c.id AS coin_id, k.close
    FROM klines AS k
             INNER JOIN coins_pairs AS cp ON cp.id = k.coin_pair_id
             INNER JOIN coins AS c ON c.id = cp.coin_id
    WHERE cp.couple = 'BUSD'
      AND c.is_enabled = 1
      AND cp.is_enabled = 1
    ORDER BY k.coin_pair_id, k.close_time DESC
)

SELECT t.*
FROM (
         SELECT c.id AS coin_id, c.code, c.rank, avg(k.open) AS avg_open, avg(k.close) AS avg_close,
                clp.close AS price, CAlC_PERCENT(avg(k.open),clp.close) AS percent_open,
                 CAlC_PERCENT(avg(k.close),clp.close) AS percent_close --array_agg(k.open) AS opens,
         FROM coins AS c
                  INNER JOIN coins_pairs cp on cp.coin_id = c.id
                  LEFT JOIN (
             SELECT date_trunc('day', k.open_time) AS day,
                    k.coin_pair_id,
                    AVG(k.open)                    AS open,
                    AVG(k.close)                   AS close
             FROM klines AS k
             WHERE k.open_time >= date_round_down(NOW() - interval '14 DAY', '1 HOUR')
             GROUP BY day, k.coin_pair_id
             ORDER BY day DESC
         ) AS k on cp.id = k.coin_pair_id
			LEFT JOIN coins_last_prices AS clp ON clp.coin_id = c.id
         WHERE c.is_enabled = 1 AND cp.is_enabled = 1
         GROUP BY c.id, c.code, clp.close
     ) AS t
WHERE (percent_open >=-3 AND percent_open <= 5) AND (percent_close >=-3 AND percent_close <= 5);
`)

	if err != nil {
		log.Error("can't get consolidation period: %v", err)
		return err
	}

	return nil
}

func getConsolidationPeriodText() string {

	var coins []ConsolidationPeriodCoin
	err := getConsolidationPeriodCoins(&coins)

	if err != nil {
		return "Возникла ошибка №435/2"
	}

	countCoins := len(coins)

	if countCoins == 0 {
		return ""
	}

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Name", "Avg open", "Avg close", "Price"})
	table.SetCaption(true, "Coins in period consolidation")

	for _, coin := range coins {
		table.Append([]string{
			coin.Code, // + " [" + IntToStr(coin.Rank) + "]"
			FloatToStr(coin.AvgOpen),
			FloatToStr(coin.AvgClose),
			FloatToStr(coin.Price),
		})
	}

	table.Render()

	result := tableString.String()

	if len(result) > 4000 {
		return result[:4000]
	}

	return result
}

func sendConsolidationPeriod() {

	fmt.Println("Send consolidationPeriod start work")

	notificationText := getConsolidationPeriodText()

	if notificationText == "" {
		fmt.Println("countCoins is zero")
		return
	}

	var subscribers []Subscriber
	err := dbConnect.Model(&subscribers).
		Where("is_enabled = ?", 1).
		Select()

	if err != nil {
		log.Warn("can't get subscribers: %v", err)
		return
	}

	bot, err := tgbotapi.NewBotAPI(appConfig.TelegramBot)
	if err != nil {
		log.Warn(err)
		return
	}

	bot.Debug = false //!!!!

	for _, subscriber := range subscribers {
		msg := tgbotapi.NewMessage(subscriber.TelegramId, "```"+notificationText+"```")
		msg.ParseMode = "MarkdownV2"
		if _, err := bot.Send(msg); err != nil {
			if strings.Contains(err.Error(), "Forbidden: bot was blocked by the user") { // to const error text
				err := subscriber.enabledFalse()
				if err != nil {
					log.Warnf("Error disable subscriber: %v", err)
					continue
				}
			} else {
				log.Error(err)
				msg := tgbotapi.NewMessage(subscriber.TelegramId, err.Error())
				bot.Send(msg)
			}
		}
	}
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
      AND k.open_time >= NOW() - INTERVAL '1 DAY'
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
	WHERE t.open_time >= date_round_down(NOW() - interval '10 MINUTE', '10 MINUTE')
    GROUP BY t.coin_pair_id
) as minute10 ON t.coin_pair_id = minute10.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW() - interval '1 HOUR', '1 HOUR')
    GROUP BY t.coin_pair_id
) as hour ON t.coin_pair_id = hour.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW() - interval '4 HOUR', '1 HOUR')
    GROUP BY t.coin_pair_id
) as hour4 ON t.coin_pair_id = hour4.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
	WHERE t.open_time >= date_round_down(NOW() - interval '12 HOUR', '1 HOUR')
    GROUP BY t.coin_pair_id
) as hour12 ON t.coin_pair_id = hour12.coin_pair_id
         LEFT JOIN (
    SELECT t.coin_pair_id,
           MIN(t.open) AS min_open,
           MAX(t.close) AS max_close,
           CAlC_PERCENT(MIN(t.open), MAX(t.close)) AS percent
    FROM coin_pairs_24_hours AS t
    GROUP BY t.coin_pair_id
) AS hour24 ON t.coin_pair_id = hour24.coin_pair_id
`, coin)

	if err != nil {
		log.Warn("can't get get actual exchange rate: %v", err)
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

func getDataForCoinGraph(coin string, typeInterval string) ([]time.Time, []float64, []float64) {
	var times []time.Time
	var closes, volumes []float64
	var klines []Kline

	if coin == "" {
		coin = "BTC"
	}

	var interval string

	switch typeInterval {
	case "4H":
	case "":
		//default:
		interval = "4 HOUR"
	case "10m":
		interval = "10 MINUTE"
	case "1H":
		interval = "1 HOUR"
	}

	res, err := dbConnect.Query(&klines, `
SELECT klines.*
FROM klines
INNER JOIN coins_pairs cp on klines.coin_pair_id = cp.id
INNER JOIN coins c on c.id = cp.coin_id
WHERE open_time >= (NOW() - interval '`+interval+`')
 AND c.code = ?
ORDER BY id ASC;
`, coin)

	if err != nil {
		log.Warn("can't get getDataForCoinGraph: %v", err)
		return nil, nil, nil
	}

	if res.RowsAffected() == 0 {
		return nil, nil, nil
	}

	for _, kline := range klines {
		times = append(times, kline.OpenTime)
		closes = append(closes, kline.Close)
		volumes = append(volumes, kline.QuoteAssetVolume)
	}

	return times, closes, volumes
}

func sendCoinGraph(telegramId int64, coin string, interval string) {
	var subscribers []Subscriber
	var query = dbConnect.Model(&subscribers).
		Where("is_enabled = ?", 1)

	if telegramId > 0 {
		query.Where("telegram_id = ?", telegramId)
	}

	err := query.Select()

	if err != nil {
		log.Warnf("can't get subscribers by get sendCoinGraph: %v", err)
		return
	}

	bot, err := tgbotapi.NewBotAPI(appConfig.TelegramBot)
	if err != nil {
		log.Warn(err)
		return
	}

	defer func() {
		subscribers = nil
		bot = nil
	}()

	bot.Debug = false //!!!!

	if coin == "" {
		coin = "BTC"
	}

	xv, yv, _ := getDataForCoinGraph(coin, interval)

	if len(xv) == 0 {
		return
	}

	if interval == "" {
		interval = "4H"
	}

	priceSeries := chart.TimeSeries{
		Name: coin + " " + interval,
		Style: chart.Style{
			Show:        true,
			StrokeColor: chart.GetDefaultColor(0),
		},
		XValues: xv,
		YValues: yv,
	}

	smaSeries := chart.SMASeries{ // красная линия
		Name: coin + " - SMA",
		Style: chart.Style{
			Show:            true,
			StrokeColor:     drawing.ColorRed,
			StrokeDashArray: []float64{5.0, 5.0},
		},
		InnerSeries: priceSeries,
	}

	bbSeries := &chart.BollingerBandsSeries{ //фоновый
		Name: coin + " - Bol. Bands",
		Style: chart.Style{
			Show:        true,
			StrokeColor: drawing.ColorFromHex("efefef"),
			FillColor:   drawing.ColorFromHex("efefef"), //.WithAlpha(100)
		},
		InnerSeries: priceSeries,
	}

	min, max := findMinAndMax(yv)

	graph := chart.Chart{
		XAxis: chart.XAxis{
			Style:        chart.Style{Show: true},
			TickPosition: chart.TickPositionBetweenTicks,
		},
		YAxis: chart.YAxis{
			Style: chart.Style{Show: true},
			Range: &chart.ContinuousRange{
				Max: max,
				Min: min,
			},
		},
		Series: []chart.Series{
			bbSeries,
			priceSeries,
			smaSeries,
		},
	}

	//----

	graph.Elements = []chart.Renderable{
		chart.Legend(&graph),
	}

	buffer := bytes.NewBuffer([]byte{})
	err = graph.Render(chart.PNG, buffer)

	for _, subscriber := range subscribers {

		photoFileBytes := tgbotapi.FileBytes{
			Name:  "picture",
			Bytes: buffer.Bytes(),
		}

		photo := tgbotapi.NewPhoto(subscriber.TelegramId, photoFileBytes)

		if _, err := bot.Send(photo); err != nil {
			if strings.Contains(err.Error(), "Forbidden: bot was blocked by the user") { // todo to const error text
				err := subscriber.enabledFalse()
				if err != nil {
					log.Warnf("Error disable subscriber: %v", err)
					continue
				}
			} else {
				log.Error(err)
			}
		}
	}
}
