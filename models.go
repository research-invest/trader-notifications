package main

import (
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api"
	"time"
)

const (
	Subscriber_IS_ENABLED_TRUE  = 1
	Subscriber_IS_ENABLED_FALSE = 0
)

type Subscriber struct {
	tableName struct{} `pg:"notifications_subscribers"`

	Id                int64
	IsEnabled         int8   `pg:",is_enabled"`
	TelegramId        int64  `pg:",telegram_id"`
	TelegramFirstName string `pg:",telegram_first_name"`
	TelegramLastName  string `pg:",telegram_last_name"`
	TelegramUsername  string `pg:",telegram_username"`
	Email             string
	CreatedAt         time.Time `pg:",created_at"`
	UpdatedAt         time.Time `pg:",updated_at"`
}

func (a *Subscriber) addNew(data *tgbotapi.Chat) (acc *Subscriber, err error) {

	newAccount := &Subscriber{
		IsEnabled:         Subscriber_IS_ENABLED_TRUE,
		TelegramId:        data.ID,
		TelegramFirstName: data.FirstName,
		TelegramLastName:  data.LastName,
		TelegramUsername:  data.UserName,
		CreatedAt:         time.Now(),
	}

	_, err = dbConnect.Model(newAccount).
		Where("telegram_id = ?telegram_id").
		OnConflict("DO NOTHING").
		SelectOrInsert()

	return newAccount, err
}

func (s *Subscriber) enabledFalse() (err error) {
	s.IsEnabled = Subscriber_IS_ENABLED_FALSE
	s.UpdatedAt = time.Now()
	_, err = dbConnect.Model(s).
		Set("is_enabled = ?is_enabled").
		Set("updated_at = ?updated_at").
		Where("id = ?id").
		Update()

	return err
}

type NotificationsLogs struct {
	tableName struct{} `pg:"notifications_logs"`

	Id           int64
	SubscriberId int64 `pg:",subscriber_id,foreign:notifications_logs_subscriber_id_foreign"`
	Notification string
	CreatedAt    time.Time `pg:",created_at"`
	UpdatedAt    time.Time `pg:",updated_at"`
}

type PercentCoin struct {
	CoinId           int64
	Code             string
	Minute10         float64
	Hour             float64
	Hour4            float64
	Hour12           float64
	Hour24           float64
	Minute10AvgOpen  float64
	Minute10MaxClose float64
	HourAvgOpen      float64
	HourMaxClose     float64
	Hour4AvgOpen     float64
	Hour4MaxClose    float64
	Hour12AvgOpen    float64
	Hour12MaxClose   float64
	Hour24AvgOpen    float64
	Hour24MaxClose   float64
}
