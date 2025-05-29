package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	HTTPPort      string            `json:"http_port"`
	KafkaBrokers  []string          `json:"kafka_brokers"`
	KafkaTopics   map[string]string `json:"kafka_topics"`
	ConsumerGroup string            `json:"consumer_group"`
}

type KafkaConsumer struct {
	reader *kafka.Reader
	cancel context.CancelFunc
}

type MovieEvent struct {
	MovieID int    `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserID  int    `json:"user_id"`
}

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID   int     `json:"payment_id"`
	UserID      int     `json:"user_id"`
	Amount      float32 `json:"amount"`
	Status      string  `json:"status"`
	Timestamp   string  `json:"timestamp"`
	Method_type string  `json:"method_type"`
}

var (
	config        Config
	movieWriter   *kafka.Writer
	userWriter    *kafka.Writer
	paymentWriter *kafka.Writer
	consumers     map[string]*KafkaConsumer
)

func main() {

	config = Config{
		HTTPPort:     getEnv("PORT", "8082"), // с fallback значением
		KafkaBrokers: getBrokersFromEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopics: map[string]string{
			"movie":   getEnv("MOVIE_TOPIC", "movie-events"),
			"user":    getEnv("USER_TOPIC", "user-events"),
			"payment": getEnv("PAYMENT_TOPIC", "payment-events"),
		},
	}

	initKafkaWriters()
	initKafkaConsumers()

	// Настройка HTTP сервера
	http.HandleFunc("/api/events/health", healthCheckHandler)
	http.HandleFunc("/api/events/movie", movieEventHandler)
	http.HandleFunc("/api/events/user", userEventHandler)
	http.HandleFunc("/api/events/payment", paymentEventHandler)

	server := &http.Server{
		Addr: ":" + config.HTTPPort,
	}

	// Запуск сервера в горутине
	go func() {
		log.Printf("Starting HTTP server on port %s", config.HTTPPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Обработка сигналов для graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server shutdown error: %v", err)
	}

	// Закрытие всех writers
	closeWriters := func() {
		if err := movieWriter.Close(); err != nil {
			log.Printf("Error closing movie writer: %v", err)
		}
		if err := userWriter.Close(); err != nil {
			log.Printf("Error closing user writer: %v", err)
		}
		if err := paymentWriter.Close(); err != nil {
			log.Printf("Error closing payment writer: %v", err)
		}
	}
	defer closeWriters()

	defer closeConsumers()

	log.Println("Server gracefully stopped")
}

func getBrokersFromEnv(envVar string, fallback string) []string {
	value := getEnv(envVar, fallback)
	return strings.Split(value, ",")
}

// getEnv читает переменную окружения или возвращает fallback значение
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func initKafkaWriters() {
	movieWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  config.KafkaBrokers,
		Topic:    config.KafkaTopics["movie"],
		Balancer: &kafka.LeastBytes{},
	})

	userWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  config.KafkaBrokers,
		Topic:    config.KafkaTopics["user"],
		Balancer: &kafka.LeastBytes{},
	})

	paymentWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  config.KafkaBrokers,
		Topic:    config.KafkaTopics["payment"],
		Balancer: &kafka.LeastBytes{},
	})
}

func initKafkaConsumers() {
	consumers = make(map[string]*KafkaConsumer)

	for eventType, topic := range config.KafkaTopics {
		ctx, cancel := context.WithCancel(context.Background())

		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: config.KafkaBrokers,
			Topic:   topic,
			GroupID: config.ConsumerGroup,
			// MinBytes: 10e3, // 10KB
			// MaxBytes: 10e6, // 10MB
		})

		consumers[eventType] = &KafkaConsumer{
			reader: r,
			cancel: cancel,
		}

		go consumeMessages(ctx, eventType, r)
	}
}

func consumeMessages(ctx context.Context, eventType string, r *kafka.Reader) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := r.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading %s message: %v", eventType, err)
				continue
			}

			log.Printf("Received %s event: %s", eventType, string(m.Value))

			// Здесь можно добавить обработку сообщения
			// processEvent(eventType, m.Value)
		}
	}
}

// func processEvent(eventType string, data []byte) {
// 	// Создаем лог-запись с полной информацией
// 	entry := map[string]interface{}{
// 		"event_type": eventType,
// 		"message":    json.RawMessage(data), // сохраняем оригинальный JSON
// 		"timestamp":  time.Now().Format(time.RFC3339Nano),
// 		"service":    "event-processor",
// 	}

// 	// Конвертируем в JSON для логов
// 	logData, err := json.Marshal(entry)
// 	if err != nil {
// 		log.Printf("[ERROR] Failed to marshal log entry: %v", err)
// 		return
// 	}

// 	// Логируем в стандартный вывод
// 	log.Printf("[KAFKA-EVENT] %s", string(logData))
// }

func closeConsumers() {
	for _, consumer := range consumers {
		consumer.cancel() // Остановка горутин
		if err := consumer.reader.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
	}
}

// Хэндлеры

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func movieEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Добавьте валидацию
	if event.MovieID == 0 || event.Action == "" || event.UserID == 0 || event.Title == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal event: %v", err), http.StatusInternalServerError)
		return
	}

	// Запись сообщения в Kafka
	err = movieWriter.WriteMessages(r.Context(), kafka.Message{
		Value: eventJSON,
	})

	if err != nil {
		log.Printf("Kafka write error: %v (value: %s, %d, %s, %d)", err, event.Action, event.MovieID, event.Title, event.UserID)
		http.Error(w, fmt.Sprintf("Failed to write to Kafka: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Event processed",
		"event":   string(eventJSON),
	})
}

func userEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event UserEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Добавьте валидацию
	if event.UserID == 0 || event.Action == "" || event.Username == "" || event.Timestamp == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal event: %v", err), http.StatusInternalServerError)
		return
	}

	// Запись сообщения в Kafka
	err = userWriter.WriteMessages(r.Context(), kafka.Message{
		Value: eventJSON,
	})

	if err != nil {
		log.Printf("Kafka write error: %v (value: %d, %s, %s, %s)", err, event.UserID, event.Action, event.Username, event.Timestamp)
		http.Error(w, fmt.Sprintf("Failed to write to Kafka: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Event processed",
		"event":   string(eventJSON),
	})
}

func paymentEventHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Добавьте валидацию
	if event.PaymentID == 0 || event.UserID == 0 || event.Amount == 0 || event.Status == "" || event.Timestamp == "" || event.Method_type == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal event: %v", err), http.StatusInternalServerError)
		return
	}

	// Запись сообщения в Kafka
	err = paymentWriter.WriteMessages(r.Context(), kafka.Message{
		Value: eventJSON,
	})

	if err != nil {
		log.Printf("Kafka write error: %v (value: %d, %d, %f, %s, %s, %s)", err, event.PaymentID, event.UserID, event.Amount, event.Status, event.Timestamp, event.Method_type)
		http.Error(w, fmt.Sprintf("Failed to write to Kafka: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Event processed",
		"event":   string(eventJSON),
	})
}
