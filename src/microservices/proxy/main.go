package main

import (
	"context"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	gradualMigration := os.Getenv("GRADUAL_MIGRATION") != "false"

	percent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	if err != nil || percent <= 0 || percent > 100 {
		log.Fatal("Invalid percent:", percent, " error:", err)
	}

	monolithURL, err := url.Parse(os.Getenv("MONOLITH_URL"))
	if err != nil {
		log.Fatal("Invalid MONOLITH_URL:", err)
	}

	moviesServiceURL, err := url.Parse(os.Getenv("MOVIES_SERVICE_URL"))
	if err != nil {
		log.Fatal("Invalid MOVIES_SERVICE_URL:", err)
	}

	monolithProxy := httputil.NewSingleHostReverseProxy(monolithURL)
	moviesProxy := httputil.NewSingleHostReverseProxy(moviesServiceURL)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/movies" && gradualMigration {
			ms := time.Now().Nanosecond() / 1e6 % 100
			if ms < percent {
				moviesProxy.ServeHTTP(w, r)
			} else {
				monolithProxy.ServeHTTP(w, r)
			}
			return
		}
		monolithProxy.ServeHTTP(w, r)
	})

	counterA := 0
	counterB := 0

	server := &http.Server{
		Addr: ":" + port,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/movies" && gradualMigration {
				ms := time.Now().Nanosecond() / 1e6 % 100
				if ms < percent {
					counterA = counterA + 1
					moviesProxy.ServeHTTP(w, r)
					log.Println("Sent to 111  counter: ", counterA)
				} else {
					counterB++
					monolithProxy.ServeHTTP(w, r)
					log.Println("Sent to 222  counter: ", counterB)
				}
				return
			}
			monolithProxy.ServeHTTP(w, r)
		}),
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Shutdown error: %v", err)
		}
	}()

	log.Printf("API Gateway запущен на :%s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
	log.Println("API Gateway stopped gracefully")
}
