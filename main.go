package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	RedisConnString  = os.Getenv("REDIS_CONN_STRING")
	QueryInterval    = mustParseDuration(envOr("QUERY_INTERVAL", "3s"))
	PingPongInterval = mustParseDuration(envOr("PING_PONG_INTERVAL", "0"))
	RedisQuery       = envOr("REDIS_QUERY", "PING")
	FreshClient      = envOr("FRESH_CLIENT", "true") != "false"
	StickyClient     = envOr("STICKY_CLIENT", "false") != "false"
	NoClose          = envOr("NO_CLOSE", "true") == "true"
)

func mustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

func envOr(k, v string) string {
	v2 := os.Getenv(k)
	if v2 == "" {
		return v
	}
	return v2
}

func main() {
	ctx, can := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer can()

	if PingPongInterval > 0 {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(PingPongInterval):
					log.Println("ping")
				}
			}
		}()
	}

	go func() {
		for ctx.Err() == nil {
			time.Sleep(time.Second)
			http.ListenAndServe(":8080", http.HandlerFunc(http.NotFound))
		}
	}()

	ticker := time.NewTicker(QueryInterval)
	defer ticker.Stop()

	u, err := url.Parse(RedisConnString)
	if err != nil {
		panic(err)
	}
	clientOptions := &redis.Options{
		Addr: u.Host,
		CredentialsProvider: func() (string, string) {
			if u.User == nil {
				return "", ""
			}
			p, _ := u.User.Password()
			return u.User.Username(), p
		},
		ContextTimeoutEnabled: true,
	}
	if u.Scheme == "rediss" {
		clientOptions.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	var stickyClient *redis.Client
	if StickyClient {
		stickyClient = redis.NewClient(clientOptions)
		defer func() {
			if NoClose {
				return
			}
			if err := stickyClient.Close(); err != nil {
				log.Printf("failed to close sticky client: %v", err)
			}
		}()
	}

	for {
		if FreshClient {
			if err := func() error {
				c := redis.NewClient(clientOptions)
				defer func() {
					if NoClose {
						return
					}
					if err := c.Close(); err != nil {
						log.Printf("failed to close fresh client: %v", err)
					}
				}()

				return tryClient(ctx, c)
			}(); err != nil {
				log.Printf("failed to use a new client: %v", err)
			}
		}

		if stickyClient != nil {
			if err := tryClient(ctx, stickyClient); err != nil {
				log.Printf("failed to use a sticky client: %v", err)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

	}
}

func tryClient(ctx context.Context, c *redis.Client) error {
	subctx, subcan := context.WithTimeout(ctx, time.Second*10)
	defer subcan()
	if result := c.Ping(subctx); result.Err() != nil {
		return fmt.Errorf("failed to use client at all: (%s) %w", result.Err(), result.Err())
	}

	args := make([]interface{}, 0)
	for _, arg := range strings.Fields(RedisQuery) {
		args = append(args, strings.TrimSpace(arg))
	}
	result := c.Do(ctx, args...)

	if result := c.Ping(subctx); result.Err() != nil {
		return fmt.Errorf("failed to use client again: (%s) %w", result.Err(), result.Err())
	}

	if err := result.Err(); err != nil {
		return fmt.Errorf("failed to use client (%s): (%s) %w", RedisQuery, err, err)
	} else if result, err := result.Result(); err != nil {
		return fmt.Errorf("failed to use client result (%s): (%s) %w", RedisQuery, err, err)
	} else {
		log.Printf("%q = %v", RedisQuery, result)
	}

	return nil
}
