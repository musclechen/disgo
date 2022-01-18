English | [中文](./Readme-CN.md)

# DisGo 
DisGo is a distributed lock based on redis, developed using GoLang.

DisGo 

The name comes from the first three letters of `distributed` and `disco`, and then adds the abbreviation `go` of `golang`. I hope that when you use distributed locks, it can be as simple as dancing disco.

Here is an example of how to use disgo
```go
package main

import (
	"context"
	"disgo"
	"github.com/go-redis/redis/v8"
	"strconv"
	"sync"
	"time"
)
    func main() {
        // Connect to redis.
        client := redis.NewClient(&redis.Options{
            Network: "tcp",
            Addr:    "127.0.0.1:6379",
        })
    
        wg := new(sync.WaitGroup)
        for i := 0; i < 100; i++ {
            wg.Add(1)
            go func() {
                defer wg.Done()
                ctx := context.Background()
                lock, _ := disgo.GetLock(client, "test")
                success, err := lock.TryLock(ctx, 5*time.Second, 10*time.Second)
                if err != nil {
                    return
                }
                if success {
                    count := client.Get(ctx, "test").Val()
                    x, _ := strconv.Atoi(count)
                    x += 1
                    client.Set(ctx, "test", x, -1)
                }
                lock.Release(ctx)
            }()
        }
        wg.Wait()
        client.Close()
    }
```
