package disgo

import (
	"context"
	"errors"
	"fmt"
	"github.com/fanliao/go-promise"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"log"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	luaAcquire = redis.NewScript(`if (redis.call('exists', KEYS[1]) == 0) then redis.call('hset', KEYS[1], ARGV[2], 1); redis.call('pexpire', KEYS[1], ARGV[1]); return 0; end; if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then redis.call('hincrby', KEYS[1], ARGV[2], 1); redis.call('pexpire', KEYS[1], ARGV[1]); return 0; end; return redis.call('pttl', KEYS[1]);`)
	luaExpire  = redis.NewScript(`if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then return redis.call('pexpire', KEYS[1], ARGV[1]) else return 0 end`)
	luaRelease = redis.NewScript(`if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then redis.call('publish', KEYS[2], 'next'); return 0; end; local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); if (counter > 0) then redis.call('pexpire', KEYS[1], ARGV[1]); return counter; else redis.call('del', KEYS[1]); redis.call('publish', KEYS[2], 'next'); end; return 0`)
	luaZSet    = redis.NewScript(`redis.call('zadd', KEYS[1], ARGV[1], ARGV[2]); redis.call('zremrangebyscore', KEYS[1], 0, ARGV[3]); return 0;`)
)

const (
	//golang distributed redis lock
	defaultLockKeyPrefix  = "GoDistRL"
	defaultExpiry         = 30 * time.Second
	defaultTimeout        = 30 * time.Second
	defaultPublishPostfix = "-pub"
	defaultZSetPostfix    = "-zset"
)

// theFutureOfSchedule is used to store the Future with the daemon thread turned on,
// avoiding the reentrant lock to open multiple daemon threads,
// it will be deleted when unlocked.
var theFutureOfSchedule = sync.Map{}

type RedisClient interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
}

type DistributedLock struct {
	redisClient RedisClient
	config      *ConfigOption
	distLock    *DistLock
}

type ConfigOption struct {
	lockKeyPrefix   string
	lockPublishName string
	lockZSetName    string
	lockName        string
	expiry          time.Duration
	timeout         time.Duration
}

type DistLock struct {
	expiry  time.Duration
	timeout time.Duration
	// hash-name
	lockName string
	// hash-key
	field string
}

// -------------The DisGo's API---------------

// GetLock is an initialization object that needs to pass in redisClient and the name of the lock.
// The return value is a DistributedLock object, you need to use this
// object to perform lock and unlock operations, or set related properties.
func GetLock(redisClient RedisClient, lockName string) (*DistributedLock, error) {
	config := &ConfigOption{
		lockKeyPrefix:   defaultLockKeyPrefix,
		lockZSetName:    defaultLockKeyPrefix + ":" + lockName + defaultZSetPostfix,
		lockPublishName: defaultLockKeyPrefix + ":" + lockName + defaultPublishPostfix,
	}
	distList := DistLock{
		expiry:   defaultExpiry,
		timeout:  defaultTimeout,
		lockName: defaultLockKeyPrefix + ":" + lockName,
		field:    uuid.New().String() + "-" + strconv.Itoa(getGoroutineId()),
	}
	return &DistributedLock{
		redisClient: redisClient,
		config:      config,
		distLock:    &distList,
	}, nil
}

// Lock is a normal lock and will not have any retry mechanism.
// Notice! Because there is no retry mechanism, there is a high probability that the lock will fail under high concurrency.
// This is a reentrant lock.
func (dl *DistributedLock) Lock(ctx context.Context, expiryTime time.Duration) (bool, error) {
	dl.config.expiry = expiryTime

	ttl, err := dl.tryAcquire(ctx, dl.distLock.lockName, dl.distLock.field, expiryTime, false)
	if err != nil {
		return false, err
	}
	if ttl == 0 {
		return true, nil
	} else {
		return false, nil
	}
}

// TryLock is a relatively fair lock with a waiting queue and a retry mechanism.
// If the lock is successful, it will return true.
// If the lock fails, it will enter the queue and wait to be woken up, or it will return false if it times out.
// This is a reentrant lock.
func (dl *DistributedLock) TryLock(ctx context.Context, expiryTime, waitTime time.Duration) (bool, error) {
	dl.config.expiry = expiryTime
	dl.config.timeout = waitTime

	ttl, err := dl.tryAcquire(ctx, dl.distLock.lockName, dl.distLock.field, expiryTime, false)
	if err != nil {
		return false, err
	}
	if ttl == 0 {
		return true, nil
	}

	// Enter the waiting queue, waiting to be woken up
	succ := dl.subscribe(ctx, dl.distLock.lockName, dl.distLock.field, expiryTime, false)
	if succ {
		return true, nil
	}
	// CAS
	return dl.cas(ctx, expiryTime, waitTime, false)
}

// TryLockWithSchedule is the same as TryLock,
// but it will open an additional thread to ensure that the lock will not expire in advance,
// which means that you must release the lock manually, otherwise a deadlock will occur.
// This is a reentrant lock.
func (dl *DistributedLock) TryLockWithSchedule(ctx context.Context, key string, waitTime time.Duration) (bool, error) {
	dl.config.timeout = waitTime

	value := uuid.New().String() + "-" + strconv.Itoa(getGoroutineId())

	ttl, err := dl.tryAcquire(ctx, key, value, dl.config.expiry, true)
	if err != nil {
		return true, err
	}
	if ttl == 0 {
		return true, nil
	}

	// Enter the waiting queue, waiting to be woken up
	succ := dl.subscribe(ctx, key, value, dl.config.expiry, true)
	if succ {
		return true, nil
	}
	// CAS
	return dl.cas(ctx, dl.config.expiry, waitTime, true)
}

// Release is a general release lock method, and all three locks above can be used.
func (dl DistributedLock) Release(ctx context.Context) (bool, error) {
	cmd := luaRelease.Run(ctx, dl.redisClient, []string{dl.distLock.lockName, dl.config.lockZSetName}, int(dl.config.expiry/time.Millisecond), dl.distLock.field)
	res, err := cmd.Int64()
	if err != nil {
		return false, err
	} else if res > 0 {
		log.Println("The current lock has ", res, " levels left.")
	} else {
		// If the unlock is successful or does not need to be unlocked, close the thread
		if f, ok := theFutureOfSchedule.Load(dl.distLock.field); ok {
			err = f.(*promise.Future).Cancel()
			if err != nil {
				log.Println("Failed to close Future, field:", dl.distLock.field)
				return false, err
			}
		}
	}

	return true, nil
}

// -------------Minimum method---------------

// tryAcquire is the smallest unit of locking, and will use lua script for locking operation
func (dl DistributedLock) tryAcquire(ctx context.Context, key, value string, releaseTime time.Duration, isNeedScheduled bool) (int64, error) {
	cmd := luaAcquire.Run(ctx, dl.redisClient, []string{key}, int(releaseTime/time.Millisecond), value)
	ttl, err := cmd.Int64()
	if err != nil {
		// int64 is not important
		return -500, err
	}

	// Successfully locked, open WatchDog
	if isNeedScheduled && ttl == 0 {
		dl.scheduleExpirationRenewal(ctx, key, value, 30*time.Second)
	}

	return ttl, nil
}

// scheduleExpirationRenewal is a guard thread (extend the expiration time)
func (dl DistributedLock) scheduleExpirationRenewal(ctx context.Context, key, field string, releaseTime time.Duration) {
	if _, ok := theFutureOfSchedule.Load(field); ok {
		return
	}

	f := promise.Start(func(canceller promise.Canceller) {
		var count = 0
		for {
			time.Sleep(releaseTime / 3)
			if canceller.IsCancelled() {
				log.Println(field, "'s watchdog is closed, count = ", count)
				return
			}
			log.Println(field, " open a watchdog")
			cmd := luaExpire.Run(ctx, dl.redisClient, []string{key}, int(releaseTime/time.Millisecond), field)
			res, err := cmd.Int64()
			if err != nil {
				log.Fatal(field, "'s watchdog has err", err)
				return
			}
			if res == 1 {
				count += 1
				continue
			} else {
				log.Println(field, "'s watchdog is closed, count = ", count)
				return
			}
		}
	}).OnComplete(func(v interface{}) {
		// It completes the asynchronous operation by itself and ends the life of the guard thread
		theFutureOfSchedule.Delete(field)
	}).OnCancel(func() {
		// It has been cancelled by Release() before executing this function
		theFutureOfSchedule.Delete(field)
	})
	theFutureOfSchedule.Store(field, f)
}

// subscribe uses the zset of redis as the queue, and the subscription channel enters the blocking state,
// it will be woken up when the lock is available, and the thread at the head of the queue will try to lock.
func (dl DistributedLock) subscribe(ctx context.Context, lockKey, field string, releaseTime time.Duration, isNeedScheduled bool) bool {
	// Push your own id to the message queue and queue
	cmd := luaZSet.Run(ctx, dl.redisClient, []string{dl.config.lockZSetName}, time.Now().Add(dl.config.timeout/3*2).UnixMicro(), field, time.Now().UnixMicro())
	if cmd.Err() != nil {
		log.Fatal(cmd.Err())
		return false
	}

	// Subscribe to the channel, block the thread waiting for the message
	pub := dl.redisClient.Subscribe(ctx, dl.config.lockPublishName)
	f := promise.Start(func() (v interface{}, err error) {
		for range pub.Channel() {
			cmd := dl.redisClient.ZRevRange(ctx, lockKey+"-zset", -1, -1)
			if cmd != nil && cmd.Val()[0] == field {
				ttl, _ := dl.tryAcquire(ctx, lockKey, field, releaseTime, isNeedScheduled)
				if ttl == 0 {
					cmd := dl.redisClient.ZRem(ctx, lockKey+"-zset", field)
					if cmd.Err() != nil {
						log.Fatal(cmd.Err())
					}
					return true, nil
				} else {
					continue
				}
			}
		}
		return false, nil
	})
	v, err, _ := f.GetOrTimeout(uint((dl.config.timeout / 3 * 2) / time.Millisecond))
	if err != nil {
		log.Fatal(err)
		return false
	}
	err = pub.Unsubscribe(ctx)
	if err != nil {
		log.Fatal(err)
		return false
	}
	err = pub.Close()
	if err != nil {
		log.Fatal(err)
		return false
	}
	if v != nil && v.(bool) {
		return true
	} else {
		return false
	}
}

// cas acts as a compensation mechanism for subscribe.
// Due to the possibility of CPU time slice switching, the locking failure in subscribe or the subscription time is too long,
// cas determines the lock snatching time by using the TTL of lock holding,
// which can make up for the lock snatching failure caused by CPU time slice switching.
func (dl DistributedLock) cas(ctx context.Context, expiryTime, waitTime time.Duration, isNeedScheduled bool) (bool, error) {
	deadlinectx, cancel := context.WithDeadline(ctx, time.Now().Add(waitTime))
	defer cancel()

	var timer *time.Timer
	for {
		ttl, err := dl.tryAcquire(deadlinectx, dl.distLock.lockName, dl.distLock.field, expiryTime, isNeedScheduled)
		if err != nil {
			return false, err
		} else if ttl == 0 {
			return true, nil
		}

		var sleepTime time.Duration
		if ttl < 300 {
			sleepTime = time.Duration(ttl)
		} else {
			sleepTime = time.Duration(ttl / 3)
		}
		if timer == nil {
			timer = time.NewTimer(sleepTime * time.Microsecond)
			defer timer.Stop()
		} else {
			timer.Reset(sleepTime)
		}

		select {
		case <-deadlinectx.Done():
			return false, errors.New("waiting time out")
		case <-timer.C:
		}
	}
}

// -------------Utils---------------

// getGoroutineId can get the id of the current thread
func getGoroutineId() int {
	defer func() {
		if err := recover(); err != nil {
			panic(fmt.Sprintf("panic recover:panic info:%+v", err))
		}
	}()

	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}
