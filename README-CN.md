[English](./README.md) | 中文

# DisGo 简介
DisGo是一个基于Redis的分布式锁，采用Golang语言开发。该名字源于`Distributed`、`Disco`和`Golang`，祝各位写代码犹如跳迪斯科一样丝滑。

# DisGo拥有的特性

### **可重入锁** 
DisGo是一个可重入锁，使用Redis的Hash类型作为锁，hash-name为锁名，hash-key存放的是当前持锁线程的唯一id，hash-value存放的是当前加锁次数。

### **公平锁**
Golang本身没有线程安全的队列可以使用，为了方便，DisGo使用Redis的ZSet模拟队列，一定程度上保证了先进先出，提供了公平抢锁机制。

### **自动续期**
DisGo提供自动续期功能，防止业务没有执行完，锁却提前释放导致的数据错误。

### **自旋抢锁**
DisGo提供自旋抢锁，在设定的等待时间内将会自动重试抢锁，直到抢到锁或者等待超时。

### **更加高效**
DisGo使用了Redis的发布订阅，在锁释放的第一时间会收到消息，然后根据等待队列的顺序执行加锁。

# DisGo的加锁流程
<details>
<summary>点击我</summary>

![](./screenshot/LockFlowChart.png)
</details>


# DisGo的API介绍
#### 获取锁对象
```go
    redisClient := redis.NewClient(&redis.Options{
        Network: "tcp",
        Addr:    "127.0.0.1:6379",
    })
    lock := disgo.GetLock(redisClient, "test")
```

#### 普通加锁（不需要自动续期）
```go
    success, err := lock.Lock(ctx, 5*time.Second, 10*time.Second)
```

#### 自旋加锁（不需要自动续期）
```go
    success, err := lock.TryLock(ctx, 5*time.Second, 10*time.Second)
```

#### 自旋加锁（自动续期）
```go
    success, err := lock.TryLockWithSchedule(ctx, 5*time.Second)
```

#### 解锁（通用）
```go
    success, err := lock.Release(ctx)
```
