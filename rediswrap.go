package QML

import (
	"strconv"
	"strings"
	"gopkg.in/redis.v5"
	"fmt"
)

//InitRedis 初始化redis连接
func InitRedis(redisConnStr string) {
	redisClient = newRedisClientByAddr(redisConnStr)
	if redisClient == nil {
		fmt.Println("redis初始化失败")
	}
}

//连接池方式redis
func InitRedisPoll(redisConnStr string, poolInt int) {
	redisClient = newRedisClientByAddrPoll(redisConnStr, poolInt)
	if redisClient == nil {
		fmt.Println("redis初始化失败")
	}
}

//连接池方式连接
func newRedisClientByAddrPoll(redisAddr string, poolInt int) *redis.Client {
	addrl := strings.Split(redisAddr, "/")
	db, _ := strconv.Atoi(addrl[len(addrl)-1])

	if isExist := strings.Contains(redisAddr, "@"); isExist {
		pwd := strings.Split(strings.Split(redisAddr, "@")[0], "://")[1]
		addr := strings.Split(strings.Split(redisAddr, "@")[1], "/")[0]
		return newRedisClientPoll(addr, pwd, db, poolInt)
	} else {
		addr := strings.Split(strings.Split(redisAddr, "://")[1], "/")[0]
		return newRedisClientPoll(addr, "", db, poolInt)
	}

}

//newRedisClient 新建redis客户端
func newRedisClientPoll(addr string, pwd string, db int, maxRedisPoll int) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       db,
		PoolSize: maxRedisPoll,
	})
	return client
}

//NewPubSub
func NewPubSub(topic string) *RedisPubSub {
	if redisClient == nil {
		return nil
	}
	return newRedisPubSub(topic, redisClient)
}

//GetRedis 获取redis连接对象
func GetRedis() *redis.Client {
	return redisClient
}

//CloseRedis 关闭redis连接
func CloseRedis() {
	redisClient.Close()
}

//RedisPubSub 发布订阅结构
//可以多个topic并存，并发并取
type RedisPubSub struct {
	Topic     string
	Client    *redis.Client
	SubClient *redis.PubSub
}

func newRedisPubSub(topic string, client *redis.Client) *RedisPubSub {
	redisPubSub := new(RedisPubSub)
	redisPubSub.Topic = topic
	redisPubSub.Client = client
	subClient, err := redisPubSub.Client.Subscribe(topic)
	if err != nil {
		return nil
	}
	redisPubSub.SubClient = subClient
	return redisPubSub
}

//Send 发布
func (r *RedisPubSub) Send(msg string) {
	r.Client.Publish(r.Topic, msg)
}

//Receive 订阅
func (r *RedisPubSub) Receive() (string, string) {
	for {
		msg, err := r.SubClient.ReceiveMessage()
		if err != nil {
			return "", ""
		}
		return msg.Channel, msg.Payload
	}
}

//RedisPushPop redis消息队列结构
//一个入列，可以有多个出列
type RedisPushPop struct {
	QueueName string
	Client    *redis.Client
}

//NewPushPop
func NewPushPop(queueName string) *RedisPushPop {
	if redisClient == nil {
		return nil
	}
	return newRedisPushPop(queueName, redisClient)
}

func newRedisPushPop(queueName string, client *redis.Client) *RedisPushPop {
	redisPushPop := new(RedisPushPop)
	redisPushPop.QueueName = queueName
	redisPushPop.Client = client
	return redisPushPop
}

//Push 入列
func (r *RedisPushPop) Push(msg ...interface{}) error {
	return r.Client.LPush(r.QueueName, msg...).Err()
}

//Pop 出列
func (r *RedisPushPop) Pop() string {
	for {
		msg, err := r.Client.RPop(r.QueueName).Result()
		if err != nil || len(msg) == 0 {
			continue
		}
		return msg
	}
}

//newRedisClientByAddr 通过redis地址,新建redis客户端
//redisAddr redis://127.0.0.1:6379/0
func newRedisClientByAddr(redisAddr string) *redis.Client {
	addrl := strings.Split(redisAddr, "/")
	db, _ := strconv.Atoi(addrl[len(addrl)-1])

	if isExist := strings.Contains(redisAddr, "@"); isExist {
		pwd := strings.Split(strings.Split(redisAddr, "@")[0], "://")[1]
		addr := strings.Split(strings.Split(redisAddr, "@")[1], "/")[0]
		return newRedisClient(addr, pwd, db)
	} else {
		addr := strings.Split(strings.Split(redisAddr, "://")[1], "/")[0]
		return newRedisClient(addr, "", db)
	}

}

//newRedisClient 新建redis客户端
func newRedisClient(addr string, pwd string, db int) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: pwd,
		DB:       db,
	})
	return client
}
