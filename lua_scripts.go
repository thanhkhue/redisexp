package redisexp

import (
	"fmt"
	"log"
	"sync"

	"github.com/go-redis/redis"
)

var redisClient *redis.Client
var luaSHA = make(map[int]string)
var luaLock sync.RWMutex

const luaScript = `
	local results = {}
	for i = 1, table.getn(KEYS) do
		results[i] = redis.call("HMGET", KEYS[i], unpack(ARGV))
	end
	return results
`

// HGetMultipleFieldsLuaScript load LuaScript
// to get multiple field in multiple hash keys
func HGetMultipleFieldsLuaScript(
	conn int, cKeys []string, fields []string,
) *map[string](map[string]([]byte)) {

	var err error

	// Multiple read is safe
	luaLock.RLock()
	sha := luaSHA[conn]
	luaLock.RUnlock()

	if sha == "" {
		sha, err = redisClient.ScriptLoad(luaScript).Result()
		if err != nil {
			log.Fatalf("Error while loading script %v\n", err)
		}
		luaLock.Lock()
		luaSHA[conn] = sha
		luaLock.Unlock()
	}

	argv := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		argv[i] = fields[i]
	}
	results, err := redisClient.EvalSha(sha, cKeys, argv...).Result()
	if err != nil {
		log.Printf("Error while load from cache %v\n", err)
		return nil
	}

	values := results.([]interface{})
	total := len(values)
	totalFields := len(fields)
	items := make(map[string](map[string]([]byte)))

	for i := 0; i < total; i++ {
		item := make(map[string]([]byte))

		val := values[i].([]interface{})
		for j := 0; j < totalFields; j++ {
			field := fields[j]
			switch v := val[j].(type) {
			case int:
				item[field] = val[j].([]byte)
			case string:
				item[field] = []byte(val[j].(string))
			case nil:
				item[field] = nil
			default:
				fmt.Printf("I don't know about type %T!\n", v)
			}
		}
		items[cKeys[i]] = item
	}
	return &items
}
