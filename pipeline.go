package redisexp

import (
	"fmt"

	"github.com/go-redis/redis"
)

// HGetMultipleFields this is an alternative way
// Using redis pipeline to get multiple hash fields
// with multiple hash keys
func HGetMultipleFields(conn int, cKeys, cFields []string) *map[string](map[string]([]byte)) {
	pipe := redisClient.Pipeline()
	lengthKeys := len(cKeys)
	cmds := make([](*redis.SliceCmd), lengthKeys)
	for i := 0; i < lengthKeys; i++ {
		cKey := cKeys[i]
		cmds[i] = pipe.HMGet(cKey, cFields...)
	}

	pipe.Exec()

	results := make(map[string](map[string]([]byte)))
	totalFields := len(cFields)
	for i := 0; i < lengthKeys; i++ {
		item := make(map[string]([]byte))
		values := cmds[i].Val()
		for j := 0; j < totalFields; j++ {
			field := cFields[j]
			switch v := values[j].(type) {
			case int:
				item[field] = values[j].([]byte)
			case string:
				item[field] = []byte(values[j].(string))
			case nil:
				item[field] = nil
			default:
				fmt.Printf("I don't know about type %T!\n", v)
			}
		}
		results[cKeys[i]] = item
	}
	return &results
}
