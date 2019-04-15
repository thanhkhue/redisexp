package redisexp

// Scan return keys matching given pattern
func Scan(conn int, match string) []string {

	var (
		cursor uint64
		keys   []string
	)

	// Using a hashtable to remove duplicates keys
	// since Scan not ensures to return unique keys
	cacheKeys := map[string]struct{}{} // empty struct to reduce the allocation

	keys, cursor = redisClient.Scan(cursor, match, 1000).Val()
	total := len(keys)
	for i := 0; i < total; i++ {
		cacheKeys[keys[i]] = struct{}{}
	}

	// the scanner not end till
	// cursor != 0
	for cursor != 0 {
		keys, cursor = redisClient.Scan(cursor, match, 1000).Val()
		total = len(keys)
		if total > 0 {
			for i := 0; i < total; i++ {
				cacheKeys[keys[i]] = struct{}{}
			}
		}
	}

	results := make([]string, len(cacheKeys))
	i := 0
	for key := range cacheKeys {
		results[i] = key
		i++
	}

	return results
}
