package cache

import (
	"hash/fnv"
	"sync"
)

type Cache struct {
	buckets []*bucket
}

func NewCache(numBuckets int) Cache {
	b := make([]*bucket, numBuckets)
	for i := 0; i < numBuckets; i++ {
		b[i] = &bucket{data: make(map[string]any)}
	}
	return Cache{buckets: b}
}

func hashKey(k string) uint32 {
	h := fnv.New32()
	h.Write([]byte(k))
	return h.Sum32()
}

func (c *Cache) getIndex(k string) int {
	hashed := hashKey(k) % uint32(len(c.buckets))
	return int(hashed)
}

func (c *Cache) Get(key string) (any, bool) {
	bucketIndex := c.getIndex(key)
	return c.buckets[bucketIndex].Get(key)
}

func (c *Cache) Set(key string, value any) {
	bucketIndex := c.getIndex(key)
	c.buckets[bucketIndex].Set(key, value)
}

func (c *Cache) Delete(key string) {
	bucketIndex := c.getIndex(key)
	c.buckets[bucketIndex].Delete(key)
}

func (c *Cache) KeysSimple() []string {
	var keys []string
	for _, b := range c.buckets {
		keys = append(keys, b.Keys()...)
	}
	return keys
}

func (c *Cache) Keys() []string {
	// get the keys in parallel
	var keys []string
	var keysMutex sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(c.buckets))
	for _, b := range c.buckets {
		go func(b *bucket) {
			bucketKeys := b.Keys()
			keysMutex.Lock()
			keys = append(keys, bucketKeys...)
			keysMutex.Unlock()
			wg.Done()
		}(b)
	}
	wg.Wait()
	return keys
}

type bucket struct {
	sync.RWMutex
	data map[string]any
}

func (c *bucket) Get(key string) (any, bool) {
	c.RLock()
	defer c.RUnlock()
	v, ok := c.data[key]
	return v, ok
}

func (c *bucket) Set(key string, value any) {
	c.Lock()
	defer c.Unlock()
	c.data[key] = value
}

func (c *bucket) Delete(key string) {
	c.Lock()
	defer c.Unlock()
	delete(c.data, key)
}

func (c *bucket) Keys() []string {
	c.RLock()
	defer c.RUnlock()
	keys := make([]string, len(c.data))
	i := 0
	for k := range c.data {
		keys[i] = k
		i++
	}
	return keys
}
