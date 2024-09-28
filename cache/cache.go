package cache

import (
	"hash/fnv"
	"sync"
)

type Cache[T any] struct {
	buckets []*bucket[T]
}

func NewCache[T any](numBuckets int) Cache[T] {
	b := make([]*bucket[T], numBuckets)
	for i := 0; i < numBuckets; i++ {
		b[i] = &bucket[T]{data: make(map[string]T)}
	}
	return Cache[T]{buckets: b}
}

func hashKey(k string) uint32 {
	h := fnv.New32()
	h.Write([]byte(k))
	return h.Sum32()
}

func (c *Cache[T]) getIndex(k string) int {
	hashed := hashKey(k) % uint32(len(c.buckets))
	return int(hashed)
}

func (c *Cache[T]) Get(key string) (T, bool) {
	bucketIndex := c.getIndex(key)
	return c.buckets[bucketIndex].Get(key)
}

func (c *Cache[T]) Set(key string, value T) {
	bucketIndex := c.getIndex(key)
	c.buckets[bucketIndex].Set(key, value)
}

func (c *Cache[T]) Delete(key string) {
	bucketIndex := c.getIndex(key)
	c.buckets[bucketIndex].Delete(key)
}

func (c *Cache[T]) KeysSimple() []string {
	var keys []string
	for _, b := range c.buckets {
		keys = append(keys, b.Keys()...)
	}
	return keys
}

func (c *Cache[T]) Keys() []string {
	// get the keys in parallel
	var keys []string
	var keysMutex sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(c.buckets))
	for _, b := range c.buckets {
		go func(b *bucket[T]) {
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

type bucket[T any] struct {
	sync.RWMutex
	data map[string]T
}

func (c *bucket[T]) Get(key string) (T, bool) {
	c.RLock()
	defer c.RUnlock()
	v, ok := c.data[key]
	return v, ok
}

func (c *bucket[T]) Set(key string, value T) {
	c.Lock()
	defer c.Unlock()
	c.data[key] = value
}

func (c *bucket[T]) Delete(key string) {
	c.Lock()
	defer c.Unlock()
	delete(c.data, key)
}

func (c *bucket[T]) Keys() []string {
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
