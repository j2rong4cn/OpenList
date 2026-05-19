package cache

import (
	"sync"
	"time"
)

type TypedCache[T any] struct {
	entries map[string]map[string]*CacheEntry[T]
	mu      sync.RWMutex
	ttl     time.Duration
}

func NewTypedCache[T any](ttl time.Duration) *TypedCache[T] {
	c := &TypedCache[T]{
		entries: make(map[string]map[string]*CacheEntry[T]),
		ttl:     ttl,
	}
	gcFuncs = append(gcFuncs, c.GC)
	return c
}

func (c *TypedCache[T]) SetType(key, typeKey string, value T) {
	c.SetTypeWithExpirable(key, typeKey, value, ExpirationTime(time.Now().Add(c.ttl)))
}

func (c *TypedCache[T]) SetTypeWithTTL(key, typeKey string, value T, ttl time.Duration) {
	c.SetTypeWithExpirable(key, typeKey, value, ExpirationTime(time.Now().Add(ttl)))
}

func (c *TypedCache[T]) SetTypeWithExpirable(key, typeKey string, value T, exp Expirable) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cache, exists := c.entries[key]
	if !exists {
		cache = make(map[string]*CacheEntry[T])
		c.entries[key] = cache
	}

	cache[typeKey] = &CacheEntry[T]{
		data:      value,
		Expirable: exp,
	}
}

func (c *TypedCache[T]) GetType(key, typeKey string) (T, bool) {
	c.mu.RLock()
	cache, exists := c.entries[key]
	if !exists {
		c.mu.RUnlock()
		var zero T
		return zero, false
	}
	entry, exists := cache[typeKey]
	c.mu.RUnlock()

	if !exists {
		var zero T
		return zero, false
	}
	if !entry.Expired() {
		return entry.data, true
	}

	c.mu.Lock()
	if cache, exists = c.entries[key]; exists {
		if cache[typeKey] == entry {
			delete(cache, typeKey)
			if len(cache) == 0 {
				delete(c.entries, key)
			}
		}
	}
	c.mu.Unlock()
	var zero T
	return zero, false
}

func (c *TypedCache[T]) DeleteKey(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

func (c *TypedCache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]map[string]*CacheEntry[T])
}

func (c *TypedCache[T]) GC() {
	c.mu.Lock()
	for key, cache := range c.entries {
		for typeKey, entry := range cache {
			if entry.Expired() {
				delete(cache, typeKey)
			}
		}
		if len(cache) == 0 {
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()
}
