package cache

import (
	"fmt"
	"testing"
)

func Test_GetSet(t *testing.T) {
	c := NewCache(4)

	_, ok := c.Get("foo")
	if ok {
		t.Error("Get key from an empty map should return !ok")
	}

	// populate with a couple of entries
	c.Set("k-1", 42)
	c.Set("k-2", "42")

	tests := []struct {
		name           string
		k              string
		expectedValue  any
		expectedExists bool
	}{
		{name: "Get existing key with numeric value",
			k:              "k-1",
			expectedValue:  42,
			expectedExists: true,
		},
		{name: "Get existing key with string value",
			k:              "k-2",
			expectedValue:  "42",
			expectedExists: true,
		},
		{name: "Get non existing key returns !exists",
			k:              "does-not-exist",
			expectedValue:  nil,
			expectedExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, exists := c.Get(tt.k)
			if exists != tt.expectedExists {
				t.Errorf("Get(%s) returned a value with exists=%v but we expected %v", tt.k, exists, tt.expectedExists)
			}
			if exists && v != tt.expectedValue {
				t.Errorf("Get(%s) returned value %v but we expected %v", tt.k, v, tt.expectedValue)
			}
		})
	}
}

// compare the elements of the two slices ignoring the order
func sameKeys(k1, k2 []string) bool {
	// compare the length first before doing any allocations
	if len(k1) != len(k2) {
		return false
	}

	// create a set with the first keys
	k1Set := map[string]struct{}{}
	for _, k := range k1 {
		k1Set[k] = struct{}{}
	}

	// if any keys are not present in the set, we know they're different
	for _, k := range k2 {
		_, present := k1Set[k]
		if !present {
			return false
		}
	}
	return true
}

func Test_Keys(t *testing.T) {
	c := NewCache(4)
	expectedEmpty := c.Keys()
	if len(expectedEmpty) != 0 {
		t.Errorf("Expected empty keys from a new cache. Got %v instead", expectedEmpty)
	}

	c.Set("k1", 1)
	c.Set("k2", 2)
	c.Set("k3", 3)
	got := c.Keys()
	expected := []string{"k1", "k2", "k3"}
	if !sameKeys(got, expected) {
		t.Errorf("Expected %v, got %v", expected, got)
	}

	c.Delete("k2")
	expected = []string{"k1", "k3"}
	got = c.Keys()
	if !sameKeys(got, expected) {
		t.Errorf("Expected %v, got %v - after deleting a key", expected, got)
	}
}

func Test_Concurrency(t *testing.T) {
	c := NewCache(8)

	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("k-%02d", i)
		go func(key string) {
			c.Set(key, i)
		}(k)
		go func() {
			_ = c.Keys()
		}()
		go func(key string) {
			_, _ = c.Get(key)
		}(k)
		go func(key string) {
		}(k)
	}
}
