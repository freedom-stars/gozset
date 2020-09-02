package gozset

import (
	"sync"
)

type (
	// SortedSet is the final exported sorted set we can use
	SortedSet struct {
		dict map[interface{}]*element
		zsl  *skipList
		mu sync.RWMutex
	}
	zRangeSpec struct {
		min   float64
		max   float64
		minex int32
		maxex int32
	}
	zLexRangeSpec struct {
		minKey int64
		maxKey int64
		minex  int
		maxex  int
	}
)



/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

// New creates a new SortedSet and return its pointer
func New(f CompareFunc) *SortedSet {
	s := &SortedSet{
		dict: make(map[interface{}]*element),
		zsl:  zslCreate(f),
	}
	return s
}

// Length returns counts of elements
func (z *SortedSet) Length() int64 {
	return z.zsl.length
}

// Set is used to add or update an element
func (z *SortedSet) Set(score float64, key interface{}, dat interface{}) {
	z.mu.Lock()
	defer z.mu.Unlock()
	v, ok := z.dict[key]
	z.dict[key] = &element{attachment: dat, key: key, score: score}
	if ok {
		/* Remove and re-insert when score changes. */
		if score != v.score {
			z.zsl.zslDelete(v.score, key)
			z.zsl.zslInsert(score, key)
		}
	} else {
		z.zsl.zslInsert(score, key)
	}
}

// IncrBy ..
func (z *SortedSet) IncrBy(score float64, key interface{}) (float64, interface{}) {
	z.mu.Lock()
	defer z.mu.Unlock()
	v, ok := z.dict[key]
	if !ok {
		// use negative infinity ?
		return 0, nil
	}
	if score != 0 {
		z.zsl.zslDelete(v.score, key)
		v.score += score
		z.zsl.zslInsert(v.score, key)
	}
	return v.score, v.attachment
}

// Delete removes an element from the SortedSet
// by its key.
func (z *SortedSet) Delete(key interface{}) (ok bool) {
	z.mu.Lock()
	defer z.mu.Unlock()
	v, ok := z.dict[key]
	if ok {
		z.zsl.zslDelete(v.score, key)
		delete(z.dict, key)
		return true
	}
	return false
}

// GetRank returns position,score and extra data of an element which
// found by the parameter key.
// The parameter reverse determines the rank is descent or ascend，
// true means descend and false means ascend.
func (z *SortedSet) GetRank(key interface{}, reverse bool) (rank int64, score float64, data interface{}) {
	z.mu.RLock()
	defer z.mu.RUnlock()
	v, ok := z.dict[key]
	if !ok {
		return -1, 0, nil
	}
	r := z.zsl.zslGetRank(v.score, key)
	if reverse {
		r = z.zsl.length - r
	} else {
		r--
	}
	return int64(r), v.score, v.attachment

}

// GetData returns data stored in the map by its key
func (z *SortedSet) GetData(key interface{}) (data interface{}, ok bool) {
	z.mu.RLock()
	defer z.mu.RUnlock()
	o, ok := z.dict[key]
	if !ok {
		return nil, false
	}
	return o.attachment, true
}

// GetDataByRank returns the id,score and extra data of an element which
// found by position in the rank.
// The parameter rank is the position, reverse says if in the descend rank.
func (z *SortedSet) GetDataByRank(rank int64, reverse bool) (key interface{}, score float64, data interface{}) {
	z.mu.RLock()
	defer z.mu.RUnlock()
	if rank < 0 || rank > z.zsl.length {
		return 0, 0, nil
	}
	if reverse {
		rank = z.zsl.length - rank
	} else {
		rank++
	}
	n := z.zsl.zslGetElementByRank(uint64(rank))
	if n == nil {
		return 0, 0, nil
	}
	dat, _ := z.dict[n.elementID]
	if dat == nil {
		return 0, 0, nil
	}
	return dat.key, dat.score, dat.attachment
}

// Range implements ZRANGE
func (z *SortedSet) Range(start, end int64, f func(float64, interface{}, interface{})) {
	z.commonRange(start, end, false, f)
}

// RevRange implements ZREVRANGE
func (z *SortedSet) RevRange(start, end int64, f func(float64, interface{}, interface{})) {
	z.commonRange(start, end, true, f)
}

func (z *SortedSet) commonRange(start, end int64, reverse bool, f func(float64, interface{}, interface{})) {
	l := z.zsl.length
	if start < 0 {
		start += l
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end += l
	}

	if start > end || start >= l {
		return
	}
	if end >= l {
		end = l - 1
	}
	span := (end - start) + 1

	var node *skipListNode
	if reverse {
		node = z.zsl.tail
		if start > 0 {
			node = z.zsl.zslGetElementByRank(uint64(l - start))
		}
	} else {
		node = z.zsl.header.level[0].forward
		if start > 0 {
			node = z.zsl.zslGetElementByRank(uint64(start + 1))
		}
	}
	for span > 0 {
		span--
		k := node.elementID
		s := node.score
		f(s, k, z.dict[k].attachment)
		if reverse {
			node = node.backward
		} else {
			node = node.level[0].forward
		}
	}
}