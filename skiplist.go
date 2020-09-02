package gozset

import "math/rand"

const (
	zSkiplistP      = 0.25 /* Skiplist P = 1/4 */
	defaultMaxLevel = 32
)

type (
	element struct {
		key        interface{}
		attachment interface{}
		score      float64
	}
	skipListNode struct {
		elementID interface{}
		score     float64
		backward  *skipListNode
		level     []*skipListLevel
	}
	skipListLevel struct {
		forward *skipListNode
		span    uint64
	}
	skipList struct {
		header  *skipListNode
		tail    *skipListNode
		length  int64
		level   int16
		compare CompareFunc
	}
)

func zslCreate(f CompareFunc) *skipList {
	return &skipList{
		level:   1,
		header:  zslCreateNode(defaultMaxLevel, 0, 0),
		compare: f,
	}
}

func zslCreateNode(level int16, score float64, id interface{}) *skipListNode {
	n := &skipListNode{
		score:     score,
		elementID: id,
		level:     make([]*skipListLevel, level),
	}
	for i := range n.level {
		n.level[i] = new(skipListLevel)
	}
	return n
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and _ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
func randomLevel() int16 {
	level := int16(1)
	for float32(rand.Int31()&0xFFFF) < (zSkiplistP * 0xFFFF) {
		level++
	}
	if level < defaultMaxLevel {
		return level
	}
	return defaultMaxLevel
}

/* zslInsert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'element'. */
func (zsl *skipList) zslInsert(score float64, id interface{}) *skipListNode {
	update := make([]*skipListNode, defaultMaxLevel)
	rank := make([]uint64, defaultMaxLevel)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		/* store rank that is crossed to reach the insert position */
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		if x.level[i] != nil {
			for x.level[i].forward != nil &&
				(x.level[i].forward.score < score ||
					//(x.level[i].forward.score == score && x.level[i].forward.elementID < id)) {
					(x.level[i].forward.score == score && zsl.compare(x.level[i].forward.elementID, id) < 0)) {
				rank[i] += x.level[i].span
				x = x.level[i].forward
			}
		}
		update[i] = x
	}
	/* we assume the element is not already inside, since we allow duplicated
	 * scores, reinserting the same element should never happen since the
	 * caller of zslInsert() should test in the hash table if the element is
	 * already inside or not. */
	level := randomLevel()
	if level > zsl.level {
		for i := zsl.level; i < level; i++ {
			rank[i] = 0
			update[i] = zsl.header
			update[i].level[i].span = uint64(zsl.length)
		}
		zsl.level = level
	}
	x = zslCreateNode(level, score, id)
	for i := int16(0); i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		/* update span covered by update[i] as x is inserted here */
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	/* increment span for untouched levels */
	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == zsl.header {
		x.backward = nil
	} else {
		x.backward = update[0]

	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}
	zsl.length++
	return x
}

/* Internal function used by zslDelete, zslDeleteByScore and zslDeleteByRank */
func (zsl *skipList) zslDeleteNode(x *skipListNode, update []*skipListNode) {
	for i := int16(0); i < zsl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}
	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->element). */
func (zsl *skipList) zslDelete(score float64, id interface{}) int {
	update := make([]*skipListNode, defaultMaxLevel)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					//x.level[i].forward.elementID < id)) {
					zsl.compare(x.level[i].forward.elementID, id) < 0)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	/* We may have multiple elements with the same score, what we need
	 * is to find the element with both the right score and elementect. */
	x = x.level[0].forward
	if x != nil && score == x.score && zsl.compare(x.elementID, id) == 0 {
		zsl.zslDeleteNode(x, update)
		return 1
	}
	return 0 /* not found */
}

/* Returns if there is a part of the zset is in range. */
func (zsl *skipList) zslIsInRange(ran *zRangeSpec) bool {
	/* Test for ranges that will always be empty. */
	if ran.min > ran.max ||
		(ran.min == ran.max && (ran.minex != 0 || ran.maxex != 0)) {
		return false
	}
	x := zsl.tail
	if x == nil || !zsl.zslValueGteMin(x.score, ran) {
		return false
	}
	x = zsl.header.level[0].forward
	if x == nil || !zsl.zslValueLteMax(x.score, ran) {
		return false
	}
	return true
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
func (zsl *skipList) zslFirstInRange(ran *zRangeSpec) *skipListNode {
	/* If everything is out of range, return early. */
	if !zsl.zslIsInRange(ran) {
		return nil
	}

	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		/* Go forward while *OUT* of range. */
		for x.level[i].forward != nil &&
			!zsl.zslValueGteMin(x.level[i].forward.score, ran) {
			x = x.level[i].forward
		}
	}
	/* This is an inner range, so the next node cannot be NULL. */
	x = x.level[0].forward
	//serverAssert(x != NULL);

	/* Check if score <= max. */
	if !zsl.zslValueLteMax(x.score, ran) {
		return nil
	}
	return x
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
func (zsl *skipList) zslLastInRange(ran *zRangeSpec) *skipListNode {

	/* If everything is out of range, return early. */
	if !zsl.zslIsInRange(ran) {
		return nil
	}
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		/* Go forward while *IN* range. */
		for x.level[i].forward != nil &&
			zsl.zslValueLteMax(x.level[i].forward.score, ran) {
			x = x.level[i].forward
		}
	}
	/* This is an inner range, so this node cannot be NULL. */
	//serverAssert(x != NULL);

	/* Check if score >= min. */
	if !zsl.zslValueGteMin(x.score, ran) {
		return nil
	}
	return x
}

/* Delete all the elements with score between min and max from the skiplist.
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
func (zsl *skipList) zslDeleteRangeByScore(ran *zRangeSpec, dict map[interface{}]*element) uint64 {
	removed := uint64(0)
	update := make([]*skipListNode, defaultMaxLevel)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil {
			var condition bool
			if ran.minex != 0 {
				condition = x.level[i].forward.score <= ran.min
			} else {
				condition = x.level[i].forward.score < ran.min
			}
			if !condition {
				break
			}
			x = x.level[i].forward
		}
		update[i] = x
	}

	/* Current node is the last with score < or <= min. */
	x = x.level[0].forward

	/* Delete nodes while in range. */
	for x != nil {
		var condition bool
		if ran.maxex != 0 {
			condition = x.score < ran.max
		} else {
			condition = x.score <= ran.max
		}
		if !condition {
			break
		}
		next := x.level[0].forward
		zsl.zslDeleteNode(x, update)
		delete(dict, x.elementID)
		// Here is where x->element is actually released.
		// And golang has GC, don't need to free manually anymore
		//zslFreeNode(x)
		removed++
		x = next
	}
	return removed
}

func (zsl *skipList) zslDeleteRangeByLex(ran *zLexRangeSpec, dict map[interface{}]*element) uint64 {
	removed := uint64(0)

	update := make([]*skipListNode, defaultMaxLevel)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && !zsl.zslLexValueGteMin(x.level[i].forward.elementID, ran) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	/* Current node is the last with score < or <= min. */
	x = x.level[0].forward

	/* Delete nodes while in range. */
	for x != nil && zsl.zslLexValueLteMax(x.elementID, ran) {
		next := x.level[0].forward
		zsl.zslDeleteNode(x, update)
		delete(dict, x.elementID)
		removed++
		x = next
	}
	return removed
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
func (zsl *skipList) zslDeleteRangeByRank(start, end uint64, dict map[interface{}]*element) uint64 {
	update := make([]*skipListNode, defaultMaxLevel)
	var traversed, removed uint64

	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (traversed+x.level[i].span) < start {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	traversed++
	x = x.level[0].forward
	for x != nil && traversed <= end {
		next := x.level[0].forward
		zsl.zslDeleteNode(x, update)
		delete(dict, x.elementID)
		removed++
		traversed++
		x = next
	}
	return removed
}

/* Find the rank for an element by both score and element.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
func (zsl *skipList) zslGetRank(score float64, key interface{}) int64 {
	rank := uint64(0)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score &&
					//x.level[i].forward.elementID <= key)) {
					zsl.compare(x.level[i].forward.elementID, key) <= 0)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		/* x might be equal to zsl->header, so test if element is non-NULL */
		//if x.elementID == key {
		if zsl.compare(x.elementID, key) == 0 {
			return int64(rank)
		}
	}
	return 0
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
func (zsl *skipList) zslGetElementByRank(rank uint64) *skipListNode {
	traversed := uint64(0)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (traversed+x.level[i].span) <= rank {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

/* compare something. */
func (zsl *skipList) zslLexValueGteMin(id interface{}, spec *zLexRangeSpec) bool {
	if spec.minex != 0 {
		return zsl.compare(id, spec.minKey) > 0
	}
	return zsl.compare(id, spec.minKey) >= 0
}

func (zsl *skipList) zslLexValueLteMax(id interface{}, spec *zLexRangeSpec) bool {
	if spec.maxex != 0 {
		return zsl.compare(id, spec.maxKey) < 0
	}
	return zsl.compare(id, spec.maxKey) <= 0
}

func (zsl *skipList) zslValueGteMin(value float64, spec *zRangeSpec) bool {
	if spec.minex != 0 {
		return value > spec.min
	}
	return value >= spec.min
}

func (zsl *skipList) zslValueLteMax(value float64, spec *zRangeSpec) bool {
	if spec.maxex != 0 {
		return value < spec.max
	}
	return value <= spec.max
}
