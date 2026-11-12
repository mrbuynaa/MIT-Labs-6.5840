package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	ck      kvtest.IKVClerk
	lockKey string // shared key that all clients contend on
	id      string // unique identity of this lock holder
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	return &Lock{ck: ck, lockKey: l, id: kvtest.RandValue(8)}
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.lockKey)
		if err == rpc.OK && val == lk.id {
			return
		}
		if err == rpc.ErrNoKey || (err == rpc.OK && val == "") {
			if err == rpc.ErrNoKey {
				ver = 0
			}
			perr := lk.ck.Put(lk.lockKey, lk.id, ver)
			if perr == rpc.OK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	_, ver, _ := lk.ck.Get(lk.lockKey)
	perr := lk.ck.Put(lk.lockKey, "", ver)
	if perr == rpc.OK {
		return
	}
}
