package redisdb

import (
	"errors"
	"fmt"
	"github.com/gottingen/crab/store"
	"strconv"
	"strings"
)

var (
	ErrHset = errors.New("HSET command requires at least three arguments: HSET <hashmap> <key> <value> [<TTL>]")
	ErrHget = errors.New("HGET command requires at least two arguments: HGET <hashmap> <key>")

	ErrHdel  = errors.New("HDEL command requires at least two arguments: HDEL <hashmap> [<key1> <key2> ...]")
	ErrHmset = errors.New("HMSET command requires at least three arguments: HMSET <hashmap> <key1> <val1> [<key2> <val2> ...]")
)

func cmdHset(ctx *RequestContext) {
	var ns, k, v string
	var ttl int

	if len(ctx.Args) < 3 {
		ctx.Err = ErrHset
	}

	ns, k, v = ctx.Args[0], ctx.Args[1], ctx.Args[2]

	if len(ctx.Args) > 3 {
		ttl, _ = strconv.Atoi(ctx.Args[3])
	}

	if err := ctx.DB.Set(ns+"/{HASH}/"+k, v, ttl); err != nil {
		ctx.Err = err
		return
	}

	ctx.Integer = 1
}

func cmdHget(ctx *RequestContext) {
	var ns, k string

	if len(ctx.Args) < 2 {
		ctx.Err = ErrHget
	}

	ns, k = ctx.Args[0], ctx.Args[1]

	ctx.Args = []string{ns + "/{HASH}/" + k}

	cmdGet(ctx)
}

func cmdHdel(ctx *RequestContext) {
	var ns string

	if len(ctx.Args) < 1 {
		ctx.Err = ErrHdel
		return
	}

	ns = ctx.Args[0]
	keys := ctx.Args[1:]

	if len(keys) > 0 {
		for i, k := range keys {
			keys[i] = ns + "/{HASH}/" + k
		}
	} else {
		prefix := ns + "/{HASH}/"
		ctx.DB.Scan(store.ScannerOptions{
			Prefix:        prefix,
			Offset:        prefix,
			IncludeOffset: true,
			FetchValues:   false,
			Handler: func(k, _ string) bool {
				keys = append(keys, k)
				return true
			},
		})
	}

	ctx.Args = keys

	cmdDel(ctx)
}

func cmdHgetAll(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = ErrHget
		return
	}

	prefix := ctx.Args[0] + "/{HASH}/"
	data := map[string]string{}
	err := ctx.DB.Scan(store.ScannerOptions{
		FetchValues:   true,
		IncludeOffset: true,
		Prefix:        prefix,
		Offset:        prefix,
		Handler: func(k, v string) bool {
			p := strings.SplitN(k, "/{HASH}/", 2)
			if len(p) < 2 {
				return true
			}
			data[p[1]] = v
			return true
		},
	})

	if err != nil {
		ctx.Err = err
		return
	}
	ctx.Array = make([]string, len(data)*2)
	i := 0
	for k, v := range data {
		ctx.Array[i] = k
		ctx.Array[i+1] = v
		i += 2
	}

}

func cmdHkeys(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = ErrHget
		return
	}

	prefix := ctx.Args[0] + "/{HASH}/"
	data := []string{}
	err := ctx.DB.Scan(store.ScannerOptions{
		FetchValues:   false,
		IncludeOffset: true,
		Prefix:        prefix,
		Offset:        prefix,
		Handler: func(k, _ string) bool {
			p := strings.SplitN(k, "/{HASH}/", 2)
			if len(p) < 2 {
				return true
			}
			data = append(data, p[1])
			return true
		},
	})

	if err != nil {
		ctx.Err = err
		return
	}
	ctx.Array = data
}

func cmdHmset(ctx *RequestContext) {
	var ns string

	if len(ctx.Args) < 3 {
		ctx.Err = ErrHmset
		return
	}

	ns = ctx.Args[0]
	args := ctx.Args[1:]

	currentCount := len(args)
	if len(args)%2 != 0 {
		ctx.Err = fmt.Errorf("HMSET {key => value} pairs must be even. You specified %d, it should be %d or %d", currentCount, currentCount+1, currentCount-1)
		return
	}

	data := map[string]string{}
	for i, v := range args {
		index := i + 1
		if index%2 == 0 {
			data[ns+"/{HASH}/"+args[i-1]] = v
		} else {
			data[ns+"/{HASH}/"+args[i]] = ""
		}
	}

	if err := ctx.DB.MSet(data); err != nil {
		ctx.Err = err
		return
	}

	ctx.Integer = int64(len(data))
}

func cmdHexist(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = fmt.Errorf("HEXISTS command requires at least one argument: HEXISTS <hashmap> [<key>]")
		return
	}

	ns := ctx.Args[0]

	if len(ctx.Args) > 1 {
		ctx.Args = []string{ns + "/{HASH}/" + ctx.Args[1]}
		cmdExists(ctx)
		return
	}

	found := 0
	prefix := ns + "/{HASH}/"

	ctx.DB.Scan(store.ScannerOptions{
		Prefix: prefix,
		Offset: prefix,
		Handler: func(_, _ string) bool {
			found++
			return false
		},
	})

	ctx.Integer = int64(found)
}

func cmdHlen(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = fmt.Errorf("HLEN command requires at least one argument: HLEN <hashmap>")
		return
	}

	found := 0
	prefix := ctx.Args[0] + "/{HASH}/"

	err := ctx.DB.Scan(store.ScannerOptions{
		FetchValues:   false,
		IncludeOffset: true,
		Prefix:        prefix,
		Offset:        prefix,
		Handler: func(_, _ string) bool {
			found++
			return true
		},
	})

	if err != nil {
		ctx.Err = err
		return
	}

	ctx.Integer = int64(found)
}

func cmdHinc(ctx *RequestContext) {
	if len(ctx.Args) < 2 {
		ctx.Err = fmt.Errorf("HINCR command must has at least two arguments: HINCR <hash> <key> [number]")
		return
	}

	ns, key, by := ctx.Args[0], ctx.Args[1], ""

	if len(ctx.Args) > 2 {
		by = ctx.Args[2]
	}

	ctx.Args = []string{ns + "/{HASH}/" + key, by}

	cmdIncr(ctx)
}

func cmdHttl(ctx *RequestContext) {
	if len(ctx.Args) < 2 {
		ctx.Err = fmt.Errorf("HTTL command requires at least 2 arguments HTTL <HASHMAP> <key>")
		return
	}
	t := ctx.DB.TTL(ctx.Args[0] + "/{HASH}/" + ctx.Args[1])

	ctx.Integer = t
}