package redisdb

import (
	"errors"
	"github.com/gottingen/crab/store"
	"regexp"
	"strconv"
)

var (
	ErrNoTTL = errors.New("SET command requires at least two arguments: SET <key> <value> [TTL Millisecond]")
	ErrNoKey = errors.New("GET command must have at least 1 argument: GET <key> [default value]")
	ErrKVNum = errors.New("MSET command arguments must be even")
)

func cmdSet(ctx *RequestContext) {
	var k, v, ttl string
	if len(ctx.Args) < 2 {
		ctx.Err = ErrNoTTL
		return
	}

	k, v = ctx.Args[0], ctx.Args[1]

	if len(ctx.Args) > 2 {
		ttl = ctx.Args[2]
	}

	ttlVal, _ := strconv.Atoi(ttl)
	if ttlVal < 0 {
		ttlVal = 0
	}

	if err := ctx.DB.Set(k, v, ttlVal); err != nil {
		ctx.Err = err
		return
	}
	ctx.Str = "OK"
}

func cmdGet(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = ErrNoKey
		return
	}

	defaultVal := ""
	data, err := ctx.DB.Get(ctx.Args[0])

	if len(ctx.Args) > 1 {
		defaultVal = ctx.Args[1]
	}

	if err != nil {
		if defaultVal != "" {
			ctx.Str = defaultVal
		}
		ctx.Err = err
		return
	}

	ctx.Str = data
}

func cmdMget(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = ErrNoKey
		return
	}

	data := ctx.DB.MGet(ctx.Args)
	ctx.Array = data
}

func cmdMset(ctx *RequestContext) {
	currentCount := len(ctx.Args)
	if currentCount%2 != 0 {

		return
	}

	data := map[string]string{}

	for i, v := range ctx.Args {
		index := i + 1
		if index%2 == 0 {
			data[ctx.Args[i-1]] = v
		} else {
			data[ctx.Args[i]] = ""
		}
	}

	if err := ctx.DB.MSet(data); err != nil {
		ctx.Err = err
		return
	}

	ctx.Integer = int64(len(data))
}

func cmdDel(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = ErrNoKey
		return
	}

	if err := ctx.DB.Del(ctx.Args); err != nil {
		ctx.Err = err
		return
	}

	ctx.Str = "OK"
}


// existsCommand - Exists <key>
func cmdExists(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = ErrNoKey
		return
	}

	_, err := ctx.DB.Get(ctx.Args[0])
	if err != nil {
		ctx.Integer  = 0
		return
	}

	ctx.Integer = 1
}


func cmdIncr(ctx *RequestContext) {
	var key string
	var by int64

	if len(ctx.Args) < 1 {
		ctx.Err = ErrNoKey
		return
	}

	key = ctx.Args[0]

	if len(ctx.Args) > 1 {
		by, _ = strconv.ParseInt(ctx.Args[1], 10, 64)
	}

	if by == 0 {
		by = 1
	}

	val, err := ctx.DB.Incr(key, by)
	if err != nil {
		ctx.Err = err
		return
	}

	ctx.Integer = val
}


// ttlCommand - TTL <key>
func cmdTtl(ctx *RequestContext) {
	if len(ctx.Args) < 1 {
		ctx.Err = ErrNoKey
		return
	}
	t := ctx.DB.TTL(ctx.Args[0])
	ctx.Integer = t
}


// keysCommand - KEYS [<regexp-pattern>]
func cmdKeys(ctx *RequestContext) {
	var data []string
	var pattern *regexp.Regexp
	var err error

	if len(ctx.Args) > 0 {
		pattern, err = regexp.CompilePOSIX(ctx.Args[0])
	}

	if err != nil {
		ctx.Err =err
		return
	}

	err = ctx.DB.Scan(store.ScannerOptions{
		FetchValues:   false,
		IncludeOffset: true,
		Handler: func(k, _ string) bool {
			if pattern != nil && pattern.MatchString(k) {
				data = append(data, k)
			} else if nil == pattern {
				data = append(data, k)
			}
			return true
		},
	})

	if err != nil {
		ctx.Err = err
		return
	}

	ctx.Array = data

}
