package redisdb

import "github.com/gottingen/crab/store"


type RequestContext struct {
	DB      store.Store
	Action  string
	Args    []string

	Array   []string
	Integer int64
	Str     string
	Err     error
}

type Commandhandler func(RequestContext)
