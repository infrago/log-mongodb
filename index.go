package log_mongodb

import (
	"github.com/infrago/infra"
	"github.com/infrago/log"
)

func Driver() log.Driver {
	return &mongodbDriver{}
}

func init() {
	infra.Register("mongodb", Driver())
}
