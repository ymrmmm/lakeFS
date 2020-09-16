package catalog

import (
	//"fmt"
	//sq "github.com/Masterminds/squirrel"

	"context"
	//sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
	//"strconv"
	"testing"
	//"time"
)

//const RowNumber = 10_000_000
//const BranchID = 4
func Test_db_big_diff(t *testing.T) {
	ctx := context.Background()
	conn, _ := sqlx.Connect("pgx", "postgres://tzahij:sa@localhost:5432/mvcc_new?search_path=public")
	cdb := db.NewSqlxDatabase(conn)
	//conf := config.NewConfig()
	c := NewCataloger(cdb)
	//c := NewCataloger(cdb, conf.GetBatchReadParams())
	d, b, err := c.Diff(ctx, "example", "m3", "m2", 100_000, "")
	_ = d
	_ = b
	if err != nil {
		panic(err)
	}
}
