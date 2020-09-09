package catalog

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
	"testing"
)

func TestCataloger_CreateMergeJob(t *testing.T) {
	ctx := context.Background()
	conn, _ := sqlx.Connect("pgx", "postgres://tzahij:sa@localhost:5432/mvcc_new?search_path=public")
	cdb := db.NewSqlxDatabase(conn)
	//conf := config.NewConfig()
	c := NewCataloger(cdb)
	//c := NewCataloger(cdb, conf.GetBatchReadParams())
	diff, stat, err := c.Diff(ctx, "example", "m3", "m2", 50, "")
	_ = err
	fmt.Print(stat)
	_ = diff

}
