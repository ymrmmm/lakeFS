package catalog

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
	//"strconv"
	"testing"
	"time"
	//ontext"
)

const RowNumber = 1_000_000

func Test_db_branch_reader(t *testing.T) {
	//x := context.Background()
	conn, _ := sqlx.Connect("pgx", "postgres://tzahij:sa@localhost:5432/mvcc_new?search_path=public")
	cdb := db.NewSqlxDatabase(conn)
	//conf := config.NewConfig()
	//:= NewCataloger(cdb)
	//c := NewCataloger(cdb, conf.GetBatchReadParams())
	_, err := cdb.Transact(func(tx db.Tx) (interface{}, error) {
		start := time.Now()
		lr := newLineageReader(tx, 1, 0, 1024, RowNumber, "")
		for i := 0; i < RowNumber+2; i++ {
			r, err := lr.getNextPK()
			if err != nil {
				panic(err)
			}
			if r == nil {
				e := time.Since(start)
				fmt.Printf("reading %d took %s\n", RowNumber, e)
				break
			}
			_ = r
			//fmt.Print(*r.Path,"\n")
		}
		return nil, nil
	}, db.ReadOnly())
	if err != nil {
		panic(err)
	}
}
