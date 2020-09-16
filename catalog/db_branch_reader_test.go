package catalog

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
	"strconv"
	"testing"
)

const (
	ObjNumber = 100
)

var ObjSkip = []int{1, 2, 3, 5, 7, 11}
var bufferSizes = []int{1, 2, 8, 64, 512, 1024 * 4}

func TestCataloger_db_branch_reader(t *testing.T) {
	ctx := context.Background()
	conn, uri := testutil.GetDB(t, databaseURI)
	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}
	baseBranchName := "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)
	maxBranchNumber := len(ObjSkip)

	for branchNo := 0; branchNo < maxBranchNumber; branchNo++ {
		branchName := "b" + strconv.Itoa(branchNo)
		if branchNo > 0 {
			testCatalogerBranch(t, ctx, c, repository, branchName, baseBranchName)
		}
		for i := 0; i < ObjNumber; i += ObjSkip[branchNo] {
			testCatalogerCreateEntry(t, ctx, c, repository, branchName, fmt.Sprintf("Obj-%04d", i), nil, "")
		}
		_, err := c.Commit(ctx, repository, branchName, "commit to "+branchName, "tester", nil)
		testutil.MustDo(t, "commit to "+branchName, err)
		baseBranchName = branchName
	}

	//_,_ = conn.Transact(func(tx db.Tx) (interface{}, error) {
	//var p string
	//// test different cache sizes
	//for k := 0;k < len(bufferSizes);k++{
	//	bufSize := bufferSizes[k]
	//// test single branch reader
	//for branchNo := 0;branchNo < maxBranchNumber;branchNo++ {
	//	branchName := "b" + strconv.Itoa(branchNo)
	//	branchReader := NewSingleBranchReader(tx, int64(branchNo+1),UncommittedID,bufSize,"")
	//	objSkipNo := ObjSkip[branchNo]
	//	for i := 0;; i += objSkipNo{
	//		o,err := branchReader.getNextPK()
	//		testutil.MustDo(t, "read from branch " + branchName, err)
	//		if o == nil{
	//			if !(i - objSkipNo < ObjNumber &&  i  >= ObjNumber){
	//				t.Fatalf("terminated at i=%d",i)
	//			}
	//			break
	//		} else {
	//			p = *o.Path
	//			objNum,err := strconv.Atoi(p[4:])
	//			testutil.MustDo(t, "convert obj number " + p, err)
	//			if objNum != i{
	//				t.Errorf(" objNum=%d, i=%d\n",objNum,i)
	//			}
	//		}
	//	}
	//}
	//// test lineage reader
	//for branchNo := 0;branchNo < maxBranchNumber;branchNo++ {
	//	branchName := "b" + strconv.Itoa(branchNo)
	//	lineageReader := newLineageReader(tx, int64(branchNo+1),UncommittedID,bufSize,0,"")
	//	for i := 0;i < ObjNumber;i++{
	//		var expectedBranch int64
	//		o,err := lineageReader.getNextPK()
	//		testutil.MustDo(t, "read from lineage " + branchName, err)
	//		if o == nil{
	//			t.Errorf("Got nil obj, branch=%s, numbr=%d\n",branchName,i)
	//		}
	//		// check item read from might branch
	//		for j := branchNo; j >= 0;j--{
	//			if i%ObjSkip[j] == 0{
	//				expectedBranch = int64(j + 1)
	//				break
	//			}
	//		}
	//		if o.BranchID != expectedBranch{
	//			t.Errorf("fetch from wrong branch.branchName=%s branchNumber=%d, i =%d\n",branchName,o.BranchID,i)
	//		}
	//	}
	//}
	// test reading committed and uncomitted data
	bufSize := 8
	testCatalogerCreateEntry(t, ctx, c, repository, "b1", "Obj-0004", nil, "sd")
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageReader_b1_U := newLineageReader(tx, 2, UncommittedID, bufSize, 0, "Obj-0004")
		lineageReader_b1_C := newLineageReader(tx, 2, CommittedID, bufSize, 0, "Obj-0004")
		lineageReader_b2_U := newLineageReader(tx, 3, UncommittedID, bufSize, 0, "Obj-0004")
		lineageReader_b2_C := newLineageReader(tx, 3, CommittedID, bufSize, 0, "Obj-0004")
		readAndTest(t, lineageReader_b1_U, "Obj-0004", "read 0004 lineage b1 U ", 2, 0)
		readAndTest(t, lineageReader_b2_U, "Obj-0004", "read 0004 lineage b1 U ", 2, 4)
		readAndTest(t, lineageReader_b1_C, "Obj-0004", "read 0004 lineage b1 U ", 2, 4)
		readAndTest(t, lineageReader_b2_C, "Obj-0004", "read 0004 lineage b1 U ", 2, 4)
		return nil, nil
	})
	_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
	testutil.MustDo(t, "commit to b1", err)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageReader_b1_U := newLineageReader(tx, 2, UncommittedID, bufSize, 0, "Obj-0004")
		lineageReader_b1_C := newLineageReader(tx, 2, CommittedID, bufSize, 0, "Obj-0004")
		lineageReader_b2_U := newLineageReader(tx, 3, UncommittedID, bufSize, 0, "Obj-0004")
		lineageReader_b2_C := newLineageReader(tx, 3, CommittedID, bufSize, 0, "Obj-0004")
		readAndTest(t, lineageReader_b1_U, "Obj-0004", "read 0004 lineage b1 U ", 2, 13)
		readAndTest(t, lineageReader_b1_C, "Obj-0004", "read 0004 lineage b1 U ", 2, 13)
		readAndTest(t, lineageReader_b2_U, "Obj-0004", "read 0004 lineage b1 U ", 2, 4)
		readAndTest(t, lineageReader_b2_C, "Obj-0004", "read 0004 lineage b1 U ", 2, 4)
		return nil, nil
	})
	_, err = c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
	testutil.MustDo(t, "merge b1 into b2", err)
	testutil.MustDo(t, "commit to b1", err)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageReader_b2_U := newLineageReader(tx, 3, UncommittedID, bufSize, 0, "Obj-0004")
		lineageReader_b2_C := newLineageReader(tx, 3, CommittedID, bufSize, 0, "Obj-0004")
		readAndTest(t, lineageReader_b2_U, "Obj-0004", "read 0004 lineage b1 U ", 2, 13)
		readAndTest(t, lineageReader_b2_C, "Obj-0004", "read 0004 lineage b1 U ", 2, 13)
		return nil, nil
	})

}

func readAndTest(t *testing.T, lReader *lineageReader, expPath string, msg string, expBranch, expMinCommit int) {
	o, err := lReader.getNextPK()
	testutil.MustDo(t, msg, err)
	if o.BranchID != int64(expBranch) || o.MinCommit != CommitID(expMinCommit) {
		t.Errorf(msg+" branch=%d, min_commit=%d", o.BranchID, o.MinCommit)
	}
}
