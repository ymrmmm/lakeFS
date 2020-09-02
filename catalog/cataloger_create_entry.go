package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateEntry(ctx context.Context, repository, branch string, entry Entry, params CreateEntryParams) (string, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "path", IsValid: ValidatePath(entry.Path)},
	}); err != nil {
		return "", err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}

		// dedup if needed
		entryAddress := entry.PhysicalAddress
		if dedupAddress, err := c.createEntryDedup(tx, params, repository, entry.PhysicalAddress); err != nil {
			return nil, err
		} else if dedupAddress != "" {
			// write dedup address to the catalog
			entryAddress = dedupAddress
		}

		// insert or update the entry
		creationDate := entry.CreationDate
		if creationDate.IsZero() {
			creationDate = time.Now()
		}

		_, err = tx.Exec(`INSERT INTO catalog_entries (branch_id,path,physical_address,checksum,size,metadata,creation_date,is_expired)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, creation_date=EXCLUDED.creation_date, is_expired=EXCLUDED.is_expired, max_commit=catalog_max_commit_id()`,
			branchID, entry.Path, entryAddress, entry.Checksum, entry.Size, entry.Metadata, creationDate, entry.Expired)
		if err != nil {
			return nil, fmt.Errorf("insert entry: %w", err)
		}
		// return the real physical address
		return entryAddress, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}

func (c *cataloger) createEntryDedup(tx db.Tx, params CreateEntryParams, repository string, physicalAddress string) (string, error) {
	if params.Dedup.ID == "" {
		return "", nil
	}
	repoID, err := c.getRepositoryIDCache(tx, repository)
	if err != nil {
		return "", err
	}

	res, err := tx.Exec(`INSERT INTO catalog_object_dedup (repository_id, dedup_id, physical_address) values ($1, decode($2,'hex'), $3)
				ON CONFLICT DO NOTHING`,
		repoID, params.Dedup.ID, physicalAddress)
	if err != nil {
		return "", err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return "", err
	}
	if rowsAffected == 1 {
		return "", nil
	}
	// get the existing address
	var address string
	err = tx.Get(&address, `SELECT physical_address FROM catalog_object_dedup WHERE repository_id=$1 AND dedup_id=decode($2,'hex')`,
		repoID, params.Dedup.ID)
	return address, err
}

func insertEntry(tx db.Tx, branchID int64, entry *Entry) (string, error) {
	var (
		ctid   string
		dbTime sql.NullTime
	)
	if entry.CreationDate.IsZero() {
		dbTime.Valid = false
	} else {
		dbTime.Time = entry.CreationDate
		dbTime.Valid = true
	}
	err := tx.Get(&ctid, `INSERT INTO catalog_entries (branch_id,path,physical_address,checksum,size,metadata,creation_date,is_expired)
                        VALUES ($1,$2,$3,$4,$5,$6, COALESCE($7, NOW()), $8)
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=$3, checksum=$4, size=$5, metadata=$6, creation_date=EXCLUDED.creation_date, is_expired=EXCLUDED.is_expired, max_commit=catalog_max_commit_id()
			RETURNING ctid`,
		branchID, entry.Path, entry.PhysicalAddress, entry.Checksum, entry.Size, entry.Metadata, dbTime, entry.Expired)
	if err != nil {
		return "", fmt.Errorf("insert entry: %w", err)
	}
	return ctid, nil
}
