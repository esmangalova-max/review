package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/LindsayBradford/go-dbf/godbf"
	"github.com/go-faster/errors"
	"github.com/google/uuid"

	"ParseReestrLZP/cnfg"
	"ParseReestrLZP/dbase"
	"ParseReestrLZP/sqlScripts"
	"ParseReestrLZP/unarch"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/schollz/progressbar/v3"
)

func HandleSz(arcname string, zf *dbase.ZFiles) error {
	data, err := unarch.Decopress(arcname, zf)
	if err != nil {
		return errors.Errorf("unarch.UnarcSZ(%q): %w", arcname, err)
	}
	szlData := (*data)["sz_l.dbf"]
	err = SaveSzL(&szlData, zf)
	if err != nil {
		return errors.Errorf("SaveSzL(%q): %w", arcname, err)
	}
	szpData := (*data)["sz_p.dbf"]
	err = SaveSzP(&szpData, zf)
	if err != nil {
		return errors.Errorf("SaveSzP(%q): %w", arcname, err)
	}
	err = MoveSZToArchWOTran()
	if err != nil {
		return errors.Errorf("MoveSZToArch(%q): %w", arcname, err)
	}
	err = TmpToCurrent()
	if err != nil {
		return errors.Errorf("TmpToCurrent(): %w", err)
	}

	err = RefreshPersonList()
	if err != nil {
		return errors.Errorf("RefreshPersonList(): %w", err)
	}

	err = AddFilesRecord(zf, 0)
	if err != nil {
		return errors.Errorf("AddFilesRecord(%q): %w", arcname, err)
	}
	return nil
}

func MoveSZToArchWOTran() error {
	sqlSelectRegId := fmt.Sprintf("select distinct registr_id from  %s.szl_curent order by  UUIDv7ToDateTime(registr_id) desc", cnfg.Cnfg.CDb)
	chd := *cnfg.Cnfg.CH
	var regId uuid.UUID
	ctx := context.Background()
	err := chd.QueryRow(ctx, sqlSelectRegId).Scan(&regId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return errors.Errorf("QueryRow failed: %w", err)
	}

	sqlChkExists := fmt.Sprintf(`select exists(select toUInt8(1) from %s.szl_arch where registr_id = toUUID('%v')) ex`, cnfg.Cnfg.CDb, regId)
	var regExistsInArch uint8
	err = chd.QueryRow(ctx, sqlChkExists).Scan(&regExistsInArch)
	if err != nil {
		return errors.Errorf("QueryRow (check exists registr_id) failed: %w", err)
	}
	if regExistsInArch == 1 {
		fmt.Println("Already moved to arch")
		return nil
	}

	err = chd.Exec(ctx, fmt.Sprintf(sqlScripts.CopySZLToArch, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb))

	if err != nil {
		return errors.Errorf("CopySZLToArch : %w", err)
	}
	err = chd.Exec(ctx, fmt.Sprintf(sqlScripts.CopySZPToArch, cnfg.Cnfg.CDb, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("CopySZPToArch : %w", err)
	}
	err = chd.Exec(ctx, fmt.Sprintf(sqlScripts.TruncateSZL, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("TruncateSZL : %w", err)
	}
	err = chd.Exec(ctx, fmt.Sprintf(sqlScripts.TruncateSZP, cnfg.Cnfg.CDb))
	if err != nil {
		return errors.Errorf("TruncateSZP : %w", err)
	}

	return nil
}
