package main

import (
	"flag"
	"fmt"
	"github.com/Clever/go-utils/flagutil"
	"github.com/Clever/redshifter/postgres"
	"github.com/Clever/redshifter/redshift"
	"github.com/facebookgo/errgroup"
	"github.com/segmentio/go-env"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	awsRegion      = env.MustGet("AWS_REGION")
	s3prefix       = flagutil.RequiredStringFlag("s3prefix", "s3 path to be used as a prefix for temporary storage of postgres data", nil)
	tablesCSV      = flagutil.RequiredStringFlag("tables", "Tables to copy as CSV", nil)
	dumppg         = flag.Bool("dumppostgres", true, "Whether to dump postgres")
	updateRS       = flag.Bool("updateredshift", true, "Whether to replace redshift")
	redshiftSchema = flag.String("redshiftschema", "public", "Schema name to store the tables.")
)

type TableInfo struct {
	Size int
}

type TableInfos []*TableInfo

func (tis *TableInfos) New() interface{} {
	ti := &TableInfo{}
	*tis = append(*tis, ti)
	return ti
}

func S3Filename(prefix string, table string) string {
	return prefix + table + ".txt.gz"
}

func main() {
	flag.Parse()
	if err := flagutil.ValidateFlags(nil); err != nil {
		log.Fatal(err.Error())
	}
	tables := strings.Split(*tablesCSV, ",")

	pgdb := postgres.NewDB(postgres.Config{PoolSize: 10})
	defer pgdb.Close()
	tsmap, err := pgdb.GetTableSchemas(tables, "")
	if err != nil {
		log.Fatal(err)
	}
	if *dumppg {
		var tableInfos TableInfos
		query := fmt.Sprintf(`SELECT id AS size FROM %s ORDER BY id DESC limit 1;`, tables[0])
		_, err := pgdb.Query(&tableInfos, query)
		if err != nil {
			panic(err)
		}

		table := tables[0]
		splits := 5
		batchSize := tableInfos[0].Size / splits
		group := new(errgroup.Group)
		for i := 0; i < splits; i++ {
			min := i * batchSize
			max := min + batchSize
			statement := fmt.Sprintf("(SELECT * FROM %s WHERE %d < id AND id < %d)", table, min, max)
			path := *s3prefix + table + strconv.Itoa(i) + ".txt.gz"
			log.Println(path)
			if err := pgdb.DumpTableToS3(statement, path); err != nil {
				panic(err)
			}
		}
	}
	if *updateRS {
		r, err := redshift.NewRedshift()
		defer r.Close()
		if err != nil {
			log.Fatal(err)
		}
		tmpSchema := fmt.Sprintf("temp_schema_%s", time.Now().Unix())
		if err := r.RefreshTables(tsmap, *redshiftSchema, tmpSchema, *s3prefix, awsRegion, '|'); err != nil {
			log.Fatal(err)
		}
	}
}
