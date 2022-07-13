package ignite

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/LazarenkoA/migrate/database"
	"github.com/amsokol/ignite-go-client/binary/v1"
	"go.uber.org/atomic"
)

const (
	cacheName   = "migrate"
	SCHEMA_NAME = "PUBLIC"
)

type Config struct {
	MigrationsTable  string
	StatementTimeout time.Duration
	Host             string
	Port             string
	Scheme           string
	Username         string
	Password         string
}

type Ignite struct {
	isLocked atomic.Bool
	client   ignite.Client
	Cfg      *Config
}

func init() {
	database.Register("ignite", &Ignite{})
}

func ConnectDB(cfg *Config) (database.Driver, error) {
	return (&Ignite{Cfg: cfg}).Open("")
}

func (I *Ignite) Close() error {
	I.client.CacheDestroy(cacheName)
	return I.client.Close()
}

func (I *Ignite) Open(url string) (database.Driver, error) {
	if I.Cfg == nil {
		return nil, errors.New("cfg variable is not initialized")
	} else if I.Cfg.MigrationsTable == "" {
		return nil, errors.New("migrationsTable must be filled")
	} else if I.Cfg.Host == "" {
		return nil, errors.New("host must be filled")
	}

	port, err := strconv.Atoi(I.Cfg.Port)
	if err != nil {
		return nil, fmt.Errorf("bad port: %w", err)
	}

	//username := purl.Query().Get("Username")
	//if username = purl.Query().Get("Username"); username == "" {
	//	return nil, errors.New("username must be filled")
	//}

	//password := purl.Query().Get("Password")
	//if password := purl.Query().Get("Password"); password == "" {
	//	return nil, errors.New("password must be filled")
	//}

	if I.Cfg.Scheme == "" {
		return nil, errors.New("scheme must be filled")
	}

	I.client, err = ignite.Connect(ignite.ConnInfo{
		Network:  I.Cfg.Scheme,
		Host:     I.Cfg.Host,
		Port:     port,
		Major:    1,
		Minor:    1,
		Username: I.Cfg.Username,
		Password: I.Cfg.Password,
		Dialer: net.Dialer{
			Timeout: I.Cfg.StatementTimeout,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connect error: %w", err)
	}

	if err = I.ensureVersionTable(); err != nil {
		return nil, fmt.Errorf("error created VersionTable: %w", err)
	}

	return I, nil
}

func (I *Ignite) Lock() error {
	return database.CasRestoreOnErr(&I.isLocked, false, true, database.ErrNotLocked, func() error { return nil })
}

func (I *Ignite) Unlock() error {
	return database.CasRestoreOnErr(&I.isLocked, true, false, database.ErrNotLocked, func() error { return nil })
}

func (I *Ignite) Run(migration io.Reader) error {
	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}
	query := strings.TrimSpace(string(migr))
	if query == "" {
		return nil
	}

	_, err = I.QuerySQLFields(query)
	return err
}

func (I *Ignite) SetVersion(version int, dirty bool) error {
	query := `DELETE FROM ` + I.Cfg.MigrationsTable // TRUNCATE not support
	_, err := I.QuerySQLFields(query)
	if err != nil {
		return fmt.Errorf("error truncate: %w", err)
	}

	query = "INSERT INTO " + I.Cfg.MigrationsTable + " (version, dirty) VALUES (?, ?)"
	_, err = I.QuerySQLFields(query, version, dirty)

	return err
}

func (I *Ignite) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + I.Cfg.MigrationsTable + ` LIMIT 1`
	res, err := I.QuerySQLFields(query)

	if err != nil {
		return 0, false, err
	}

	if len(res.QuerySQLFieldsPage.Rows) == 0 || len(res.QuerySQLFieldsPage.Rows[0]) == 0 {
		return database.NilVersion, false, nil
	}

	return int(res.QuerySQLFieldsPage.Rows[0][0].(int64)), res.QuerySQLFieldsPage.Rows[0][1].(bool), err
}

func (I *Ignite) Drop() error {
	query := "SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = 'PUBLIC'"
	req, err := I.QuerySQLFields(query)
	if err != nil {
		return err
	}

	dropTable := func(tableName string) error {
		query := "DROP TABLE IF EXISTS " + tableName
		_, err := I.QuerySQLFields(query)
		return err
	}

	for _, row := range req.QuerySQLFieldsPage.Rows {
		if len(row) > 0 {
			tabName := row[0].(string)
			if err := dropTable(tabName); err != nil {
				return fmt.Errorf("drop error table %q: %w", tabName, err)
			}
		}
	}

	return nil
}

func (I *Ignite) ensureVersionTable() error {
	I.client.CacheCreateWithName(cacheName)

	query := `CREATE TABLE IF NOT EXISTS ` + I.Cfg.MigrationsTable + ` (version bigint not null primary key, dirty boolean not null)`
	_, err := I.QuerySQLFields(query)
	return err
}

func (I *Ignite) QuerySQLFields(query string, queryArgs ...interface{}) (ignite.QuerySQLFieldsResult, error) {
	return I.client.QuerySQLFields(cacheName, false, ignite.QuerySQLFieldsData{
		Schema:           SCHEMA_NAME,
		PageSize:         10,
		Query:            query,
		QueryArgs:        queryArgs,
		DistributedJoins: true,
	})
}
