package ignite

import (
	"fmt"
	"testing"
	"time"

	"github.com/dhui/dktest"
	"github.com/golang-migrate/migrate/v4"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	"github.com/golang-migrate/migrate/v4/dktesting"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	opts  = dktest.Options{PortRequired: true}
	specs = []dktesting.ContainerSpec{
		{ImageName: "pacheignite/ignite", Options: opts},
	}
)

func TestMigrate(t *testing.T) {
	addr := fmt.Sprintf("ignite://%s:%s?&Scheme=tcp", "localhost", "10800")
	d, err := (&Ignite{
		Cfg: &Config{
			MigrationsTable:  "schema_migrations",
			StatementTimeout: 10 * time.Second,
		},
	}).Open(addr)

	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()

	m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "", d)
	if err != nil {
		t.Fatal(err)
	}
	dt.TestMigrate(t, m)

	// todo: пока так, надо будетпотом разобраться
	return
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}
		addr := fmt.Sprintf("ignite://%s:%s?Scheme=tcp", ip, port)
		d, err := new(Ignite).Open(addr)

		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "", d)
		if err != nil {
			t.Fatal(err)
		}
		dt.TestMigrate(t, m)
	})
}
