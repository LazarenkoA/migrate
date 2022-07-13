package ignite

import (
	"testing"
	"time"

	"github.com/LazarenkoA/migrate"
	dt "github.com/LazarenkoA/migrate/database/testing"
	"github.com/LazarenkoA/migrate/dktesting"
	_ "github.com/LazarenkoA/migrate/source/file"
	"github.com/dhui/dktest"
)

var (
	opts  = dktest.Options{PortRequired: true}
	specs = []dktesting.ContainerSpec{
		{ImageName: "pacheignite/ignite", Options: opts},
	}
)

func TestMigrate(t *testing.T) {
	d, err := ConnectDB(&Config{
		MigrationsTable:  "schema_migrations",
		StatementTimeout: 10 * time.Second,
		Host:             "localhost",
		Port:             "10800",
		Scheme:           "tcp",
	})

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

	// todo: пока так, надо будет потом разобраться c тестированием через докер
	return
	dktesting.ParallelTest(t, specs, func(t *testing.T, c dktest.ContainerInfo) {
		ip, port, err := c.FirstPort()
		if err != nil {
			t.Fatal(err)
		}
		d, err := ConnectDB(&Config{
			MigrationsTable:  "schema_migrations",
			StatementTimeout: 10 * time.Second,
			Host:             ip,
			Port:             port,
			Scheme:           "tcp",
		})

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
