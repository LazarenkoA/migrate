# pkger
```go
package main

import (
	"errors"
	"log"

	"github.com/LazarenkoA/migrate"
	"github.com/markbates/pkger"

	_ "github.com/LazarenkoA/migrate/database/postgres"
	_ "github.com/LazarenkoA/migrate/source/pkger"
	_ "github.com/lib/pq"
)

func main() {
	pkger.Include("/module/path/to/migrations")
	m, err := migrate.New("pkger:///module/path/to/migrations", "postgres://postgres@localhost/postgres?sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}
	if err := m.Up(); errors.Is(err, migrate.ErrNoChange) {
		log.Println(err)
	} else if err != nil {
		log.Fatalln(err)
	}
}
```
