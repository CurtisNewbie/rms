package main

import (
	"flag"
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	host     = flag.String("host", "localhost", "host")
	password = flag.String("password", "", "password")
	user     = flag.String("user", "", "username")
	port     = flag.Int("port", 3306, "port")
	from     = flag.String("from", "", "from schema name")
	to       = flag.String("to", "", "to schema name")
)

func main() {
	flag.Parse()
	if err := FlagsMustPresent("from", "to"); err != nil {
		fmt.Println(err)
		return
	}
	db, err := OpenConn(*host, *user, *password, *port)
	if err != nil {
		fmt.Printf("Failed to connect database, %v", err)
		return
	}
	if err := MigrateSchema(db, *from, *to); err != nil {
		fmt.Printf("Failed to migrate schema from %v to %v, %v\n", *from, *to, err)
		return
	}
	fmt.Printf("Finished migrating schema from %v to %v\n", *from, *to)
}

func MigrateSchema(db *gorm.DB, from string, to string) error {
	tables, err := ListTables(db, from)
	if err != nil {
		return fmt.Errorf("failed to list tables in %v, %v", from, err)
	}

	if err := CreateDatabase(db, to); err != nil {
		return fmt.Errorf("failed to create database %v", err)
	}

	for _, t := range tables {
		if err := CopyTableStruct(db, from, to, t); err != nil {
			return fmt.Errorf("failed to copy table structure, %v, %v", t, err)
		}
	}

	for _, t := range tables {
		if err := CopyTableData(db, from, to, t); err != nil {
			return fmt.Errorf("failed to copy table structure, %v, %v", t, err)
		}
	}

	return nil
}

func CopyTableStruct(db *gorm.DB, from string, to string, table string) error {
	return db.Exec(fmt.Sprintf(`CREATE TABLE %v.%v LIKE %v.%v`, to, table, from, table)).Error
}

func CopyTableData(db *gorm.DB, from string, to string, table string) error {
	return db.Exec(fmt.Sprintf(`INSERT INTO %v.%v SELECT * FROM %v.%v`, to, table, from, table)).Error
}

func FlagsMustPresent(names ...string) error {
	for _, n := range names {
		if err := FlagMustPresent(n); err != nil {
			return err
		}
	}
	return nil
}

func FlagMustPresent(name string) error {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	if !found {
		return fmt.Errorf("please specify -%v", name)
	}
	return nil
}

func OpenConn(host string, user string, password string, port int) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port)
	conn, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	return conn.Debug(), nil
}

func ListTables(db *gorm.DB, schema string) ([]string, error) {
	var tables []string
	return tables, db.Raw(fmt.Sprintf(`show tables in %v`, schema)).Scan(&tables).Error
}

func CreateDatabase(db *gorm.DB, schema string) error {
	return db.Exec(fmt.Sprintf(`create database if not exists %v`, schema)).Error
}
