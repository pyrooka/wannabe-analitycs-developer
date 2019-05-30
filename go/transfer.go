package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// MySQL timestamp format.
const datetimeLayout = "2006-01-02 15:04:05"

// Print or not to print SQL queries.
var isDebug = false

// The config file.
type config struct {
	SourceDSN string `json:"sourceDB"`
	TargetDSN string `json:"targetDB"`
}

// To the target DB.
type event struct {
	last   time.Time
	metric string
	count  int
}

// logDebug print the text to the log output if isDebug is true.
func logDebug(text string) {
	if isDebug {
		log.Println(text)
	}
}

// parseConfig reads and parse the config JSON file.
func parseConfig() (conf config, err error) {
	content, err := ioutil.ReadFile("config.json")
	if err != nil {
		return
	}

	err = json.Unmarshal(content, &conf)
	return
}

// isTableExists checks is the table in the DB or not.
func isTableExists(db *sql.DB, tableName string) bool {
	query := fmt.Sprintf("SELECT 1 FROM %s LIMIT 1;", tableName)
	logDebug(query)

	_, err := db.Exec(query)
	if err != nil {
		return false
	}

	return true
}

// createTablesIfNotExist checks the tables in the provided DB and creates them if aren't there.
func createTablesIfNotExist(db *sql.DB) (err error) {
	if !isTableExists(db, "metrics") {
		query := `CREATE TABLE metrics(
			id SERIAL PRIMARY KEY NOT null,
			name VARCHAR(20) NOT null
			);`
		logDebug(query)

		_, err = db.Exec(query)
		if err != nil {
			return err
		}
	}
	if !isTableExists(db, "events") {
		query := `CREATE TABLE events(
			id SERIAL PRIMARY KEY NOT NULL,
			day_last TIMESTAMP NOT NULL,
			count INT NOT NULL,
			metric_id INT,
			FOREIGN KEY(metric_id) REFERENCES metrics(id) ON DELETE CASCADE
			);`
		logDebug(query)

		_, err = db.Exec(query)
		if err != nil {
			return err
		}
	}

	return
}

// getLastMetricsTime query the DB for the last times for each metrics occured.
func getLastMetricsTime(db *sql.DB) (lasts map[string]time.Time, err error) {
	query := `SELECT metrics.name, max(day_last) as last
		FROM events
		INNER JOIN metrics ON events.metric_id = metrics.id
		GROUP BY name;`
	logDebug(query)

	res, err := db.Query(query)
	if err != nil {
		return
	}
	defer res.Close()

	lasts = map[string]time.Time{}

	for res.Next() {
		var name, temp string
		var last time.Time

		err = res.Scan(&name, &temp)
		if err != nil {
			return
		}

		last, err = time.Parse(time.RFC3339, temp)
		if err != nil {
			return nil, err
		}

		lasts[name] = last
	}

	return
}

// getOrCreateMetric returns the ID of a metric by name or create a new one.
func getOrCreateMetric(db *sql.DB, name string) (id int, err error) {
	query := fmt.Sprintf("SELECT id FROM metrics WHERE name = '%s';", name)
	logDebug(query)

	err = db.QueryRow(query).Scan(&id)
	if err == sql.ErrNoRows {
		err = nil

		query = fmt.Sprintf("INSERT INTO metrics (name) VALUES ('%s');", name)
		logDebug(query)

		_, err := db.Exec(query)
		if err != nil {
			return 0, err
		}

		id, err = getOrCreateMetric(db, name)
	}

	return
}

func main() {
	// Get the CLI flag.
	isDebugPtr := flag.Bool("d", false, "log SQL queries to stdout")
	flag.Parse()
	isDebug = *isDebugPtr

	// Parse the config file if exists.
	conf, err := parseConfig()
	if os.IsNotExist(err) {
		log.Println("config.json not found. Continue with default values.")
	} else if err != nil {
		log.Fatal(err)
	}

	// Prepare the data source names.
	var dsnSource, dsnTarget string
	if conf.SourceDSN != "" {
		dsnSource = conf.SourceDSN
	} else {
		dsnSource = "liligo:liligo@tcp(localhost:3306)/liligo"
	}
	if dsnTarget != "" {
		dsnTarget = conf.TargetDSN
	} else {
		dsnTarget = "postgres://liligo:liligo@localhost:5432/liligo?sslmode=disable"
	}

	// Create connection to the databases.
	dbSource, err := sql.Open("mysql", dsnSource)
	if err != nil {
		log.Fatal(err)
	}
	defer dbSource.Close()

	dbTarget, err := sql.Open("postgres", dsnTarget)
	if err != nil {
		log.Fatal(err)
	}
	defer dbTarget.Close()

	// Create tables if needed.
	if err = createTablesIfNotExist(dbTarget); err != nil {
		log.Fatal("Error while creating the tables. ", err)
	}

	// Get the last times for each metric in the events database.
	lasts, err := getLastMetricsTime(dbTarget)
	if err != nil {
		log.Fatal(err)
	}

	// Fetch all the data from the source DB.
	query := "SELECT `index`, `timestamp`, metric FROM liligo"
	logDebug(query)

	res, err := dbSource.Query(query)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Close()

	// Process the records.
	processed := map[string]event{}
	for res.Next() {
		var index, temp, metric string
		var timestamp time.Time

		// Scan the next row.
		err = res.Scan(&index, &temp, &metric)
		if err != nil {
			log.Fatal(err)
		}

		// Parse the datetime string.
		timestamp, err = time.Parse(datetimeLayout, temp)
		if err != nil {
			log.Fatal(err)
		}

		// Check the time of this record. Modify history is NOT allowed.
		last, exists := lasts[metric]
		if exists && last.After(timestamp) || last.Equal(timestamp) {
			continue
		}

		// Contruct a key for the map.
		key := metric + timestamp.String()[:11]

		// Add to the map or update existing element.
		value, exists := processed[key]
		if !exists {
			value = event{
				last:   timestamp,
				metric: metric,
				count:  1,
			}
		} else {
			value.count++
			if value.last.Before(timestamp) {
				value.last = timestamp
			}
		}

		processed[key] = value
	}

	// Insert the processed data to the new DB.
	for _, value := range processed {
		// Get the ID for the metric. If not exitst create one and return that new ID.
		mID, err := getOrCreateMetric(dbTarget, value.metric)
		if err != nil {
			log.Fatal(err)
		}

		// Create the insert command,
		query := fmt.Sprintf("INSERT INTO events (day_last, count, metric_id) VALUES('%s', %d, %d)", value.last.Format(time.RFC3339), value.count, mID)
		logDebug(query)

		// then execute it.
		_, err = dbTarget.Exec(query)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("DONE")
}
