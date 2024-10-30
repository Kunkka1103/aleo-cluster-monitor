package sqlhandler

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
	"log"
)

// InitDB initializes a database connection.
func InitDB(category, name, DSN string) (db *sql.DB, err error) {
	// Open a database connection
	db, err = sql.Open(category, DSN)
	if err != nil {
		return nil, err
	}

	log.Printf("%s DSN check success", name)

	// Ping the database to ensure the connection is established
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	log.Printf("%s database connected successfully", name)
	return db, nil
}

// GetCluster retrieves the list of clusters from the database.
func GetCluster(db *sql.DB) (clusterList []string, err error) {
	SQL := "SELECT cluster_name FROM aleo_cluster"
	rows, err := db.Query(SQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cluster string
	for rows.Next() {
		err = rows.Scan(&cluster)
		if err != nil {
			return nil, err
		}
		clusterList = append(clusterList, cluster)
	}
	return clusterList, err
}

// GetMachine retrieves the count of different machine states for a given cluster.
func GetMachine(db *sql.DB, cluster string) (total, active, inactive, failed, invalid int, err error) {
	query := fmt.Sprintf(`
		SELECT COUNT(m.id) AS total_machines,
		       COALESCE(SUM(CASE WHEN m.last_commit_solution >= EXTRACT(EPOCH FROM NOW()) - 600 THEN 1 ELSE 0 END), 0) AS active_machines,
		       COALESCE(SUM(CASE WHEN m.last_commit_solution < EXTRACT(EPOCH FROM NOW()) - 600 AND m.last_commit_solution >= EXTRACT(EPOCH FROM NOW()) - 86400 THEN 1 ELSE 0 END), 0) AS inactive_machines,
		       COALESCE(SUM(CASE WHEN m.last_commit_solution < EXTRACT(EPOCH FROM NOW()) - 86400 THEN 1 ELSE 0 END), 0) AS failed_machines,
		       COALESCE(SUM(CASE WHEN m.last_commit_solution IS NULL THEN 1 ELSE 0 END), 0) AS invalid_machines
		FROM miner_account ma
		         LEFT JOIN machine m ON m.miner_account_id = ma.id
		         JOIN "user" u ON ma.main_user_id = u.id
		WHERE ma.name = '%s'
		GROUP BY u.email
	`, cluster)
	err = db.QueryRow(query).Scan(&total, &active, &inactive, &failed, &invalid)

	if err == sql.ErrNoRows {
		total, active, inactive, failed, invalid = 0, 0, 0, 0, 0
		err = nil
	}

	return total, active, inactive, failed, invalid, nil
}

// GetLast24hPower retrieves the computing power for the last 24 hours for a given cluster.
func GetLast24hPower(db *sql.DB, cluster string) (power float64, err error) {
	query := fmt.Sprintf(`
		WITH filtered_epochs AS (
		    SELECT *
		    FROM epoch_distributor
		    WHERE epoch_time >= NOW() - INTERVAL '24 HOURS'
		      AND miner_account_id IN (SELECT id FROM miner_account WHERE name='%s')
		    ORDER BY epoch_time ASC
		),
		filtered_epochs_excluding_oldest AS (
		    SELECT *
		    FROM filtered_epochs
		    OFFSET 1  
		)
		SELECT COALESCE(SUM(hash_count) / EXTRACT(EPOCH FROM (MAX(epoch_time) - MIN(epoch_time))) / 1000000, 0)
		FROM filtered_epochs_excluding_oldest
	`, cluster)
	err = db.QueryRow(query).Scan(&power)

	if err == sql.ErrNoRows {
		power = 0
		err = nil
	}

	return power, err
}

// GetLastEpochPower retrieves the computing power for the last epoch for a given cluster.
func GetLastEpochPower(db *sql.DB, cluster string) (power float64, err error) {
	query := fmt.Sprintf(`
		WITH recent_epochs AS (
		    SELECT miner_account_id, epoch_time, hash_count
		    FROM epoch_distributor
		    WHERE miner_account_id IN (SELECT id FROM miner_account WHERE name='%s')
		    ORDER BY epoch_time DESC
		    LIMIT 2
		)
		SELECT COALESCE((SELECT hash_count FROM recent_epochs ORDER BY epoch_time DESC LIMIT 1) /
		       EXTRACT(EPOCH FROM (MAX(epoch_time) - MIN(epoch_time))) / 1000000, 0) AS avg_computing_power
		FROM recent_epochs;
	`, cluster)
	err = db.QueryRow(query).Scan(&power)

	if err == sql.ErrNoRows {
		power = 0
		err = nil
	}

	return power, err
}

// GetYesterdayReward retrieves the reward for yesterday for a given cluster.
func GetYesterdayReward(db *sql.DB, cluster string) (reward float64, err error) {
	query := fmt.Sprintf(`
		SELECT COALESCE(reward, 0)
		FROM distributor
		WHERE miner_account_id IN (SELECT id FROM miner_account WHERE name = '%s')
		  AND distributor_date = CURRENT_DATE - 1
	`, cluster)
	err = db.QueryRow(query).Scan(&reward)

	if err == sql.ErrNoRows {
		reward = 0
		err = nil
	}

	return reward, err
}

// GetTodayReward retrieves the reward for today for a given cluster.
func GetTodayReward(db *sql.DB, cluster string) (reward float64, err error) {
	query := fmt.Sprintf(`
		SELECT COALESCE(SUM(reward), 0) 
		FROM epoch_distributor 
		WHERE miner_account_id IN (SELECT id FROM miner_account WHERE name = '%s')
		  AND DATE(epoch_time AT TIME ZONE 'Asia/Shanghai') = CURRENT_DATE
	`, cluster)
	err = db.QueryRow(query).Scan(&reward)

	if err == sql.ErrNoRows {
		reward = 0
		err = nil
	}

	return reward, err
}

// GetNetworkReward retrieves the average reward over the past 24 hours.
func GetNetworkReward(db *sql.DB) (decimal.Decimal, error) {
	var avgReward decimal.Decimal
	SQL := `
		SELECT COALESCE(AVG(reward) / 1000000::numeric, 0) AS reward
		FROM solution s
		JOIN block b ON b.height = s.height
		WHERE b.timestamp > FLOOR(EXTRACT(EPOCH FROM NOW())) - 86400;
	`

	err := db.QueryRow(SQL).Scan(&avgReward)
	if err == sql.ErrNoRows {
		avgReward = decimal.Zero
		err = nil
	}

	if err != nil {
		return decimal.Zero, err
	}

	return avgReward, nil
}

// GetAuxiliaryParameter retrieves the average proof_target over the past 24 hours.
func GetAuxiliaryParameter(db *sql.DB) (decimal.Decimal, error) {
	var avgTarget decimal.Decimal
	SQL := `
		SELECT COALESCE(AVG(proof_target)::numeric, 0) AS target
		FROM block b
		WHERE b.timestamp > FLOOR(EXTRACT(EPOCH FROM NOW())) - 86400;
	`

	err := db.QueryRow(SQL).Scan(&avgTarget)
	if err == sql.ErrNoRows {
		avgTarget = decimal.Zero
		err = nil
	}

	if err != nil {
		return decimal.Zero, err
	}

	// If avgTarget > 0, calculate 86400 / avgTarget
	if avgTarget.GreaterThan(decimal.Zero) {
		return decimal.NewFromFloat(86400).Div(avgTarget), nil
	}

	return decimal.Zero, nil
}

// InsertData inserts or updates a cluster's statistics in the MySQL database.
func InsertData(db *sql.DB, cluster string, total, active, inactive, failed, invalid int,
	last24hPower, lastEpochPower, yesterdayReward, todayReward float64,
	expectedReward decimal.Decimal) error {
	// SQL query to insert data or update if the cluster_name already exists
	query := `
		INSERT INTO aleo_cluster_stats 
		(cluster_name, total, active, inactive, failed, invalid, 
		 last_24h_power, last_epoch_power, yesterday_reward, today_reward, expected_reward)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		  total = VALUES(total),
		  active = VALUES(active),
		  inactive = VALUES(inactive),
		  failed = VALUES(failed),
		  invalid = VALUES(invalid),
		  last_24h_power = VALUES(last_24h_power),
		  last_epoch_power = VALUES(last_epoch_power),
		  yesterday_reward = VALUES(yesterday_reward),
		  today_reward = VALUES(today_reward),
		  expected_reward = VALUES(expected_reward)
	`

	// Execute the insert or update operation
	_, err := db.Exec(query, cluster, total, active, inactive, failed, invalid,
		last24hPower, lastEpochPower, yesterdayReward, todayReward,
		expectedReward.String())
	if err != nil {
		return fmt.Errorf("failed to insert or update data: %v", err)
	}

	log.Println("Data inserted or updated successfully")
	return nil
}
