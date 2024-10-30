package main

import (
	"aleo-cluster-monitor/sqlhandler"
	"flag"
	"github.com/shopspring/decimal"
	"log"
	"time"
)

var oulaDsn = flag.String("oula", "", "oula dsn")
var opsDsn = flag.String("ops", "", "ops dsn")
var interval = flag.Int("i", 2, "check interval(min)")

func main() {
	flag.Parse()

	if len(*opsDsn) == 0 || len(*oulaDsn) == 0 {
		log.Fatalln("oula and ops dns are required")
	}

	// Initialize databases
	oulaDB, err := sqlhandler.InitDB("postgres", "oula", *oulaDsn)
	if err != nil {
		log.Fatalf("oulaDB init failed, err: %s", err)
	}
	defer oulaDB.Close()

	opsDB, err := sqlhandler.InitDB("mysql", "ops", *opsDsn)
	if err != nil {
		log.Fatalf("opsDB init failed, err: %s", err)
	}
	defer opsDB.Close()

	for {
		// Get Cluster List
		clusterList, err := sqlhandler.GetCluster(opsDB)
		if err != nil {
			log.Printf("get aleo cluster info failed, err: %s, will retry in %d min", err, *interval)
			time.Sleep(time.Duration(*interval) * time.Minute)
			continue
		}
		log.Println("get aleo cluster info success, cluster: ", clusterList)

		for _, cluster := range clusterList {
			// Get machine information
			total, active, inactive, failed, invalid, err := sqlhandler.GetMachine(oulaDB, cluster)
			if err != nil {
				log.Printf("get machine info failed, err: %s, will retry in %d min", err, *interval)
				time.Sleep(time.Duration(*interval) * time.Minute)
				continue
			}
			log.Printf("get machine info success, cluster: %s, total: %d, active: %d, inactive: %d, failed: %d, invalid: %d", cluster, total, active, inactive, failed, invalid)

			// Get last 24-hour average power
			last24hPower, err := sqlhandler.GetLast24hPower(oulaDB, cluster)
			if err != nil {
				log.Printf("get last 24h power failed, err: %s, will retry in %d min", err, *interval)
				time.Sleep(time.Duration(*interval) * time.Minute)
				continue
			}
			log.Printf("get last 24h power success, cluster: %s, last24hPower: %f", cluster, last24hPower)

			// Get last epoch average power
			lastEpochPower, err := sqlhandler.GetLastEpochPower(oulaDB, cluster)
			if err != nil {
				log.Printf("get last epoch power failed, err: %s, will retry in %d min", err, *interval)
				time.Sleep(time.Duration(*interval) * time.Minute)
				continue
			}
			log.Printf("get last epoch power success, cluster: %s, lastEpochPower: %f", cluster, lastEpochPower)

			// Get yesterday's reward
			yesterdayReward, err := sqlhandler.GetYesterdayReward(oulaDB, cluster)
			if err != nil {
				log.Printf("get yesterday reward failed, err: %s, will retry in %d min", err, *interval)
				time.Sleep(time.Duration(*interval) * time.Minute)
				continue
			}
			log.Printf("get yesterday reward success, cluster: %s, yesterdayReward: %f", cluster, yesterdayReward)

			// Get today's reward
			todayReward, err := sqlhandler.GetTodayReward(oulaDB, cluster)
			if err != nil {
				log.Printf("get today reward failed, err: %s, will retry in %d min", err, *interval)
				time.Sleep(time.Duration(*interval) * time.Minute)
				continue
			}
			log.Printf("get today reward success, cluster: %s, todayReward: %f", cluster, todayReward)

			// Get expected reward
			networkReward, err := sqlhandler.GetNetworkReward(oulaDB)
			if err != nil {
				log.Printf("get network reward failed, err: %s, will retry in %d min", err, *interval)
				time.Sleep(time.Duration(*interval) * time.Minute)
				continue
			}

			auxiliaryParameter, err := sqlhandler.GetAuxiliaryParameter(oulaDB)
			if err != nil {
				log.Printf("get auxiliary parameter failed, err: %s, will retry in %d min", err, *interval)
				time.Sleep(time.Duration(*interval) * time.Minute)
				continue
			}

			rewardPerM := networkReward.Mul(auxiliaryParameter).Mul(decimal.NewFromFloat(1e6))
			log.Println("reward per M: ", rewardPerM.String())
			expectedReward := rewardPerM.Mul(decimal.NewFromFloat(last24hPower))
			log.Printf("%s expected reward: %s", cluster, expectedReward.String())

			// Insert data into the database
			err = sqlhandler.InsertData(opsDB, cluster, total, active, inactive, failed, invalid, last24hPower, lastEpochPower, yesterdayReward, todayReward, expectedReward)
			if err != nil {
				log.Printf("insert data failed, err: %s, will retry in %d min", err, *interval)
				time.Sleep(time.Duration(*interval) * time.Minute)
				continue
			}
			log.Printf("insert data success, cluster: %s", cluster)
		}

		time.Sleep(time.Duration(*interval) * time.Minute)
	}
}
