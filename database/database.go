package database

import (
	"log"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/scyllacloud"
)

const (
	connectionBundlePath = "connect-bundle-testing.yaml"
)

func Connect() (*gocql.Session, error) {
	cluster, err := scyllacloud.NewCloudCluster(connectionBundlePath)
	if err != nil {
		log.Fatalf("Failed to create cloud cluster config: %s", err)
	}

	cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy("us-east-1")
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to cluster: %s", err)
	}
	// defer session.Close()
	log.Println("DB Connected!")
	return session, nil
}
