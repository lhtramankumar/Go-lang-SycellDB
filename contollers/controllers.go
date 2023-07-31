package contollers

import (
	"context"
	"fmt"
	"log"
	"scylladb/database"
	"scylladb/graph/model"

	"github.com/gocql/gocql"
)

const (
	keyspaceName = "books"
	tableName    = "book"
)

func CreateBookListing(ctx context.Context, input model.CreateBookListingInput) (*model.BookListing, error) {

	session, err := database.Connect()
	if err != nil {
		return nil, fmt.Errorf(`failed to connect db"%s"`, err)
	}

	log.Println("db connected")
	createKeyspaceIfNotExists(session, keyspaceName)

	if !tableExists(session, keyspaceName, tableName) {
		createBookTable(session, keyspaceName, tableName)
		fmt.Println("Created new tables")
	}
	// Generate a new UUID for the book listing ID.
	bookID := gocql.TimeUUID()

	// Prepare the INSERT CQL statement.
	insertQuery := fmt.Sprintf("INSERT INTO %s.%s (%s, %s, %s, %s, %s) VALUES (?, ?, ?, ?, ?)",
		keyspaceName, tableName, "id", "title", "bookname", "description", "author")

	// Execute the INSERT query.
	if err := session.Query(insertQuery, bookID, input.Title, input.Bookname, input.Description, input.Author).Exec(); err != nil {
		return nil, fmt.Errorf("failed to insert book listing: %s", err)
	}
	// Return the created book listing.
	bookListing := &model.BookListing{
		ID:          bookID.String(),
		Title:       input.Title,
		Bookname:    input.Bookname,
		Description: input.Description,
		Author:      input.Author,
	}

	log.Println("new Book information inserted successfully")
	return bookListing, nil
}

func createKeyspaceIfNotExists(session *gocql.Session, keyspaceName string) error {
	keyspaceExists, err := keyspaceExists(session, keyspaceName)
	if err != nil {
		return err
	}

	if !keyspaceExists {
		keyspaceQuery := fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};", keyspaceName)
		if err := session.Query(keyspaceQuery).Exec(); err != nil {
			return err
		}
		fmt.Printf("Keyspace '%s' created successfully.\n", keyspaceName)
	} else {
		fmt.Printf("Keyspace '%s' already exists.\n", keyspaceName)
	}

	return nil
}

func keyspaceExists(session *gocql.Session, keyspaceName string) (bool, error) {
	query := session.Query("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", keyspaceName)

	var resultKeyspaceName string
	if err := query.Scan(&resultKeyspaceName); err != nil {
		if err == gocql.ErrNotFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func tableExists(session *gocql.Session, keyspaceName string, tableName string) bool {
	// Check if the table exists by querying the system tables.
	query := fmt.Sprintf("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s';", keyspaceName, tableName)
	iter := session.Query(query).Iter()
	defer iter.Close()

	if iter.NumRows() > 0 {
		fmt.Println("table is already exists")
	}
	// Check if any rows were returned (table exists).
	return iter.NumRows() > 0
}

func createBookTable(session *gocql.Session, keyspaceName string, tableName string) {
	// Define the CQL query to create the table.
	createTableQuery := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id UUID PRIMARY KEY,
			title TEXT,
			bookname TEXT,
			description TEXT,
			author TEXT
		);
	`, keyspaceName, tableName)

	// Execute the CREATE TABLE query.
	if err := session.Query(createTableQuery).Exec(); err != nil {
		log.Fatal(err)
	}
}
func ReadBookListings() ([]*model.BookListing, error) {
	session, err := database.Connect()
	if err != nil {
		return nil, fmt.Errorf(`failed to connect db"%s"`, err)
	}

	defer session.Close()

	// Prepare the SELECT CQL statement.
	selectQuery := fmt.Sprintf("SELECT id, title, bookname, description, author FROM %s.%s", keyspaceName, tableName)

	// Execute the SELECT query.
	iter := session.Query(selectQuery).Iter()

	// Iterate over the results and build book listings.
	bookListings := []*model.BookListing{}
	var id gocql.UUID
	var title, bookname, description, author string

	for iter.Scan(&id, &title, &bookname, &description, &author) {
		bookListing := &model.BookListing{
			ID:          id.String(),
			Title:       title,
			Bookname:    bookname,
			Description: description,
			Author:      author,
		}
		bookListings = append(bookListings, bookListing)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to read book listings: %s", err)
	}

	return bookListings, nil
}
