package postgresql

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccPostgresqlDataSourceQuery(t *testing.T) {
	skipIfNotAcc(t)

	// Create the database outside of resource.Test
	// because we need to create test schemas.
	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	schemas := []string{"test_schema1", "test_schema2"}
	createTestSchemas(t, dbSuffix, schemas, "")

	testTables := []string{"test_schema.test_table", "test_schema1.test_table1"}
	createTestTables(t, dbSuffix, testTables, "")

	insertTestRows(t, dbSuffix, "test_schema1.test_table1", []string{"val"}, [][]interface{}{{"foo"}, {"bar"}})

	dbName, _ := getTestDBNames(dbSuffix)

	testAccPostgresqlDataSourceQueryConfig := generateDataSourceQueryConfig(dbName)

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: testAccPostgresqlDataSourceQueryConfig,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("data.postgresql_query.test_empty", "columns.#", "1"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_empty", "columns.0.name", "val"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_empty", "columns.0.type", "TEXT"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_empty", "rows.#", "0"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select", "columns.#", "2"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select", "columns.0.name", "a"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select", "columns.0.type", "INT4"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select", "columns.1.name", "b"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select", "columns.1.type", "TEXT"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select", "rows.#", "1"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select", "rows.0.a", "1"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select", "rows.0.b", "2"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select_table", "columns.#", "1"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select_table", "columns.0.name", "val"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select_table", "columns.0.type", "TEXT"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select_table", "rows.#", "2"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select_table", "rows.0.val", "foo"),
					resource.TestCheckResourceAttr("data.postgresql_query.test_select_table", "rows.1.val", "bar"),
				),
			},
		},
	})
}

func generateDataSourceQueryConfig(dbName string) string {
	return fmt.Sprintf(`
	data "postgresql_query" "test_empty" {
		database = "%[1]s"
		query = "SELECT * FROM test_schema.test_table"
	}
	data "postgresql_query" "test_select" {
		database = "%[1]s"
  		query = "SELECT 1 as a, '2' as b;"
	}
	data "postgresql_query" "test_select_table" {
		database = "%[1]s"
		query = "SELECT * FROM test_schema1.test_table1"
	}
	`, dbName)
}
