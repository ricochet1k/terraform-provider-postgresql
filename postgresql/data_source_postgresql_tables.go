package postgresql

import (
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	tableQuery = `
	SELECT schemaname, tablename
	FROM pg_catalog.pg_tables
	WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
	`
	tableSchemaKeyword         = "schemaname"
	tablePatternMatchingTarget = "tablename"
)

func dataSourcePostgreSQLDatabaseTables() *schema.Resource {
	return &schema.Resource{
		Read: PGResourceFunc(dataSourcePostgreSQLTablesRead),
		Schema: map[string]*schema.Schema{
			"database": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The PostgreSQL database which will be queried for table names",
			},
			"schemas": {
				Type:        schema.TypeList,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				MinItems:    0,
				Description: "The PostgreSQL schema(s) which will be queried for table names. Queries all schemas in the database by default",
			},
			"table_types": {
				Type:        schema.TypeList,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				MinItems:    0,
				Description: "The PostgreSQL table types which will be queried for table names. Includes all table types by default. Use 'BASE TABLE' for normal tables only",
			},
			"like_any_patterns": {
				Type:        schema.TypeList,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				MinItems:    0,
				Description: "Expression(s) which will be pattern matched against table names in the query using the PostgreSQL LIKE ANY operator",
			},
			"like_all_patterns": {
				Type:        schema.TypeList,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				MinItems:    0,
				Description: "Expression(s) which will be pattern matched against table names in the query using the PostgreSQL LIKE ALL operator",
			},
			"not_like_all_patterns": {
				Type:        schema.TypeList,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				MinItems:    0,
				Description: "Expression(s) which will be pattern matched against table names in the query using the PostgreSQL NOT LIKE ALL operator",
			},
			"regex_pattern": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Expression which will be pattern matched against table names in the query using the PostgreSQL ~ (regular expression match) operator",
			},
			"tables": {
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"schema_name": {
							Type:     schema.TypeString,
							Computed: true,
						},
						"table_name": {
							Type:     schema.TypeString,
							Computed: true,
						},
					},
				},
				Description: "The list of PostgreSQL tables retrieved from pg_catalog.pg_tables.",
			},
		},
	}
}

func dataSourcePostgreSQLTablesRead(db *DBConnection, d *schema.ResourceData) error {
	database := d.Get("database").(string)

	txn, err := startTransaction(db.client, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	query := tableQuery
	queryConcatKeyword := queryConcatKeywordAnd

	query = applyEqualsAnyFilteringToQuery(query, &queryConcatKeyword, tableSchemaKeyword, d.Get("schemas").([]interface{}))
	query = applyOptionalPatternMatchingToQuery(query, tablePatternMatchingTarget, &queryConcatKeyword, d)
	query += " ORDER BY schemaname, tablename"

	rows, err := txn.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	tables := make([]interface{}, 0)
	for rows.Next() {
		var table_name string
		var schema_name string

		if err = rows.Scan(&schema_name, &table_name); err != nil {
			return fmt.Errorf("could not scan table output for database: %w", err)
		}

		result := make(map[string]interface{})
		result["schema_name"] = schema_name
		result["table_name"] = table_name
		tables = append(tables, result)
	}

	d.Set("tables", tables)
	d.SetId(generateDataSourceTablesID(d, database))

	return nil
}

func generateDataSourceTablesID(d *schema.ResourceData, databaseName string) string {
	return strings.Join([]string{
		databaseName,
		generatePatternArrayString(d.Get("schemas").([]interface{}), queryArrayKeywordAny),
		generatePatternArrayString(d.Get("table_types").([]interface{}), queryArrayKeywordAny),
		generatePatternArrayString(d.Get("like_any_patterns").([]interface{}), queryArrayKeywordAny),
		generatePatternArrayString(d.Get("like_all_patterns").([]interface{}), queryArrayKeywordAll),
		generatePatternArrayString(d.Get("not_like_all_patterns").([]interface{}), queryArrayKeywordAll),
		d.Get("regex_pattern").(string),
	}, "_")
}
