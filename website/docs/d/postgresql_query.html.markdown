---
layout: "postgresql"
page_title: "PostgreSQL: postgresql_query"
sidebar_current: "docs-postgresql-data-source-postgresql_query"
description: |-
  Runs a query on a PostgreSQL database, returning columns and rows
---

# postgresql\_query

The ``postgresql_query`` data source runs a query on a specified PostgreSQL database.


## Usage

```hcl
data "postgresql_query" "my_query" {
  database = "my_database"
  query = <<-EOF
    SELECT * FROM your.tables
    WHERE somecol = $1
  EOF
  args = ["someval"]
}

```

## Argument Reference

* `database` - (Required) The PostgreSQL database which will be queried for table names.
* `query` - (Required) SQL Query string. `$1`, `$2`, etc. can be used as placeholders for args
* `args` - (Optional) List of arguments to fill in placeholders.

## Attributes Reference

* `columns` - A list of columns in the query result. Each column consists of the fields documented below.

* `rows` - A list of rows in the query result. Each row is a map of column_name to value. Value is converted to a string due to restrictions in Terraform's schema type system.
___

The `columns` block consists of: 

* `name` - The column name.

* `type` - The column type in the Database, `INT4`, `TEXT`, etc.

