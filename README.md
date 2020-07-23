# Sql Bulk Copy Tool

Simple tool to copy all data from one SQL Server database to another, using SQL Bulk Copy

## Usage

Set the source and target connection strings below.

The copy only works if the database schemas match.
Tables and table columns must be structurally equal.

``` csharp
Copy(
    sourceConnectionString: @"Server=.\sqlexpress;Database=sourcedatabase;Trusted_Connection=True;",
    targetConnectionString: @"Server=.\sqlexpress;Database=targetdatabase;Trusted_Connection=True;");
```

## Warning

All data in the target database will be deleted.

Works best if the target database is brand new.

Everything runs under a transaction to minimize risks.
