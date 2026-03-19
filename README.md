# JSqlServerBulkInsert: High-Performance Bulk Inserts to SQL Server #

JSqlServerBulkInsert is a high-performance Java library for Bulk Inserts to Microsoft 
SQL Server using the native `ISQLServerBulkData` API.

It provides an elegant, functional wrapper around the SQL Server Bulk Copy functionality:

> Bulk copy is a feature that allows efficient bulk import of data into a SQL Server table. It 
> is significantly faster than using standard `INSERT` statements, especially for large datasets.

## Setup ##

JSqlServerBulkInsert is designed for Java 17+ and the Microsoft JDBC Driver for SQL Server (version 12.6.5.jre11 or higher).

Add the following dependency to your pom.xml:

```xml
<dependency>
    <groupId>de.bytefish</groupId>
    <artifactId>jsqlserverbulkinsert</artifactId>
    <version>6.0.0</version>
</dependency>
```

## A Re-Designed API for 6.0.0 ##

Version 6.0.0 introduces a completely redesigned API. It strictly separates 
the **What** (Structure and Mapping) from the **How** (Execution and I/O).

### Key Features: ###

* Stateless Mapping: Define your schema once and reuse it across multiple writers.
* Primitive Support: Specialized functional interfaces for primitive types (int, long, boolean, float, double) simplify the mapping process.
* Automated Bracketing: Automatic [] escaping for all schema, table, and column names to prevent keyword conflicts (e.g., with columns named `LOCALTIME` or `DATE`).
* Modern Time API: Native support for java.time types with optimized binary transfer.

## Quick Start ##

### 1. Define your Data Model ###

The library works perfectly with modern Java record types or traditional POJOs.

```java
public record SensorData(
    UUID id,
    String name,
    int temperature,
    double signal,
    OffsetDateTime timestamp
) {}
```

### 2. Define your Mapping (Stateless & Thread-Safe) ###

The `SqlServerMapper<T>` is the heart of the library. It is stateless after configuration and should 
be instantiated only once (e.g., as a `static final` field).

```java
private static final SqlServerMapper<SensorData> MAPPER = 
    SqlServerMapper.forClass(SensorData.class)
        .map("Id", SqlServerTypes.UNIQUEIDENTIFIER.from(SensorData::id))
        
        // Use specialized primitive extractors for better readability
        .map("Temperature", SqlServerTypes.INT.primitive(SensorData::temperature))
        .map("SignalStrength", SqlServerTypes.FLOAT.primitive(SensorData::signal))
        
        .map("Name", SqlServerTypes.NVARCHAR.from(SensorData::name))
        
        // TIME TYPES: Optimized binary transfer via native precision metadata
        .map("Timestamp", SqlServerTypes.DATETIMEOFFSET.offsetDateTime(SensorData::timestamp));
```

### 3. Execute the Bulk Insert ###

The `SqlServerBulkWriter<T>` is a lightweight, transient executor that streams the data to the database.

```java
public void saveAll(Connection conn, List<SensorData> data) {

    SqlServerBulkWriter<SensorData> writer = new SqlServerBulkWriter<>(MAPPER)
        .withBatchSize(1000)
        .withTableLock(true);
    
    BulkInsertResult result = writer.saveAll(conn, "dbo", "Sensors", data);
    
    if (result.success()) {
        System.out.println("Inserted " + result.rowsAffected() + " rows.");
    }
}
```

## Mastering the Fluent API ##

### Monitoring Progress ###

You can monitor the progress of long-running bulk inserts:

```java
writer.withNotifyAfter(1000, rows -> System.out.println("Processed " + rows + " rows."));
```

### Error Handling ###

Catch and handle specific SQL Server errors through a dedicated handler:

```java
writer.withErrorHandler(ex -> log.error("Bulk Insert failed: " + ex.getMessage()));
```

## Supported SQL Server Types ##

* Numeric Types: `BIT` (Boolean), `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `REAL`, `FLOAT` (Double), `NUMERIC`, `DECIMAL`, `MONEY`, `SMALLMONEY`
* Character Types: `CHAR`, `VARCHAR`, `NCHAR`, `NVARCHAR` (including `.max()` support)
* Temporal Types: `DATE`, `TIME`, `DATETIME`, `DATETIME2`, `SMALLDATETIME`, `DATETIMEOFFSET`
* Binary Types: `VARBINARY` (including `.max()` support)
* Other Types: `UNIQUEIDENTIFIER` (UUID)
