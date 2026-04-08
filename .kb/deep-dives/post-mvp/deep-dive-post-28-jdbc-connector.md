# Deep Dive #28: JDBC Connector -- Factory, Connection Pooling, SQL Dialect

## Executive Summary

No JDBC integration exists in ES|QL today. Building one requires implementing
`ConnectorFactory`, `Connector`, and `ResultCursor` -- the same three-interface
pattern used by the Arrow Flight connector. The Flight connector
(`esql-datasource-grpc`) serves as the direct template. The framework already
handles async execution, buffer management, and split-based parallelism for
connectors. The main gaps are: (1) no connection pooling infrastructure anywhere
in Elasticsearch, (2) no JDBC drivers in the dependency tree, (3) `QueryRequest`
carries no pushed filters so SQL pushdown cannot happen today, and (4) no SQL
dialect abstraction exists.

---

## 1. Connector SPI -- What JDBC Must Implement

### 1.1 ConnectorFactory

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ConnectorFactory.java`

```java
public interface ConnectorFactory extends ExternalSourceFactory {
    String type();                                                      // "jdbc"
    boolean canHandle(String location);                                 // jdbc:postgresql://, jdbc:mysql://, etc.
    SourceMetadata resolveMetadata(String location, Map<String, Object> config);  // DatabaseMetaData -> schema
    Connector open(Map<String, Object> config);                         // return pooled JdbcConnector
}
```

`ConnectorFactory` extends `ExternalSourceFactory` (line 17), which provides
default `filterPushdownSupport()`, `operatorFactory()`, and `splitProvider()`
returning `SplitProvider.SINGLE`.

**Registration path**: A `DataSourcePlugin` declares `supportedConnectorSchemes()`
and returns the factory from `connectors(Settings)`. The `DataSourceModule`
wraps it in a `LazyConnectorFactory` for deferred classloading.

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/DataSourceModule.java` (lines 168-174)

```java
Set<String> connectorSchemes = plugin.supportedConnectorSchemes();
if (connectorSchemes.isEmpty() == false) {
    LazyConnectorFactory lazyConnector = new LazyConnectorFactory(state, connectorSchemes, ...);
    for (String scheme : connectorSchemes) {
        sourceFactoryMap.putIfAbsent(scheme, lazyConnector);
    }
}
```

The lazy wrapper checks `canHandle()` against declared schemes *without* loading
the plugin JAR. Full plugin classes load only when `resolveMetadata()` or
`open()` is called.

### 1.2 Connector

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/Connector.java`

```java
public interface Connector extends Closeable {
    ResultCursor execute(QueryRequest request, Split split);
    default ResultCursor execute(QueryRequest request, ExternalSplit split) {
        return execute(request, Split.SINGLE);
    }
}
```

The lifecycle is:
1. `ConnectorFactory.open(config)` -- creates a `Connector` (called once per query)
2. `connector.execute(request, split)` -- called once per split (or once with `Split.SINGLE`)
3. `connector.close()` -- called in the finally block of `AsyncConnectorSourceOperatorFactory`

The framework creates one `Connector` per query. For JDBC, this is where
connection checkout from a pool would happen.

### 1.3 ResultCursor

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/ResultCursor.java`

```java
public interface ResultCursor extends CloseableIterator<Page> {
    default void cancel() {}
}
```

`CloseableIterator<Page>` extends `Iterator<Page>` and `Closeable`.
**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/CloseableIterator.java` (line 18)

The contract: `hasNext()` returns true while more pages exist; `next()` returns
an ESQL `Page` of columnar `Block[]` data; `close()` releases resources; `cancel()`
can abort early.

### 1.4 QueryRequest

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/QueryRequest.java`

```java
public record QueryRequest(
    String target,                    // table name or query string
    List<String> projectedColumns,    // columns to SELECT
    List<Attribute> attributes,       // typed ESQL attributes
    Map<String, Object> config,       // connection config
    int batchSize,                    // fetch size hint
    BlockFactory blockFactory         // for building ESQL Blocks
) { ... }
```

**Critical gap**: `QueryRequest` has no field for pushed filters. The
`OperatorFactoryRegistry.factory()` method (line 73-81) constructs the
`QueryRequest` but does not pass `context.pushedFilter()` into it. For a JDBC
connector doing SQL pushdown, filters need to reach the connector. Options:
- Add a `pushedFilter` field to `QueryRequest`
- Stuff filters into the `config` map (hacky but works today)
- Extend `QueryRequest` with `withPushedFilter(Object)` similar to `withBlockFactory()`

---

## 2. Flight Connector as Template

The Arrow Flight connector is the only production `ConnectorFactory` implementation.
It lives in a standalone plugin module.

### 2.1 Plugin Structure

**Module**: `x-pack/plugin/esql-datasource-grpc/`

| File | Purpose |
|------|---------|
| `GrpcDataSourcePlugin.java` | `Plugin` + `DataSourcePlugin` entry point |
| `FlightConnectorFactory.java` | Schema resolution, connection creation |
| `FlightConnector.java` | Execute queries via Flight DoGet |
| `FlightResultCursor.java` | Arrow VectorSchemaRoot -> ESQL Page |
| `FlightTypeMapping.java` | Arrow types -> ESQL DataType + Block conversion |
| `FlightSplit.java` | Serializable split for multi-endpoint parallelism |
| `FlightSplitProvider.java` | Discovers splits via getFlightInfo |

### 2.2 GrpcDataSourcePlugin (Registration)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/GrpcDataSourcePlugin.java`

```java
public class GrpcDataSourcePlugin extends Plugin implements DataSourcePlugin {
    @Override public Set<String> supportedConnectorSchemes() { return Set.of("flight", "grpc"); }
    @Override public Map<String, ConnectorFactory> connectors(Settings settings) {
        return Map.of("flight", new FlightConnectorFactory());
    }
    @Override public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(FlightSplit.ENTRY);  // register split serialization
    }
}
```

**JDBC parallel**: `JdbcDataSourcePlugin` would declare `supportedConnectorSchemes()` = `Set.of("jdbc")`,
return a `JdbcConnectorFactory`, and register `JdbcSplit.ENTRY` if using multi-split execution.

### 2.3 FlightConnectorFactory (Schema Resolution + open)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnectorFactory.java`

Key patterns:
- `canHandle()` (line 40): checks URI scheme prefix (`flight://` or `grpc://`)
- `resolveMetadata()` (lines 46-67): opens a transient `FlightClient`, calls
  `getSchema()`, converts Arrow `Schema` -> `List<Attribute>` via
  `FlightTypeMapping.toAttributes()`, returns `SimpleSourceMetadata`
- `open()` (lines 70-76): creates a `FlightConnector` with just the endpoint
- `splitProvider()` (lines 79-81): returns a `FlightSplitProvider`

**JDBC parallel**:
- `canHandle()`: check for `jdbc:postgresql://`, `jdbc:mysql://`, `jdbc:snowflake://`
- `resolveMetadata()`: open a JDBC connection, call `DatabaseMetaData.getColumns()`,
  convert `java.sql.Types` -> `DataType`, return `SimpleSourceMetadata`
- `open()`: checkout a connection from the pool, return `JdbcConnector`
- `splitProvider()`: return `SplitProvider.SINGLE` initially (no partitioning)

### 2.4 FlightConnector (Connection + Execution)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightConnector.java`

- Holds a `defaultClient` and a `ConcurrentHashMap<String, FlightClient>` for
  multi-endpoint scenarios (lines 44-45)
- `execute(request, Split.SINGLE)` (line 61-62): uses default client, calls
  `getInfo()` then `getStream(ticket)`
- `execute(request, ExternalSplit)` (lines 66-68): dispatches to split-specific
  client via `clientForSplit()`
- `close()` (lines 113-126): closes all clients and the allocator

**JDBC parallel**: `JdbcConnector` would hold a `java.sql.Connection` (or pool
reference), execute SQL via `Statement.executeQuery()`, and wrap the `ResultSet`
in a `JdbcResultCursor`.

### 2.5 FlightResultCursor (Page Production)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightResultCursor.java`

```java
class FlightResultCursor implements ResultCursor {
    @Override public boolean hasNext() { return hasNextBatch; }
    @Override public Page next() {
        VectorSchemaRoot root = stream.getRoot();
        int rowCount = root.getRowCount();
        Block[] blocks = new Block[attributes.size()];
        for (int col = 0; col < attributes.size(); col++) {
            blocks[col] = FlightTypeMapping.toBlock(root.getVector(col), rowCount, blockFactory);
        }
        hasNextBatch = advance();
        return new Page(rowCount, blocks);
    }
}
```

**JDBC parallel**: `JdbcResultCursor.next()` would read `batchSize` rows from
`ResultSet`, build `Block[]` column-by-column using `BlockFactory` builders
(IntBlock.Builder, LongBlock.Builder, etc.), and return a `Page`. The key
challenge is that JDBC is row-oriented while ESQL is columnar, requiring a
row-to-columnar transpose during batch reads.

### 2.6 FlightTypeMapping (Type Conversion)

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/java/org/elasticsearch/xpack/esql/datasource/grpc/FlightTypeMapping.java`

Handles 6 Arrow types: Int, FloatingPoint, Utf8, Bool, Timestamp -> DataType
mappings and Arrow FieldVector -> ESQL Block conversions.

---

## 3. Connection Pooling Infrastructure

### 3.1 Current State: No JDBC Pooling Exists

**There is zero JDBC connection pooling infrastructure in Elasticsearch.** Searching
for HikariCP, Apache DBCP, c3p0, or any `ConnectionPool` class found nothing
relevant to JDBC. The `version.properties` file contains no JDBC-related
dependency versions.

The closest analog is LDAP connection pooling in the security module:
- `PoolingSessionFactory` in `x-pack/plugin/security/` uses the UnboundID
  `LDAPConnectionPool` (not JDBC)
- HTTP connection pooling exists in `x-pack/plugin/inference/` via Apache
  HttpClient's `PoolingHttpClientConnectionManager` (not JDBC either)

### 3.2 What's Needed

A JDBC connector would need:

1. **Connection pool library**: HikariCP is the standard choice (small, fast, well-tested).
   Would be a new dependency in `version.properties`.

2. **Pool lifecycle management**: Pools must be created per data source definition
   (by CRUD API), cached across queries, and closed when the data source is
   removed. The `DataSourceModule` manages `Closeable` resources but currently
   only for `TableCatalog` instances.

3. **Pool per data source, not per query**: The current `ConnectorFactory.open()`
   is called per query and creates a new `Connector`. For JDBC, `open()` should
   return a `JdbcConnector` that checks out a connection from a shared pool.
   The pool itself lives in the `JdbcConnectorFactory` (or a dedicated pool
   manager). The `Connector.close()` call returns the connection to the pool
   rather than closing it.

4. **Configuration**: Pool size, idle timeout, connection timeout, validation
   query. These would come from the data source CRUD API config.

### 3.3 No Existing JDBC Drivers

ES has its own JDBC driver (`x-pack/plugin/sql/jdbc/`) but this is an
*outbound* JDBC driver for clients connecting *to* Elasticsearch. It cannot
be reused for connecting Elasticsearch to external databases.

External JDBC drivers needed:
- `org.postgresql:postgresql` (PostgreSQL)
- `com.mysql:mysql-connector-j` (MySQL)
- `net.snowflake:snowflake-jdbc` (Snowflake)
- `com.microsoft.sqlserver:mssql-jdbc` (SQL Server)

Each would need to be a separate optional dependency, likely in separate
plugin modules (e.g., `esql-datasource-jdbc-postgresql`).

---

## 4. Thread Pool Considerations

### 4.1 Current Execution Model

External source operators run on the `esql_worker` thread pool.

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/EsqlPlugin.java` (line 120, 434-447)

```java
public static final String ESQL_WORKER_THREAD_POOL_NAME = "esql_worker";
// Pool size: configurable, defaults to search thread pool size
int poolSize = configuredSize > 0 ? configuredSize : ThreadPool.searchOrGetThreadPoolSize(allocatedProcessors);
// Fixed pool with 1000-task queue
new FixedExecutorBuilder(settings, ESQL_WORKER_THREAD_POOL_NAME, poolSize, 1000, ...)
```

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/plugin/ComputeService.java` (line 966)

```java
transportService.getThreadPool().executor(ESQL_WORKER_THREAD_POOL_NAME),
```

This executor is passed to `OperatorFactoryRegistry`, which passes it to
`AsyncConnectorSourceOperatorFactory`.

### 4.2 How Blocking I/O is Handled

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/AsyncConnectorSourceOperatorFactory.java`

The connector execution is already offloaded to a background thread:

```java
@Override
public SourceOperator get(DriverContext driverContext) {
    QueryRequest request = baseRequest.withBlockFactory(driverContext.blockFactory());
    AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(maxBufferSize);
    driverContext.addAsyncAction();
    executor.execute(() -> {           // <-- runs on esql_worker thread
        try (ResultCursor cursor = connector.execute(request, Split.SINGLE)) {
            ExternalSourceDrainUtils.drainPages(cursor, buffer);
            buffer.finish(false);
        } catch (Exception e) {
            buffer.onFailure(e);
        } finally { ... driverContext.removeAsyncAction(); }
    });
    return new AsyncExternalSourceOperator(buffer);
}
```

This pattern already handles blocking I/O correctly:
- The blocking `ResultCursor` iteration runs on the `esql_worker` thread
- The driver reads from `AsyncExternalSourceBuffer` without blocking
- Back-pressure is managed by `maxBufferSize` (currently 10 pages)

**JDBC impact**: JDBC `ResultSet.next()` is blocking I/O. The existing async
wrapper handles this correctly. However, JDBC operations can be very slow
(network round-trips per batch, or even per row if fetch size is too small).
Concerns:
- **Thread starvation**: A slow JDBC query could hold an `esql_worker` thread
  for minutes. With the default pool size equal to CPU cores, a few slow JDBC
  queries could starve other ESQL queries.
- **Mitigation**: Consider a dedicated thread pool for JDBC connectors (e.g.,
  `esql_jdbc_worker` with virtual threads or a larger fixed pool). The
  entitlement for `manage_threads` would allow this.

### 4.3 Virtual Threads Consideration

Java 21's virtual threads would be ideal for JDBC blocking I/O. The connector
could use `Executors.newVirtualThreadPerTaskExecutor()` to avoid thread
starvation. However, Elasticsearch's thread pool infrastructure doesn't
currently use virtual threads, so this would be a novel pattern.

---

## 5. ResultCursor: JDBC ResultSet -> ESQL Page

### 5.1 The Mapping Challenge

JDBC is row-oriented; ESQL is columnar. A `JdbcResultCursor` must:

1. Read N rows from `ResultSet` (N = `batchSize`, typically 1000)
2. Transpose into column-major `Block[]`:
   ```java
   Block[] blocks = new Block[columnCount];
   IntBlock.Builder intBuilder = blockFactory.newIntBlockBuilder(batchSize);
   // ... for each column type
   for (int row = 0; row < rowCount; row++) {
       resultSet.next();
       intBuilder.appendInt(resultSet.getInt(colIdx));
       // ... other columns
   }
   blocks[0] = intBuilder.build();
   return new Page(rowCount, blocks);
   ```

3. Handle nulls: `resultSet.wasNull()` after each `getXxx()` call, then
   `builder.appendNull()`.

### 5.2 Fetch Size

JDBC `Statement.setFetchSize()` controls how many rows the driver fetches
per network round-trip. Critical for performance:
- Too small (e.g., 1): one network round-trip per row -- extremely slow
- Too large (e.g., 1M): loads too much data into memory
- Recommended: match ESQL's `batchSize` (typically 1000)

Note: PostgreSQL requires `autoCommit=false` for fetch size to work.

### 5.3 Memory Management

Each `Block` built by `BlockFactory` is tracked by the circuit breaker.
The `JdbcResultCursor` must use `blockFactory` from the `QueryRequest`:

```java
QueryRequest request = baseRequest.withBlockFactory(driverContext.blockFactory());
```

This is already done by `AsyncConnectorSourceOperatorFactory.get()` (line 66).

---

## 6. Type Mapping: java.sql.Types -> ESQL DataType

### 6.1 ESQL DataType Enum

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/core/type/DataType.java`

Core types available for external sources:

| ESQL DataType | Java representation | Block type |
|---|---|---|
| `BOOLEAN` | boolean | BooleanBlock |
| `INTEGER` | int | IntBlock |
| `LONG` | long | LongBlock |
| `DOUBLE` | double | DoubleBlock |
| `KEYWORD` | BytesRef (UTF-8) | BytesRefBlock |
| `TEXT` | BytesRef (UTF-8) | BytesRefBlock |
| `DATETIME` | long (epoch millis) | LongBlock |
| `DATE_NANOS` | long (epoch nanos) | LongBlock |
| `UNSIGNED_LONG` | long (unsigned) | LongBlock |
| `IP` | BytesRef (16 bytes) | BytesRefBlock |
| `VERSION` | BytesRef | BytesRefBlock |

Note: `SHORT` and `BYTE` are widened to `INTEGER` on load. `FLOAT` and
`HALF_FLOAT` are widened to `DOUBLE`.

### 6.2 Proposed JDBC -> ESQL Type Mapping

| java.sql.Types | ESQL DataType | Notes |
|---|---|---|
| `BIT`, `BOOLEAN` | BOOLEAN | |
| `TINYINT` | INTEGER | widen from byte |
| `SMALLINT` | INTEGER | widen from short |
| `INTEGER` | INTEGER | |
| `BIGINT` | LONG | |
| `REAL`, `FLOAT` | DOUBLE | widen from float |
| `DOUBLE` | DOUBLE | |
| `NUMERIC`, `DECIMAL` | DOUBLE | lossy for high-precision; could use KEYWORD for exact |
| `CHAR`, `VARCHAR`, `LONGVARCHAR` | KEYWORD | |
| `NCHAR`, `NVARCHAR`, `LONGNVARCHAR` | KEYWORD | |
| `DATE` | DATETIME | convert to epoch millis |
| `TIME` | KEYWORD | no native TIME type in ESQL |
| `TIMESTAMP` | DATETIME | convert to epoch millis |
| `TIMESTAMP_WITH_TIMEZONE` | DATETIME | convert to UTC epoch millis |
| `BINARY`, `VARBINARY`, `LONGVARBINARY` | KEYWORD | hex-encode or base64 |
| `CLOB`, `NCLOB`, `BLOB` | UNSUPPORTED | too large |
| `ARRAY`, `STRUCT`, `OTHER` | UNSUPPORTED | |

### 6.3 Reference: ES SQL JDBC EsType Mapping

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/sql/jdbc/src/main/java/org/elasticsearch/xpack/sql/jdbc/EsType.java`

This maps ES types *to* JDBC types (opposite direction), but provides a useful
reference for the type correspondence:

```java
BOOLEAN(Types.BOOLEAN), BYTE(Types.TINYINT), SHORT(Types.SMALLINT),
INTEGER(Types.INTEGER), LONG(Types.BIGINT), DOUBLE(Types.DOUBLE),
FLOAT(Types.REAL), KEYWORD(Types.VARCHAR), TEXT(Types.VARCHAR),
DATETIME(Types.TIMESTAMP), IP(Types.VARCHAR), ...
```

### 6.4 TypeConverter Reference

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/sql/jdbc/src/main/java/org/elasticsearch/xpack/sql/jdbc/TypeConverter.java`

Contains extensive Java type conversion logic (lines 56-685). While written for
the outbound direction (ES -> JDBC client), the conversion patterns (especially
date/time handling, null checking, safe numeric narrowing) are directly
reusable as reference for the inbound direction (JDBC source -> ESQL).

---

## 7. SQL Dialect Abstraction

### 7.1 No Existing Infrastructure

There is no `SqlDialect` interface, SQL generator, or dialect abstraction
anywhere in the Elasticsearch codebase. The ES SQL module generates SQL-like
syntax for its own query language but does not target external databases.

### 7.2 What's Needed

A `SqlDialect` interface would handle database-specific SQL generation:

```java
public interface SqlDialect {
    String quoteIdentifier(String name);          // "col" vs `col` vs [col]
    String limitClause(int limit);                // LIMIT N vs FETCH FIRST N ROWS ONLY
    String buildSelect(List<String> columns, String table, Object pushedFilter, int limit);
    String translateFunction(String esqlFunction, List<String> args);
    // Type-specific literal formatting
    String dateLiteral(long epochMillis);
    String booleanLiteral(boolean value);         // true/false vs 1/0
}
```

Initial dialects needed:
- **PostgreSqlDialect**: `"identifier"`, `LIMIT N`, standard SQL
- **MySqlDialect**: `` `identifier` ``, `LIMIT N`, MySQL-specific functions
- **SnowflakeDialect**: `"IDENTIFIER"`, `LIMIT N`, case-sensitive identifiers

### 7.3 SQL Pushdown Integration

The existing `FilterPushdownSupport` interface:

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java`

This interface (lines 34-128) provides the mechanism to push ESQL filter
expressions to a source. For JDBC:

1. `pushFilters(List<Expression>)` receives ESQL expressions
2. The JDBC implementation translates supported expressions to SQL WHERE clauses
3. Unsupported expressions remain as ESQL-side filters
4. The opaque `pushedFilter` (a SQL WHERE string or AST) flows through to the
   connector at execution time

**Gap**: The pushed filter reaches `SourceOperatorContext.pushedFilter()` but is
NOT propagated to `QueryRequest`. See Section 1.4.

---

## 8. ES SQL JDBC Driver -- Reusable Components

### 8.1 Module Structure

**Path**: `x-pack/plugin/sql/jdbc/`

Key files:
- `EsType.java` -- ES type -> JDBC type mapping (reference for reverse mapping)
- `TypeConverter.java` -- Java type conversion utilities (685 lines)
- `TypeUtils.java` -- Type utility functions
- `JdbcConfiguration.java` -- JDBC URL parsing, connection properties
- `JdbcResultSet.java` -- ResultSet implementation (reference for reading pattern)
- `JdbcColumnInfo.java` -- Column metadata representation

### 8.2 Reusability Assessment

| Component | Reusable? | Notes |
|---|---|---|
| `EsType` mapping | Reference only | Direction is reversed |
| `TypeConverter` | Reference only | Conversion patterns, especially date/time |
| `JdbcConfiguration` | No | ES-specific URL format |
| `JdbcResultSet` | No | ES-specific cursor protocol |
| `JdbcDateUtils` | Possibly | UTC/timezone conversion utilities |

The ES SQL JDBC module is designed as a JDBC *driver* (client-side). It connects
*to* Elasticsearch, not *from* Elasticsearch. Almost none of the code can be
reused directly, but the type mapping and conversion patterns are valuable
reference material.

---

## 9. Plugin Entitlements

### 9.1 Pattern from Existing Plugins

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/src/main/plugin-metadata/entitlement-policy.yaml`

```yaml
ALL-UNNAMED:
  - manage_threads
  - outbound_network
```

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-s3/src/main/plugin-metadata/entitlement-policy.yaml`

```yaml
ALL-UNNAMED:
  - manage_threads
  - outbound_network
  - files:
    - path: "/dev/null"
      mode: "read"
```

### 9.2 JDBC Plugin Entitlements

A JDBC plugin would need:

```yaml
ALL-UNNAMED:
  - manage_threads       # for connection pool threads
  - outbound_network     # for TCP connections to databases
```

This matches the gRPC plugin exactly. If JDBC drivers need to read local
config files (e.g., pgpass, truststore), additional `files` entries would
be needed.

### 9.3 Build Configuration Pattern

**File**: `/Users/oleglvovitch/github/root/elasticsearch/x-pack/plugin/esql-datasource-grpc/build.gradle`

```groovy
apply plugin: 'elasticsearch.internal-es-plugin'
esplugin {
  name = 'esql-datasource-grpc'
  description = 'Arrow Flight connector for ESQL'
  classname = 'org.elasticsearch.xpack.esql.datasource.grpc.GrpcDataSourcePlugin'
  extendedPlugins = ['x-pack-esql']
}
dependencies {
  compileOnly project(path: xpackModule('esql'))
  compileOnly project(path: xpackModule('esql-core'))
  compileOnly project(':server')
  compileOnly project(xpackModule('esql:compute'))
  // ... driver-specific dependencies
}
```

---

## 10. Architecture: JDBC Connector Implementation Plan

### 10.1 Module Layout

```
x-pack/plugin/esql-datasource-jdbc/
  build.gradle
  src/main/java/.../jdbc/
    JdbcDataSourcePlugin.java          # Plugin entry point
    JdbcConnectorFactory.java          # ConnectorFactory impl
    JdbcConnector.java                 # Connector impl (holds Connection)
    JdbcResultCursor.java              # ResultCursor impl (wraps ResultSet)
    JdbcTypeMapping.java               # java.sql.Types -> DataType + Block builders
    JdbcSplitProvider.java             # SplitProvider (SINGLE initially)
    JdbcFilterPushdownSupport.java     # Expression -> SQL WHERE translation
    dialect/
      SqlDialect.java                  # Dialect interface
      PostgreSqlDialect.java           # PostgreSQL dialect
      MySqlDialect.java                # MySQL dialect
      AnsiSqlDialect.java             # Fallback ANSI SQL dialect
    pool/
      JdbcConnectionPool.java          # Wrapper around HikariCP
      JdbcConnectionConfig.java        # Pool configuration
  src/main/plugin-metadata/
    entitlement-policy.yaml
  src/test/java/.../jdbc/
    JdbcConnectorFactoryTests.java
    JdbcResultCursorTests.java
    JdbcTypeMappingTests.java
```

### 10.2 Key Implementation Details

**JdbcConnectorFactory.resolveMetadata():**
```java
// Pseudo-code
try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
    DatabaseMetaData meta = conn.getMetaData();
    ResultSet columns = meta.getColumns(catalog, schema, table, "%");
    List<Attribute> attrs = new ArrayList<>();
    while (columns.next()) {
        int sqlType = columns.getInt("DATA_TYPE");
        String name = columns.getString("COLUMN_NAME");
        DataType dt = JdbcTypeMapping.toDataType(sqlType);
        attrs.add(new ReferenceAttribute(Source.EMPTY, name, dt));
    }
    return new SimpleSourceMetadata(attrs, "jdbc", location, null, null, null,
        Map.of("jdbc_url", jdbcUrl, "table", table));
}
```

**JdbcResultCursor.next():**
```java
// Read batchSize rows, transpose to columnar
Block.Builder[] builders = createBuilders(attributes, batchSize, blockFactory);
int rowCount = 0;
while (rowCount < batchSize && resultSet.next()) {
    for (int col = 0; col < attributes.size(); col++) {
        appendToBuilder(builders[col], resultSet, col + 1, attributes.get(col).dataType());
    }
    rowCount++;
}
if (rowCount == 0) return null; // signal end
Block[] blocks = new Block[builders.length];
for (int i = 0; i < builders.length; i++) blocks[i] = builders[i].build();
return new Page(rowCount, blocks);
```

### 10.3 Infrastructure Gaps Summary

| Gap | Severity | Notes |
|-----|----------|-------|
| No JDBC drivers in dependency tree | High | Need PostgreSQL, MySQL drivers as new deps |
| No connection pooling library | High | Need HikariCP or similar |
| QueryRequest lacks pushed filter field | Medium | Blocks SQL WHERE pushdown |
| No SqlDialect abstraction | Medium | Needed for multi-database support |
| Shared esql_worker pool for blocking I/O | Medium | Risk of thread starvation |
| No CRUD API for data source config | Medium | Needed for credential/connection storage |
| No virtual thread support | Low | Would improve scalability for blocking I/O |

### 10.4 Effort Estimate

Based on the Flight connector as a baseline (which took approximately 1 sprint):

| Component | Estimate | Dependencies |
|---|---|---|
| Core connector (Factory + Connector + ResultCursor) | 1.5 weeks | None |
| Type mapping (java.sql.Types -> DataType -> Block) | 1 week | Core connector |
| Connection pooling (HikariCP integration) | 1 week | Core connector |
| SQL dialect abstraction + PostgreSQL dialect | 1 week | Core connector |
| Filter pushdown (Expression -> SQL WHERE) | 1.5 weeks | Dialect, QueryRequest fix |
| Plugin packaging + entitlements + tests | 0.5 week | All above |
| **Total** | **~6.5 weeks** | |

The critical path is: core connector -> type mapping -> connection pooling,
then dialect + pushdown can parallelize.

---

## 11. Key Decisions Required

1. **Connection pool library**: HikariCP vs. roll-your-own. HikariCP is ~130KB,
   well-tested, but adds a new dependency. Rolling your own is risky.

2. **Driver bundling strategy**: Bundle drivers in the plugin JAR (fat JAR) vs.
   require users to install drivers separately. Licensing considerations:
   PostgreSQL driver is BSD, MySQL connector is GPL.

3. **Thread pool strategy**: Reuse `esql_worker` (simple, risk of starvation) vs.
   dedicated `esql_jdbc` pool (isolated, more configuration).

4. **QueryRequest extension**: Add `pushedFilter` field to `QueryRequest` record
   (SPI change, affects all connectors) vs. pass through `config` map (hacky).

5. **Dialect discovery**: Auto-detect from JDBC URL (e.g., `jdbc:postgresql` ->
   PostgreSqlDialect) vs. explicit configuration.

6. **Schema caching**: JDBC `DatabaseMetaData.getColumns()` can be slow. Cache
   schema per data source, invalidate on CRUD update. The existing
   `SimpleSourceMetadata` is immutable and suitable for caching.
