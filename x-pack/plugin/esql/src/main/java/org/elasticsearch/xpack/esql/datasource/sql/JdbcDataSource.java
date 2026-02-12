/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.sql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasource.DataSourceDescriptor;
import org.elasticsearch.xpack.esql.datasource.DataSourcePartition;
import org.elasticsearch.xpack.esql.datasource.DataSourcePlan;
import org.elasticsearch.xpack.esql.expression.Order;

import java.util.List;
import java.util.Locale;

/**
 * Example JDBC data source for reading from SQL databases.
 *
 * <p>This demonstrates a SQL-style data source that:
 * <ul>
 *   <li>Resolves table names or native SQL queries via JDBC</li>
 *   <li>Translates ES|QL operations (filter, order, limit, aggregation) to SQL</li>
 *   <li>Executes the combined SQL query on the coordinator via JDBC</li>
 * </ul>
 *
 * <p>While this example uses MySQL as the target database, the same pattern applies
 * to any JDBC-accessible database (PostgreSQL, Oracle, SQL Server, etc.) — only the
 * SQL dialect translation differs.
 *
 * <h2>Syntax examples</h2>
 * <pre>
 * FROM my_db:users                                       -- table name
 * FROM my_db:orders                                      -- table name
 * FROM my_db:query("SELECT * FROM users WHERE active")   -- native SQL
 * FROM EXTERNAL({"type": "jdbc", "configuration": {"url": "jdbc:mysql://host:3306/db",
 *   "username": "...", "password": "..."}, "settings": {}}):users
 * </pre>
 *
 * <h2>Third-party dependencies (stubbed)</h2>
 * <p>A real implementation would depend on:
 * <ul>
 *   <li>A JDBC driver for the target database (e.g., {@code mysql:mysql-connector-java})</li>
 * </ul>
 */
public class JdbcDataSource extends SqlDataSource {

    private static final Logger logger = LogManager.getLogger(JdbcDataSource.class);

    @Override
    public String type() {
        return "jdbc";
    }

    // =========================================================================
    // PHASE 1: RESOLUTION
    // =========================================================================

    /**
     * Resolve a JDBC source reference.
     *
     * <p>Connects via JDBC, determines if the expression is a table name or native query,
     * reads the result schema, and returns a {@link JdbcPlan}.
     */
    @Override
    public DataSourcePlan resolve(DataSourceDescriptor source, ResolutionContext context) {
        String url = source.requireConfig("url");
        String username = source.requireConfig("username");
        String password = source.requireConfig("password");

        // The expression is the table name or SQL query (parser already stripped quotes/query wrapper)
        String expression = source.expression();

        // --- JDBC connection (stubbed) ---
        // In a real implementation:
        // try (Connection conn = DriverManager.getConnection(url, username, password)) {
        // ...
        // }

        // Detect if expression is a native SQL query or table reference
        String tableName;
        if (looksLikeQuery(expression)) {
            // Native query: wrap as subquery so we can add WHERE/ORDER BY/LIMIT on top
            tableName = "(" + expression + ") AS _subq";
        } else {
            // Simple table name
            tableName = expression;
        }

        // --- Schema reading (stubbed) ---
        // In a real implementation:
        // String schemaSql = "SELECT * FROM " + tableName + " LIMIT 0";
        // ResultSetMetaData meta = conn.prepareStatement(schemaSql).getMetaData();
        // List<Attribute> attrs = new ArrayList<>();
        // for (int i = 1; i <= meta.getColumnCount(); i++) {
        // attrs.add(toAttribute(meta.getColumnName(i), meta.getColumnType(i)));
        // }
        List<Attribute> attrs = readTableSchema(url, username, password, tableName);
        logger.debug("Resolved JDBC source [{}] with [{}] columns", tableName, attrs.size());

        return new JdbcPlan(source.source(), this, source.describe(), attrs, tableName, null, null, null, null, null);
    }

    // =========================================================================
    // SQL TRANSLATION
    // =========================================================================

    @Override
    protected String getTableName(SqlPlan plan) {
        return ((JdbcPlan) plan).tableName();
    }

    /**
     * Translate an ES|QL filter expression to a SQL WHERE clause.
     *
     * <p>Translatable expressions:
     * <ul>
     *   <li>Comparisons: {@code =, !=, <, >, <=, >=}</li>
     *   <li>Logical: {@code AND, OR, NOT}</li>
     *   <li>IS NULL / IS NOT NULL</li>
     *   <li>IN (...)</li>
     *   <li>LIKE</li>
     * </ul>
     *
     * <p>Non-translatable (ES|QL evaluates these):
     * <ul>
     *   <li>ES|QL-specific functions (e.g., CIDR_MATCH, GROK)</li>
     *   <li>Complex nested expressions the translator doesn't handle</li>
     * </ul>
     */
    @Override
    protected SqlFragment translateFilter(Expression filter) {
        // --- Filter translation (stubbed) ---
        // In a real implementation:
        // SqlExpressionTranslator translator = new SqlExpressionTranslator(dialect);
        // return translator.toWhere(filter); // e.g., SqlFragment("age > ? AND status = ?", [21, "active"])

        // Stub: translate to a placeholder
        String sql = stubTranslateExpression(filter);
        if (sql != null) {
            return new SqlFragment(sql);
        }
        return null;
    }

    /**
     * Translate ES|QL aggregates to SQL SELECT clause.
     *
     * <p>Maps ES|QL aggregate functions to SQL:
     * <ul>
     *   <li>{@code AVG(x)} -&gt; {@code AVG(x)}</li>
     *   <li>{@code SUM(x)} -&gt; {@code SUM(x)}</li>
     *   <li>{@code COUNT(x)} -&gt; {@code COUNT(x)}</li>
     *   <li>{@code MAX(x)} -&gt; {@code MAX(x)}</li>
     *   <li>{@code MIN(x)} -&gt; {@code MIN(x)}</li>
     *   <li>{@code COUNT_DISTINCT(x)} -&gt; {@code COUNT(DISTINCT x)}</li>
     * </ul>
     */
    @Override
    protected SqlFragment translateAggregates(List<? extends NamedExpression> aggregates) {
        // --- Aggregate translation (stubbed) ---
        // In a real implementation:
        // SqlExpressionTranslator translator = new SqlExpressionTranslator(dialect);
        // List<String> selectItems = aggregates.stream()
        // .map(agg -> translator.toSelectItem(agg))
        // .toList();
        // return new SqlFragment(String.join(", ", selectItems));

        // Stub
        return new SqlFragment("/* aggregates */");
    }

    @Override
    protected SqlFragment translateGroupBy(List<Expression> groupBy) {
        if (groupBy == null || groupBy.isEmpty()) {
            return null;
        }
        // --- GROUP BY translation (stubbed) ---
        // In a real implementation:
        // List<String> columns = groupBy.stream()
        // .map(expr -> translator.toExpression(expr))
        // .toList();
        // return new SqlFragment(String.join(", ", columns));

        // Stub
        return new SqlFragment("/* group by */");
    }

    /**
     * Translate ES|QL ORDER BY to SQL ORDER BY clause.
     *
     * <p>Maps ES|QL order to SQL. Dialect-specific handling (e.g., MySQL's
     * NULLS FIRST/LAST workaround) would be configured per database.
     */
    @Override
    protected SqlFragment translateOrderBy(List<Order> orders) {
        // --- ORDER BY translation (stubbed) ---
        // In a real implementation:
        // List<String> clauses = orders.stream()
        // .map(order -> {
        // String col = translator.toExpression(order.child());
        // String dir = order.direction() == Order.OrderDirection.ASC ? "ASC" : "DESC";
        // return col + " " + dir;
        // })
        // .toList();
        // return new SqlFragment(String.join(", ", clauses));

        // Stub
        return new SqlFragment("/* order by */");
    }

    @Override
    protected SqlPlan applyBuiltSql(SqlPlan plan, SqlBuilder sql) {
        JdbcPlan jdbcPlan = (JdbcPlan) plan;
        return jdbcPlan.withBuiltSql(sql.build());
    }

    // =========================================================================
    // PHYSICAL PLANNING & EXECUTION
    // =========================================================================

    // createPhysicalPlan() is inherited from SqlDataSource — creates a DataSourceExec
    // wrapping the JdbcPlan with all pushed-down operations and the built SQL.

    /**
     * Create a source operator that executes the built SQL via JDBC and streams results.
     *
     * <p>Returns a factory that creates one {@link JdbcSourceOperator} per driver.
     * Each operator opens a JDBC connection, executes the SQL, and converts
     * ResultSet rows into ES|QL Pages using the BlockFactory.
     */
    @Override
    public SourceOperator.SourceOperatorFactory createSourceOperator(DataSourcePartition partition, ExecutionContext context) {
        JdbcPlan plan = (JdbcPlan) partition.plan();
        String sql = plan.builtSql() != null ? plan.builtSql() : "SELECT * FROM " + plan.tableName();
        List<Attribute> schema = plan.output();
        String description = "JdbcSourceOperator[" + plan.tableName() + "]";
        logger.debug("Creating source operator for [{}], executing SQL: {}", plan.tableName(), sql);

        return new SourceOperator.SourceOperatorFactory() {
            @Override
            public SourceOperator get(DriverContext driverContext) {
                return new JdbcSourceOperator(sql, schema, driverContext.blockFactory());
            }

            @Override
            public String describe() {
                return description;
            }
        };
    }

    /**
     * Source operator that reads from a JDBC ResultSet and produces Pages.
     *
     * <p>Each call to {@link #getOutput()} reads up to {@code BATCH_SIZE} rows from the
     * ResultSet, converts them to columnar Blocks, and returns a Page.
     *
     * <p>This demonstrates the row-oriented pattern: read rows from the source,
     * accumulate into Block.Builders, build a Page.
     */
    private static class JdbcSourceOperator extends SourceOperator {

        private static final int BATCH_SIZE = 1000;

        private final String sql;
        private final List<Attribute> schema;
        private final BlockFactory blockFactory;
        // In a real implementation, these would be actual JDBC objects:
        // private final Connection connection;
        // private final PreparedStatement statement;
        // private final ResultSet resultSet;
        private boolean finished;

        JdbcSourceOperator(String sql, List<Attribute> schema, BlockFactory blockFactory) {
            this.sql = sql;
            this.schema = schema;
            this.blockFactory = blockFactory;
            // --- JDBC connection (stubbed) ---
            // this.connection = DriverManager.getConnection(url, username, password);
            // this.statement = connection.prepareStatement(sql);
            // this.resultSet = statement.executeQuery();
            this.finished = true; // Stub: no real data to read
        }

        @Override
        public void finish() {
            finished = true;
        }

        @Override
        public boolean isFinished() {
            return finished;
        }

        @Override
        public Page getOutput() {
            if (finished) {
                return null;
            }

            // --- Row-oriented reading pattern ---
            // 1. Create a Block.Builder for each output column
            // Block.Builder[] builders = new Block.Builder[schema.size()];
            // for (int col = 0; col < schema.size(); col++) {
            // ElementType type = mapAttributeToElementType(schema.get(col));
            // builders[col] = type.newBlockBuilder(blockFactory, BATCH_SIZE);
            // }
            //
            // 2. Read rows from ResultSet, append to builders
            // int rows = 0;
            // while (resultSet.next() && rows < BATCH_SIZE) {
            // for (int col = 0; col < schema.size(); col++) {
            // appendValue(builders[col], resultSet, col + 1, schema.get(col));
            // }
            // rows++;
            // }
            //
            // 3. Check if exhausted
            // if (rows == 0) {
            // for (Block.Builder b : builders) b.close();
            // finished = true;
            // return null;
            // }
            // if (resultSet.isAfterLast()) {
            // finished = true;
            // }
            //
            // 4. Build blocks and assemble Page
            // Block[] blocks = new Block[schema.size()];
            // for (int col = 0; col < schema.size(); col++) {
            // blocks[col] = builders[col].build();
            // }
            // return new Page(rows, blocks);

            return null; // Stub: no real data
        }

        @Override
        public void close() {
            // --- JDBC cleanup ---
            // resultSet.close();
            // statement.close();
            // connection.close();
        }
    }

    // =========================================================================
    // STUBS - would use real JDBC
    // =========================================================================

    /**
     * Check if an expression looks like a SQL query (vs a table name).
     */
    private boolean looksLikeQuery(String expression) {
        String trimmed = expression.trim().toUpperCase(Locale.ROOT);
        return trimmed.startsWith("SELECT") || trimmed.startsWith("WITH");
    }

    /**
     * Read table schema via JDBC.
     * Stub - real implementation uses {@code java.sql.DatabaseMetaData} or {@code ResultSetMetaData}.
     */
    private List<Attribute> readTableSchema(String url, String username, String password, String tableName) {
        // Stub: return empty schema. Real implementation would:
        // 1. Open JDBC connection
        // 2. Execute "SELECT * FROM <table> LIMIT 0" or use DatabaseMetaData
        // 3. Convert ResultSetMetaData columns to ES|QL Attributes
        // 4. Map JDBC types: VARCHAR->keyword, INT->integer, BIGINT->long,
        // DOUBLE->double, TIMESTAMP->datetime, DECIMAL->double, etc.
        return List.of();
    }

    /**
     * Translate an ES|QL expression to a SQL fragment.
     * Stub - real implementation would walk the expression tree.
     */
    private String stubTranslateExpression(Expression filter) {
        // Stub: return a placeholder. Real implementation would:
        // 1. Walk the ES|QL expression tree
        // 2. Map field references to column names
        // 3. Map operators: Equals->"=", GreaterThan->">", etc.
        // 4. Map functions: StartsWith->"LIKE ?%", Contains->"LIKE %?%"
        // 5. Use parameterized queries to prevent SQL injection
        // 6. Return null for untranslatable expressions
        return "1 = 1 /* stub */";
    }
}
