/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.sql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.connector.Connector;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Logical plan node for reading from a SQL database via JDBC.
 *
 * <p>This is an example connector implementation showing how a SQL-style
 * connector creates its plan node. Operations are accumulated as SQL fragments
 * that are combined into a single query for the database to execute.
 *
 * <h2>Lifecycle</h2>
 * <ol>
 *   <li>{@link JdbcConnector#resolve} connects via JDBC, reads table schema, creates this node</li>
 *   <li>Connector optimization rules push filter/limit/orderBy/aggregation via {@code with*()} methods</li>
 *   <li>{@code BuildSql} rule translates accumulated operations to SQL</li>
 *   <li>At execution time, the built SQL is sent to the database via JDBC</li>
 * </ol>
 *
 * <h2>Example queries</h2>
 * <pre>
 * FROM my_db:users | WHERE age &gt; 21 | SORT name | LIMIT 100
 * FROM my_db:query("SELECT * FROM orders WHERE total &gt; 1000") | STATS avg(total) BY region
 * </pre>
 *
 * <p>After optimization, the first query becomes:
 * {@code SELECT name, age FROM users WHERE age > 21 ORDER BY name LIMIT 100}
 */
public class JdbcPlan extends SqlPlan {

    private final JdbcConnector connector;
    private final String location;
    private final List<Attribute> output;

    /** The table name or subquery expression from the data source */
    private final String tableName;

    /** The fully built SQL query (set after BuildSql rule translates operations) */
    private final String builtSql;

    private final Expression filter;
    private final Integer limit;
    private final List<Order> orderBy;
    private final Aggregation aggregation;

    /**
     * Full constructor used by {@link NodeInfo} for plan transformations.
     */
    public JdbcPlan(
        Source source,
        JdbcConnector connector,
        String location,
        List<Attribute> output,
        String tableName,
        String builtSql,
        Expression filter,
        Integer limit,
        List<Order> orderBy,
        Aggregation aggregation
    ) {
        super(source);
        this.connector = connector;
        this.location = location;
        this.output = output;
        this.tableName = tableName;
        this.builtSql = builtSql;
        this.filter = filter;
        this.limit = limit;
        this.orderBy = orderBy;
        this.aggregation = aggregation;
    }

    // =========================================================================
    // Serialization (stub - real implementation would register with PlanNameRegistry)
    // =========================================================================

    @Override
    public String getWriteableName() {
        return "esql.plan.jdbc";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Serialization not yet implemented for JDBC connector");
    }

    // =========================================================================
    // ConnectorPlan
    // =========================================================================

    @Override
    public Connector connector() {
        return connector;
    }

    @Override
    public String location() {
        return location;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    // =========================================================================
    // SqlPlan
    // =========================================================================

    @Override
    public Expression filter() {
        return filter;
    }

    @Override
    public Integer limit() {
        return limit;
    }

    @Override
    public List<Order> orderBy() {
        return orderBy;
    }

    @Override
    public Aggregation aggregation() {
        return aggregation;
    }

    @Override
    public SqlPlan withFilter(Expression filter) {
        return new JdbcPlan(source(), connector, location, output, tableName, builtSql, filter, limit, orderBy, aggregation);
    }

    @Override
    public SqlPlan withLimit(Integer limit) {
        return new JdbcPlan(source(), connector, location, output, tableName, builtSql, filter, limit, orderBy, aggregation);
    }

    @Override
    public SqlPlan withOrderBy(List<Order> orderBy) {
        return new JdbcPlan(source(), connector, location, output, tableName, builtSql, filter, limit, orderBy, aggregation);
    }

    @Override
    public SqlPlan withAggregation(Aggregation aggregation) {
        return new JdbcPlan(source(), connector, location, output, tableName, builtSql, filter, limit, orderBy, aggregation);
    }

    @Override
    public SqlPlan withOutput(List<Attribute> output) {
        return new JdbcPlan(source(), connector, location, output, tableName, builtSql, filter, limit, orderBy, aggregation);
    }

    // =========================================================================
    // JDBC-specific accessors
    // =========================================================================

    /**
     * The table name or subquery expression.
     * For a simple table reference like {@code users}, this is {@code "users"}.
     * For a native query like {@code query("SELECT ...")}, this is {@code "(SELECT ...) AS subq"}.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * The fully built SQL query, or null if not yet built.
     * Set by the {@code BuildSql} rule after translating all operations.
     */
    public String builtSql() {
        return builtSql;
    }

    /**
     * Return a copy with the built SQL set.
     */
    public JdbcPlan withBuiltSql(String builtSql) {
        return new JdbcPlan(source(), connector, location, output, tableName, builtSql, filter, limit, orderBy, aggregation);
    }

    // =========================================================================
    // LeafPlan / Node
    // =========================================================================

    @Override
    protected NodeInfo<JdbcPlan> info() {
        return NodeInfo.create(this, JdbcPlan::new, connector, location, output, tableName, builtSql, filter, limit, orderBy, aggregation);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(connector, location, output, tableName, builtSql, filter, limit, orderBy, aggregation);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JdbcPlan other = (JdbcPlan) obj;
        return Objects.equals(connector, other.connector)
            && Objects.equals(location, other.location)
            && Objects.equals(output, other.output)
            && Objects.equals(tableName, other.tableName)
            && Objects.equals(builtSql, other.builtSql)
            && Objects.equals(filter, other.filter)
            && Objects.equals(limit, other.limit)
            && Objects.equals(orderBy, other.orderBy)
            && Objects.equals(aggregation, other.aggregation);
    }
}
