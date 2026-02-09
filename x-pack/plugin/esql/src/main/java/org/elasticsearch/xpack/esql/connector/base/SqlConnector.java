/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.base;

import org.elasticsearch.xpack.esql.connector.Connector;
import org.elasticsearch.xpack.esql.connector.ConnectorCapabilities;
import org.elasticsearch.xpack.esql.connector.ConnectorPlan;
import org.elasticsearch.xpack.esql.connector.DistributionHints;
import org.elasticsearch.xpack.esql.connector.ConnectorPartition;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;

/**
 * Base class for SQL connectors (Postgres, MySQL, Oracle, etc.).
 *
 * <h2>What This Base Class Provides</h2>
 *
 * <p>This class implements the {@link Connector} SPI methods with SQL-appropriate
 * behavior. It accumulates operations into a SQL query that the database executes.
 *
 * <p><b>{@link #optimizationRules} implementation:</b> Provides default rules that push
 * Filter, Limit, OrderBy, and Aggregate nodes into {@link SqlPlan} leaves. Each rule
 * checks translatability via the abstract translate methods; if translation fails, the
 * operation stays in the ES|QL plan. A finalization rule builds the SQL from accumulated state.
 *
 * <p><b>{@link #planPartitions} implementation:</b> Returns a single partition since
 * SQL databases typically execute on coordinator only. Sharded databases can override.
 *
 * <h2>Subclass Responsibilities</h2>
 * <ul>
 *   <li>Define a connector-specific {@link SqlPlan} implementation</li>
 *   <li>{@link #getTableName} - Extract table name from plan for SQL FROM clause</li>
 *   <li>{@link #translateFilter} - Convert ESQL filter to SQL WHERE fragment</li>
 *   <li>{@link #translateAggregates} - Convert ESQL aggregates to SQL SELECT fragment</li>
 *   <li>{@link #translateGroupBy} - Convert ESQL GROUP BY to SQL GROUP BY fragment</li>
 *   <li>{@link #translateOrderBy} - Convert ESQL ORDER BY to SQL ORDER BY fragment</li>
 *   <li>{@link #applyBuiltSql} - Return updated plan with the built SQL</li>
 *   <li>{@link #createPhysicalPlan} - Create physical plan node for this source</li>
 *   <li>{@link Connector#createSourceOperator} - Execute SQL and stream results</li>
 * </ul>
 *
 * @see SqlPlan
 */
public abstract class SqlConnector implements Connector {

    @Override
    public ConnectorCapabilities capabilities() {
        return ConnectorCapabilities.forCoordinatorOnly();
    }

    /**
     * Returns default optimization rules for SQL connectors:
     * <ul>
     *   <li>{@code PushFilterToSql} - pushes Filter into the leaf if translatable to SQL WHERE</li>
     *   <li>{@code PushLimitToSql} - pushes Limit into the leaf as SQL LIMIT</li>
     *   <li>{@code PushOrderByToSql} - pushes OrderBy into the leaf as SQL ORDER BY</li>
     *   <li>{@code PushAggregateToSql} - pushes Aggregate into the leaf as SQL GROUP BY</li>
     *   <li>{@code BuildSql} - finalization rule that builds the SQL string from accumulated state</li>
     * </ul>
     *
     * <p>Rules are non-static inner classes that call this connector's abstract translate methods.
     * The {@code BuildSql} rule must run last (after all pushdown rules).
     */
    @Override
    public List<Rule<?, LogicalPlan>> optimizationRules() {
        return List.of(new PushFilterToSql(), new PushLimitToSql(), new PushOrderByToSql(), new PushAggregateToSql(), new BuildSql());
    }

    // =========================================================================
    // OPTIMIZATION RULES
    // =========================================================================

    /**
     * Pushes a Filter node into a SqlPlan leaf when the filter is translatable to SQL.
     */
    private class PushFilterToSql extends OptimizerRules.OptimizerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter filter) {
            if (filter.child() instanceof SqlPlan == false) {
                return filter;
            }
            SqlPlan sqlPlan = (SqlPlan) filter.child();
            if (sqlPlan.connector() != SqlConnector.this) {
                return filter;
            }
            SqlFragment where = translateFilter(filter.condition());
            if (where == null) {
                return filter; // Cannot translate — ES|QL will evaluate
            }
            return (LogicalPlan) sqlPlan.withFilter(filter.condition());
        }
    }

    /**
     * Pushes a Limit node into a SqlPlan leaf when the database supports LIMIT.
     */
    private class PushLimitToSql extends OptimizerRules.OptimizerRule<Limit> {

        @Override
        protected LogicalPlan rule(Limit limit) {
            if (limit.child() instanceof SqlPlan == false) {
                return limit;
            }
            SqlPlan sqlPlan = (SqlPlan) limit.child();
            if (sqlPlan.connector() != SqlConnector.this) {
                return limit;
            }
            if (limit.limit().foldable() == false) {
                return limit;
            }
            int limitValue = ((Number) limit.limit().fold(org.elasticsearch.xpack.esql.core.expression.FoldContext.small())).intValue();
            if (translateLimit(limitValue) == false) {
                return limit; // Cannot translate — ES|QL will evaluate
            }
            return (LogicalPlan) sqlPlan.withLimit(limitValue);
        }
    }

    /**
     * Pushes an OrderBy node into a SqlPlan leaf when translatable to SQL ORDER BY.
     */
    private class PushOrderByToSql extends OptimizerRules.OptimizerRule<OrderBy> {

        @Override
        protected LogicalPlan rule(OrderBy orderBy) {
            if (orderBy.child() instanceof SqlPlan == false) {
                return orderBy;
            }
            SqlPlan sqlPlan = (SqlPlan) orderBy.child();
            if (sqlPlan.connector() != SqlConnector.this) {
                return orderBy;
            }
            SqlFragment orderBySql = translateOrderBy(orderBy.order());
            if (orderBySql == null) {
                return orderBy; // Cannot translate
            }
            return (LogicalPlan) sqlPlan.withOrderBy(orderBy.order());
        }
    }

    /**
     * Pushes an Aggregate node into a SqlPlan leaf when translatable to SQL GROUP BY.
     */
    private class PushAggregateToSql extends OptimizerRules.OptimizerRule<Aggregate> {

        @Override
        protected LogicalPlan rule(Aggregate aggregate) {
            if (aggregate.child() instanceof SqlPlan == false) {
                return aggregate;
            }
            SqlPlan sqlPlan = (SqlPlan) aggregate.child();
            if (sqlPlan.connector() != SqlConnector.this) {
                return aggregate;
            }
            SqlFragment select = translateAggregates(aggregate.aggregates());
            if (select == null) {
                return aggregate; // Cannot translate
            }
            SqlPlan.Aggregation agg = new SqlPlan.Aggregation(aggregate.groupings(), aggregate.aggregates());
            return (LogicalPlan) sqlPlan.withAggregation(agg).withOutput(aggregate.output());
        }
    }

    /**
     * Finalization rule that builds the SQL string from accumulated operations.
     *
     * <p>Must run after all pushdown rules. Walks the tree to find SqlPlan nodes
     * with accumulated operations and builds the SQL using {@link SqlBuilder}.
     */
    private class BuildSql extends OptimizerRules.OptimizerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan instanceof SqlPlan == false) {
                return plan;
            }
            SqlPlan sqlPlan = (SqlPlan) plan;
            if (sqlPlan.connector() != SqlConnector.this) {
                return plan;
            }
            // Only build SQL if operations were accumulated
            if (sqlPlan.hasFilter() == false
                && sqlPlan.hasLimit() == false
                && sqlPlan.hasOrderBy() == false
                && sqlPlan.hasAggregation() == false) {
                return plan;
            }

            SqlBuilder sql = SqlBuilder.from(getTableName(sqlPlan));

            if (sqlPlan.hasFilter()) {
                SqlFragment where = translateFilter(sqlPlan.filter());
                if (where != null) {
                    sql = sql.where(where);
                }
            }
            if (sqlPlan.hasAggregation()) {
                SqlFragment select = translateAggregates(sqlPlan.aggregation().aggregates());
                SqlFragment groupBySql = translateGroupBy(sqlPlan.aggregation().groupBy());
                if (select != null) {
                    sql = sql.aggregate(select, groupBySql);
                }
            }
            if (sqlPlan.hasOrderBy()) {
                SqlFragment orderBySql = translateOrderBy(sqlPlan.orderBy());
                if (orderBySql != null) {
                    sql = sql.orderBy(orderBySql);
                }
            }
            if (sqlPlan.hasLimit()) {
                sql = sql.limit(sqlPlan.limit());
            }
            sql = sql.select(sqlPlan.output());

            return (LogicalPlan) applyBuiltSql(sqlPlan, sql);
        }
    }

    /**
     * Called by the physical planner on the coordinator.
     *
     * <p>Default returns single partition for coordinator-only execution.
     * Sharded databases can override to create partitions per shard for parallel execution.
     */
    @Override
    public List<ConnectorPartition> planPartitions(ConnectorPlan plan, DistributionHints hints) {
        return List.of(ConnectorPartition.single(plan));
    }

    // createPhysicalPlan() is inherited from Connector — creates a ConnectorExec
    // wrapping the SqlPlan with all pushed-down operations and the built SQL.

    // =========================================================================
    // ABSTRACT METHODS - Subclasses implement these
    // =========================================================================

    /**
     * Get the table name from the plan.
     *
     * @param plan The SQL plan
     * @return The table name for SQL generation
     */
    protected abstract String getTableName(SqlPlan plan);

    /**
     * Translate an ESQL filter to SQL WHERE clause.
     *
     * @param filter The ESQL filter expression
     * @return SQL fragment for WHERE clause, or null if can't translate
     */
    protected abstract SqlFragment translateFilter(Expression filter);

    /**
     * Check whether a LIMIT can be pushed to this SQL database.
     *
     * <p>Default returns true since most SQL databases support some form of limit
     * (LIMIT, TOP, FETCH FIRST, ROWNUM). Override to return false if the database
     * does not support row limiting.
     *
     * @param limit The limit value
     * @return true if the limit can be pushed down
     */
    protected boolean translateLimit(int limit) {
        return true;
    }

    /**
     * Translate ESQL aggregates to SQL SELECT clause.
     *
     * @param aggregates The aggregate expressions
     * @return SQL fragment for SELECT clause, or null if can't translate
     */
    protected abstract SqlFragment translateAggregates(List<? extends NamedExpression> aggregates);

    /**
     * Translate ESQL GROUP BY to SQL GROUP BY clause.
     *
     * @param groupBy The GROUP BY expressions
     * @return SQL fragment for GROUP BY clause (can be null for no GROUP BY)
     */
    protected abstract SqlFragment translateGroupBy(List<Expression> groupBy);

    /**
     * Translate ESQL ORDER BY to SQL ORDER BY clause.
     *
     * @param orders The ORDER BY specifications
     * @return SQL fragment for ORDER BY clause, or null if can't translate
     */
    protected abstract SqlFragment translateOrderBy(List<Order> orders);

    /**
     * Apply the built SQL to the plan.
     *
     * @param plan The SQL plan
     * @param sql The built SQL
     * @return Updated plan containing the SQL for execution
     */
    protected abstract SqlPlan applyBuiltSql(SqlPlan plan, SqlBuilder sql);

    // =========================================================================
    // SQL BUILDER
    // =========================================================================

    /**
     * Immutable SQL query builder.
     * Accumulates clauses and produces a SQL string.
     */
    public record SqlBuilder(
        String table,
        List<String> selectColumns,
        SqlFragment where,
        SqlFragment orderBy,
        Integer limit,
        SqlFragment aggregateSelect,
        SqlFragment groupBy
    ) {
        /**
         * Create a builder for a table.
         */
        public static SqlBuilder from(String table) {
            return new SqlBuilder(table, List.of("*"), null, null, null, null, null);
        }

        /**
         * Set the SELECT columns.
         */
        public SqlBuilder select(List<Attribute> columns) {
            List<String> names = columns.stream().map(Attribute::name).toList();
            return new SqlBuilder(table, names, where, orderBy, limit, aggregateSelect, groupBy);
        }

        /**
         * Set the WHERE clause.
         */
        public SqlBuilder where(SqlFragment where) {
            return new SqlBuilder(table, selectColumns, where, orderBy, limit, aggregateSelect, groupBy);
        }

        /**
         * Set the ORDER BY clause.
         */
        public SqlBuilder orderBy(SqlFragment orderBy) {
            return new SqlBuilder(table, selectColumns, where, orderBy, limit, aggregateSelect, groupBy);
        }

        /**
         * Set the LIMIT.
         */
        public SqlBuilder limit(int limit) {
            return new SqlBuilder(table, selectColumns, where, orderBy, limit, aggregateSelect, groupBy);
        }

        /**
         * Set aggregation (SELECT and GROUP BY).
         */
        public SqlBuilder aggregate(SqlFragment select, SqlFragment groupBy) {
            return new SqlBuilder(table, selectColumns, where, orderBy, limit, select, groupBy);
        }

        /**
         * Build the SQL string.
         */
        public String build() {
            StringBuilder sql = new StringBuilder("SELECT ");

            // SELECT clause
            if (aggregateSelect != null) {
                sql.append(aggregateSelect.sql());
            } else {
                sql.append(String.join(", ", selectColumns));
            }

            // FROM clause
            sql.append(" FROM ").append(table);

            // WHERE clause
            if (where != null) {
                sql.append(" WHERE ").append(where.sql());
            }

            // GROUP BY clause
            if (groupBy != null) {
                sql.append(" GROUP BY ").append(groupBy.sql());
            }

            // ORDER BY clause
            if (orderBy != null) {
                sql.append(" ORDER BY ").append(orderBy.sql());
            }

            // LIMIT clause
            if (limit != null) {
                sql.append(" LIMIT ").append(limit);
            }

            return sql.toString();
        }

        /**
         * Check if this is an aggregation query.
         */
        public boolean isAggregation() {
            return aggregateSelect != null;
        }
    }

    /**
     * A fragment of SQL (e.g., a WHERE condition, ORDER BY clause).
     *
     * @param sql The SQL string
     * @param parameters Bind parameters (for prepared statements)
     */
    public record SqlFragment(String sql, List<Object> parameters) {
        /**
         * Create a fragment with no parameters.
         */
        public SqlFragment(String sql) {
            this(sql, List.of());
        }

        /**
         * Create a fragment with parameters.
         */
        public static SqlFragment withParams(String sql, Object... params) {
            return new SqlFragment(sql, List.of(params));
        }
    }
}
