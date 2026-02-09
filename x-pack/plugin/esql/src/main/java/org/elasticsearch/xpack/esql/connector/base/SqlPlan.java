/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector.base;

import org.elasticsearch.xpack.esql.connector.ConnectorPlan;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.Order;

import java.util.List;

/**
 * Base class for SQL connector plan nodes (PostgreSQL, MySQL, Oracle, etc.).
 *
 * <p>SQL connectors typically support full pushdown:
 * <ul>
 *   <li>Filter to SQL WHERE</li>
 *   <li>ORDER BY to SQL ORDER BY</li>
 *   <li>LIMIT to SQL LIMIT</li>
 *   <li>Aggregations to SQL SELECT with GROUP BY</li>
 * </ul>
 *
 * <p>Operations are accumulated into a SQL query that the database executes,
 * avoiding the need to stream all data and process locally.
 *
 * <p>All methods that modify state return new instances (immutable pattern).
 *
 * @see SqlConnector
 */
public abstract class SqlPlan extends ConnectorPlan {

    protected SqlPlan(Source source) {
        super(source);
    }

    /**
     * The filter expression to apply, or null if none.
     */
    public abstract Expression filter();

    /**
     * The limit to apply, or null if none.
     */
    public abstract Integer limit();

    /**
     * The ORDER BY specification, or null if none.
     */
    public abstract List<Order> orderBy();

    /**
     * The aggregation specification, or null if none.
     */
    public abstract Aggregation aggregation();

    /**
     * Whether a filter is present.
     */
    public boolean hasFilter() {
        return filter() != null;
    }

    /**
     * Whether a limit is present.
     */
    public boolean hasLimit() {
        return limit() != null;
    }

    /**
     * Whether ORDER BY is present.
     */
    public boolean hasOrderBy() {
        return orderBy() != null && orderBy().isEmpty() == false;
    }

    /**
     * Whether aggregation is present.
     */
    public boolean hasAggregation() {
        return aggregation() != null;
    }

    /**
     * Create a copy with the given filter.
     */
    public abstract SqlPlan withFilter(Expression filter);

    /**
     * Create a copy with the given limit.
     */
    public abstract SqlPlan withLimit(Integer limit);

    /**
     * Create a copy with the given ORDER BY.
     */
    public abstract SqlPlan withOrderBy(List<Order> orderBy);

    /**
     * Create a copy with the given aggregation.
     */
    public abstract SqlPlan withAggregation(Aggregation aggregation);

    /**
     * Create a copy with the given output columns.
     */
    public abstract SqlPlan withOutput(List<Attribute> output);

    /**
     * Aggregation specification (aggregates + grouping).
     *
     * @param groupBy GROUP BY expressions
     * @param aggregates Aggregate expressions with names
     */
    public record Aggregation(List<Expression> groupBy, List<? extends NamedExpression> aggregates) {
        /**
         * Whether GROUP BY is present.
         */
        public boolean hasGroupBy() {
            return groupBy != null && groupBy.isEmpty() == false;
        }
    }
}
