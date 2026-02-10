/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * Convenience base class for connector optimization rules that push operations into connector plan leaves.
 *
 * <p>This is a <b>helper class</b>, not part of the core Connector SPI. Connectors are free to write
 * optimization rules directly as {@link OptimizerRules.OptimizerRule} subclasses — this class simply
 * extracts the guard logic that most pushdown rules need:
 * <ol>
 *   <li>Check that the plan's child is the expected connector plan type</li>
 *   <li>Check that the plan belongs to the correct connector instance (for multi-connector queries)</li>
 *   <li>Delegate to {@link #pushDown} for the connector-specific logic</li>
 * </ol>
 *
 * <p>Both {@link org.elasticsearch.xpack.esql.connector.base.SqlConnector} and
 * {@link org.elasticsearch.xpack.esql.connector.base.DataLakeConnector} use this class for their
 * built-in pushdown rules.
 *
 * <p>Usage example:
 * <pre>{@code
 * private class PushFilterToSql extends ConnectorPushdownRule<Filter, SqlPlan> {
 *     PushFilterToSql() { super(SqlConnector.this, SqlPlan.class); }
 *
 *     @Override
 *     protected LogicalPlan pushDown(Filter filter, SqlPlan sqlPlan) {
 *         SqlFragment where = translateFilter(filter.condition());
 *         if (where == null) return filter;
 *         return sqlPlan.withFilter(filter.condition());
 *     }
 * }
 * }</pre>
 *
 * @param <T> The ES|QL plan node type to match (e.g., Filter, Limit, OrderBy)
 * @param <P> The connector plan type to expect as the child (e.g., SqlPlan, DataLakePlan)
 */
public abstract class ConnectorPushdownRule<T extends UnaryPlan, P extends ConnectorPlan> extends OptimizerRules.OptimizerRule<T> {

    private final Logger logger = LogManager.getLogger(getClass());

    private final Connector connector;
    private final Class<P> planType;

    /**
     * @param connector the connector instance to match against (identity check)
     * @param planType the expected connector plan class
     */
    protected ConnectorPushdownRule(Connector connector, Class<P> planType) {
        this.connector = connector;
        this.planType = planType;
    }

    @Override
    protected LogicalPlan rule(T plan) {
        if (planType.isInstance(plan.child()) == false) {
            return plan;
        }
        P connectorPlan = planType.cast(plan.child());
        if (connectorPlan.connector() != connector) {
            return plan;
        }
        logger.trace(
            "Pushing [{}] into [{}] for connector [{}]",
            plan.getClass().getSimpleName(),
            planType.getSimpleName(),
            connector.type()
        );
        LogicalPlan result = pushDown(plan, connectorPlan);
        if (result == plan) {
            logger.trace("Pushdown skipped — operation not translatable");
        }
        return result;
    }

    /**
     * Apply the pushdown logic.
     *
     * <p>Called only when the child is the expected connector plan type and belongs
     * to the correct connector instance. Return the original {@code plan} if the
     * operation cannot be pushed down (e.g., untranslatable filter).
     *
     * @param plan the ES|QL plan node (Filter, Limit, etc.)
     * @param connectorPlan the connector's plan leaf
     * @return the rewritten plan, or the original if pushdown is not possible
     */
    protected abstract LogicalPlan pushDown(T plan, P connectorPlan);
}
