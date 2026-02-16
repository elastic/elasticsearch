/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * Convenience base class for data source optimization rules that push operations into data source plan leaves.
 *
 * <p>This is a <b>helper class</b>, not part of the core DataSource SPI. Data sources are free to write
 * optimization rules directly as {@link OptimizerRules.OptimizerRule} subclasses — this class simply
 * extracts the guard logic that most pushdown rules need:
 * <ol>
 *   <li>Check that the plan's child is the expected data source plan type</li>
 *   <li>Check that the plan belongs to the correct data source instance (for multi-data-source queries)</li>
 *   <li>Delegate to {@link #pushDown} for the data source-specific logic</li>
 * </ol>
 *
 * <p>{@link org.elasticsearch.xpack.esql.datasource.lakehouse.spi.LakehouseDataSource} uses this class for its
 * built-in pushdown rules.
 *
 * <p>Usage example:
 * <pre>{@code
 * private class PushFilterToLakehouse extends DataSourcePushdownRule<Filter, MyPlan> {
 *     PushFilterToLakehouse() { super(MyDataSource.this, MyPlan.class); }
 *
 *     @Override
 *     protected LogicalPlan pushDown(Filter filter, MyPlan plan) {
 *         Expression pushed = translateFilter(filter.condition());
 *         if (pushed == null) return filter;
 *         return plan.withFilter(filter.condition());
 *     }
 * }
 * }</pre>
 *
 * @param <T> The ES|QL plan node type to match (e.g., Filter, Limit)
 * @param <P> The data source plan type to expect as the child (e.g., LakehousePlan)
 */
public abstract class DataSourcePushdownRule<T extends UnaryPlan, P extends DataSourcePlan> extends OptimizerRules.OptimizerRule<T> {

    private final Logger logger = LogManager.getLogger(getClass());

    private final DataSource dataSource;
    private final Class<P> planType;

    /**
     * @param dataSource the data source instance to match against (identity check)
     * @param planType the expected data source plan class
     */
    protected DataSourcePushdownRule(DataSource dataSource, Class<P> planType) {
        this.dataSource = dataSource;
        this.planType = planType;
    }

    @Override
    protected LogicalPlan rule(T plan) {
        if (planType.isInstance(plan.child()) == false) {
            return plan;
        }
        P dataSourcePlan = planType.cast(plan.child());
        if (dataSourcePlan.dataSource() != dataSource) {
            return plan;
        }
        logger.trace(
            "Pushing [{}] into [{}] for data source [{}]",
            plan.getClass().getSimpleName(),
            planType.getSimpleName(),
            dataSource.type()
        );
        LogicalPlan result = pushDown(plan, dataSourcePlan);
        if (result == plan) {
            logger.trace("Pushdown skipped — operation not translatable");
        }
        return result;
    }

    /**
     * Apply the pushdown logic.
     *
     * <p>Called only when the child is the expected data source plan type and belongs
     * to the correct data source instance. Return the original {@code plan} if the
     * operation cannot be pushed down (e.g., untranslatable filter).
     *
     * @param plan the ES|QL plan node (Filter, Limit, etc.)
     * @param dataSourcePlan the data source's plan leaf
     * @return the rewritten plan, or the original if pushdown is not possible
     */
    protected abstract LogicalPlan pushDown(T plan, P dataSourcePlan);
}
