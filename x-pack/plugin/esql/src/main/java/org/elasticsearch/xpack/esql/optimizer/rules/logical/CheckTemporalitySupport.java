/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.expression.function.TemporalityAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;

public class CheckTemporalitySupport extends OptimizerRules.ParameterizedOptimizerRule<TimeSeriesAggregate, LogicalOptimizerContext> {

    public CheckTemporalitySupport() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(TimeSeriesAggregate aggregate, LogicalOptimizerContext context) {
        return aggregate.transformExpressionsOnly(TimeSeriesAggregateFunction.class, agg -> {
            if (agg instanceof TemporalityAware temporalityAware) {
                return temporalityAware.checkTemporalitySupport(context.minimumVersion());
            }
            return agg;
        });
    }

}
