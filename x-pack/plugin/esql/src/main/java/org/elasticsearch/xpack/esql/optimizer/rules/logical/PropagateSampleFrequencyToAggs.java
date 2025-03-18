/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.RandomSample;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.concurrent.atomic.AtomicReference;

public class PropagateSampleFrequencyToAggs extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        AtomicReference<Expression> sampleProbability = new AtomicReference<>(null);
        return logicalPlan.transformUp(plan -> {
            if (plan instanceof RandomSample randomSample) {
                sampleProbability.set(randomSample.probability());
            }
            if (sampleProbability.get() != null) {
                plan = plan.transformExpressionsOnly(AggregateFunction.class, af -> af.correctForSampling(sampleProbability.get()));
            }
            return plan;
        });
    }
}
