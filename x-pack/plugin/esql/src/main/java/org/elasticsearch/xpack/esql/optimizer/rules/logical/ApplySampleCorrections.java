/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HasSampleCorrection;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.RandomSample;
import org.elasticsearch.xpack.esql.rule.Rule;

public class ApplySampleCorrections extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        Holder<Expression> sampleProbability = new Holder<>(null);
        return logicalPlan.transformUp(plan -> {
            if (plan instanceof RandomSample randomSample) {
                sampleProbability.set(randomSample.probability());
            }
            if (plan instanceof Aggregate && sampleProbability.get() != null) {
                plan = plan.transformExpressionsOnly(
                    e -> e instanceof HasSampleCorrection hsc && hsc.sampleCorrected() == false ? hsc.sampleCorrection(sampleProbability.get()) : e
                );
                sampleProbability.set(null);
            }
            return plan;
        });
    }
}
