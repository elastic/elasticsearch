/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.HasSampleCorrection;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

public class ApplySampleCorrections extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan logicalPlan) {
        List<Expression> sampleProbabilities = new ArrayList<>();
        return logicalPlan.transformUp(plan -> {
            if (plan instanceof Sample sample) {
                sampleProbabilities.add(sample.probability());
            }
            if (plan instanceof Aggregate && sampleProbabilities.isEmpty() == false) {
                plan = plan.transformExpressionsOnly(
                    e -> e instanceof HasSampleCorrection hsc && hsc.isSampleCorrected() == false
                        ? hsc.sampleCorrection(getSampleProbability(sampleProbabilities, e.source()))
                        : e
                );
            }
            // Operations that map many to many rows break/reset sampling.
            // Therefore, the sample probabilities are cleared.
            if (plan instanceof Aggregate || plan instanceof Limit) {
                sampleProbabilities.clear();
            }
            return plan;
        });
    }

    private static Expression getSampleProbability(List<Expression> sampleProbabilities, Source source) {
        Expression result = null;
        for (Expression probability : sampleProbabilities) {
            result = result == null ? probability : new Mul(source, result, probability);
        }
        return result;
    }
}
