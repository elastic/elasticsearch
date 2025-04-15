/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Foldables;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.SampleBreaking;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

public class PushDownAndCombineSample extends OptimizerRules.ParameterizedOptimizerRule<Sample, LogicalOptimizerContext> {

    public PushDownAndCombineSample() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Sample sample, LogicalOptimizerContext context) {
        LogicalPlan plan = sample;
        var child = sample.child();
        if (child instanceof Sample sampleChild) {
            var probability = combinedProbability(context, sample, sampleChild);
            var seed = combinedSeed(context, sample, sampleChild);
            plan = new Sample(sample.source(), probability, seed, sampleChild.child());
        } else if (child instanceof UnaryPlan unaryChild && child instanceof SampleBreaking == false) {
            plan = unaryChild.replaceChild(sample.replaceChild(unaryChild.child()));
        }
        return plan;
    }

    private static Expression combinedProbability(LogicalOptimizerContext context, Sample parent, Sample child) {
        var parentProbability = (double) Foldables.valueOf(context.foldCtx(), parent.probability());
        var childProbability = (double) Foldables.valueOf(context.foldCtx(), child.probability());
        return Literal.of(parent.probability(), parentProbability * childProbability);
    }

    private static Expression combinedSeed(LogicalOptimizerContext context, Sample parent, Sample child) {
        var parentSeed = parent.seed();
        var childSeed = child.seed();
        Expression seed;
        if (parentSeed != null) {
            if (childSeed != null) {
                var seedValue = (int) Foldables.valueOf(context.foldCtx(), parentSeed);
                seedValue ^= (int) Foldables.valueOf(context.foldCtx(), childSeed);
                seed = Literal.of(parentSeed, seedValue);
            } else {
                seed = parentSeed;
            }
        } else {
            seed = childSeed;
        }
        return seed;
    }
}
