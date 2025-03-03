/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Foldables;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RandomSample;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

public class PushDownAndCombineRandomSample extends OptimizerRules.ParameterizedOptimizerRule<RandomSample, LogicalOptimizerContext> {

    public PushDownAndCombineRandomSample() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(RandomSample randomSample, LogicalOptimizerContext context) {
        LogicalPlan plan = randomSample;
        var child = randomSample.child();
        if (child instanceof RandomSample rsChild) {
            var parentProbability = (double) Foldables.valueOf(context.foldCtx(), randomSample.probability());
            var childProbability = (double) Foldables.valueOf(context.foldCtx(), rsChild.probability());
            var probability = Literal.of(randomSample.probability(), parentProbability * childProbability);

            var parentSeed = randomSample.seed() != null ? (int) Foldables.valueOf(context.foldCtx(), randomSample.seed()) : null;
            var childSeed = rsChild.seed() != null ? (int) Foldables.valueOf(context.foldCtx(), rsChild.seed()) : null;
            var seedValue = parentSeed != null ? Integer.valueOf(childSeed != null ? parentSeed ^ childSeed : parentSeed) : childSeed;
            var seed = seedValue != null ? Literal.of(randomSample.seed(), seedValue) : null;

            plan = new RandomSample(randomSample.source(), probability, seed, rsChild.child());
        } else if (child instanceof Enrich
            || child instanceof Eval
            || child instanceof Insist
            || child instanceof Project
            || child instanceof RegexExtract) {
                var unaryChild = (UnaryPlan) child;
                plan = unaryChild.replaceChild(randomSample.replaceChild(unaryChild.child()));
            }
        return plan;
    }
}
