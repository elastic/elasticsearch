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
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Insist;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
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
            var probability = combinedProbability(context, randomSample, rsChild);
            var seed = combinedSeed(context, randomSample, rsChild);
            plan = new RandomSample(randomSample.source(), probability, seed, rsChild.child());
        } else if (child instanceof Enrich
            || child instanceof Eval
            || child instanceof Filter
            || child instanceof Insist
            || child instanceof OrderBy
            || child instanceof Project
            || child instanceof RegexExtract) {
                var unaryChild = (UnaryPlan) child;
                plan = unaryChild.replaceChild(randomSample.replaceChild(unaryChild.child()));
            }
        return plan;
    }

    private static Expression combinedProbability(LogicalOptimizerContext context, RandomSample parent, RandomSample child) {
        var parentProbability = (double) Foldables.valueOf(context.foldCtx(), parent.probability());
        var childProbability = (double) Foldables.valueOf(context.foldCtx(), child.probability());
        return Literal.of(parent.probability(), parentProbability * childProbability);
    }

    private static Expression combinedSeed(LogicalOptimizerContext context, RandomSample parent, RandomSample child) {
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
