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
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

/**
 * Pushes down the SAMPLE operator. SAMPLE can be pushed down through an
 * operator if
 * <p>
 * <code>| SAMPLE p | OPERATOR</code>
 * <p>
 * is equivalent to
 * <p>
 * <code>| OPERATOR | SAMPLE p</code>
 * <p>
 * statistically (i.e. same possible output with same probabilities).
 * In that case, we push down sampling to Lucene for efficiency.
 *  <p>
 *
 *  As a rule of thumb, if an operator can be swapped with sampling if it maps:
 *  <ul>
 *      <li>
 *          one row to one row (e.g. <code>DISSECT</code>, <code>DROP</code>, <code>ENRICH</code>,
 *          <code>EVAL</code>, <code>GROK</code>, <code>KEEP</code>, <code>RENAME</code>)
 *      </li>
 *      <li>
 *          one row to zero or one row (<code>WHERE</code>)
 *      </li>
 *      <li>
 *          reorders the rows (<code>SORT</code>)
 *      </li>
 *  </ul>
 */
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
            plan = new Sample(sample.source(), probability, sampleChild.child());
        } else if (child instanceof Enrich
            || child instanceof Eval
            || child instanceof Filter
            || child instanceof Insist
            || child instanceof OrderBy
            || child instanceof Project
            || child instanceof RegexExtract) {
                var unaryChild = (UnaryPlan) child;
                plan = unaryChild.replaceChild(sample.replaceChild(unaryChild.child()));
            }
        return plan;
    }

    private static Expression combinedProbability(LogicalOptimizerContext context, Sample parent, Sample child) {
        var parentProbability = (double) Foldables.valueOf(context.foldCtx(), parent.probability());
        var childProbability = (double) Foldables.valueOf(context.foldCtx(), child.probability());
        return Literal.of(parent.probability(), parentProbability * childProbability);
    }
}
