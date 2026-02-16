/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.RuleUtils;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.List;

/**
 * Replace any reference attribute with its source, if it does not affect the result.
 * This avoids ulterior look-ups between attributes and its source across nodes.
 */
public final class PropagateEvalFoldables extends ParameterizedRule<LogicalPlan, LogicalPlan, LogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LogicalOptimizerContext ctx) {
        AttributeMap<Expression> collectRefs = RuleUtils.foldableReferences(plan, ctx);
        if (collectRefs.isEmpty()) {
            return plan;
        }
        // Build the final set of foldable references to propagate.
        // We start with all collected foldable references, then exclude multi-value grouping keys.
        AttributeMap.Builder<Expression> builder = AttributeMap.builder();
        builder.putAll(collectRefs);

        // Exclude multi-value (List) literals used as GROUP BY keys from propagation.
        //
        // Rationale: GROUP BY explodes multi-value fields into single values. For example:
        // ROW a = [1, 2] | STATS x = a + SUM(a) BY a
        // Before aggregation, `a` is multi-valued [1, 2]. After GROUP BY, `a` becomes single-valued
        // (either 1 or 2 per group). If we propagate the original multi-value literal [1, 2] into
        // expressions after the Aggregate (e.g., `x = a + SUM(a)`), this would incorrectly treat `a`
        // as still being multi-valued, leading to wrong results.
        plan = plan.transformUp(p -> {
            if (p instanceof Aggregate aggregate) {
                aggregate.groupings().forEach(group -> {
                    Expression resolved = collectRefs.resolve(group, group);
                    if (resolved instanceof Literal literal && literal.value() instanceof List<?>) {
                        builder.remove(group);
                    }
                });
            }
            // Apply the replacement inside Filter and Eval (which shouldn't make a difference)
            // TODO: also allow aggregates once aggs on constants are supported.
            // C.f. https://github.com/elastic/elasticsearch/issues/100634
            if (p instanceof Filter || p instanceof Eval) {
                p = p.transformExpressionsOnly(ReferenceAttribute.class, r -> builder.build().resolve(r, r));
            }
            return p;
        });

        return plan;
    }
}
