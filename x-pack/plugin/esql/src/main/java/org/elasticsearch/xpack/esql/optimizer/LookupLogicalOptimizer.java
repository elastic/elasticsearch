/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneFilters;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStringCasingWithInsensitiveRegexMatch;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferIsNotNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.LookupPruneFilters;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceFieldWithConstantOrNull;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.operators;

/**
 * Logical plan optimizer for the lookup node. Extends {@link LocalLogicalPlanOptimizer} with a
 * reduced rule set appropriate for lookup plans (rooted at
 * {@link org.elasticsearch.xpack.esql.plan.logical.ParameterizedQuery}, not
 * {@link org.elasticsearch.xpack.esql.plan.logical.EsRelation}).
 *
 * <p>The lookup logical plan is narrow: {@code Project -> optional Filter -> ParameterizedQuery}.
 * This optimizer runs {@link ReplaceFieldWithConstantOrNull} to replace missing/constant fields,
 * then the standard operator-optimization rules to fold nulls, simplify booleans, and prune filters.</p>
 */
public class LookupLogicalOptimizer extends LocalLogicalPlanOptimizer {

    private static final List<Batch<LogicalPlan>> RULES = List.of(
        new Batch<>("Lookup local rewrite", Limiter.ONCE, new ReplaceFieldWithConstantOrNull(), new InferIsNotNull()),
        lookupOperators()
    );

    public LookupLogicalOptimizer(LocalLogicalOptimizerContext context) {
        super(context);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        return RULES;
    }

    @SuppressWarnings("unchecked")
    private static Batch<LogicalPlan> lookupOperators() {
        Batch<LogicalPlan> batch = localBatch(operators(), new ReplaceStringCasingWithInsensitiveRegexMatch());
        List<Rule<?, LogicalPlan>> rules = new ArrayList<>(batch.rules().length);
        for (Rule<?, LogicalPlan> r : batch.rules()) {
            rules.add(r instanceof PruneFilters ? new LookupPruneFilters() : r);
        }
        return batch.with(rules.toArray(Rule[]::new));
    }
}
