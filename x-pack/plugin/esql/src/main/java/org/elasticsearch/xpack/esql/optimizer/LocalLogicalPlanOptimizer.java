/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceStringCasingWithInsensitiveRegexMatch;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.IgnoreNullMetrics;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferIsNotNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.InferNonNullAggConstraint;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.PushExpressionsToFieldLoad;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceDateTruncBucketWithRoundTo;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceFieldWithConstantOrNull;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.ReplaceTopNWithLimitAndSort;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.cleanup;
import static org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer.operators;

/**
 * This class is part of the planner. Data node level logical optimizations.  At this point we have access to
 * {@link org.elasticsearch.xpack.esql.stats.SearchStats} which provides access to metadata about the index.
 *
 * <p>NB: This class also reapplies all the rules from {@link LogicalPlanOptimizer#operators()}
 * and {@link LogicalPlanOptimizer#cleanup()}
 */
public class LocalLogicalPlanOptimizer extends ParameterizedRuleExecutor<LogicalPlan, LocalLogicalOptimizerContext> {

    private final LogicalVerifier verifier = LogicalVerifier.LOCAL_INSTANCE;

    private static final List<Batch<LogicalPlan>> RULES;

    static {

        RULES = arrayAsArrayList(localRewrites(), localOperators(), localCleanup());
    }

    public LocalLogicalPlanOptimizer(LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        super(localLogicalOptimizerContext);
    }

    @Override
    protected List<Batch<LogicalPlan>> batches() {
        return RULES;
    }

    @SuppressWarnings("unchecked")
    private static Batch<LogicalPlan> localRewrites() {
        List<Rule<?, LogicalPlan>> rules = new ArrayList<>(
            List.of(
                new IgnoreNullMetrics(),
                new ReplaceTopNWithLimitAndSort(),
                new ReplaceFieldWithConstantOrNull(),
                new InferIsNotNull(),
                new InferNonNullAggConstraint(),
                new ReplaceDateTruncBucketWithRoundTo()
            )
        );
        if (EsqlCapabilities.Cap.VECTOR_SIMILARITY_FUNCTIONS_PUSHDOWN.isEnabled()) {
            rules.add(new PushExpressionsToFieldLoad());
        }

        return new Batch<>("Local rewrite", Limiter.ONCE, rules.toArray(Rule[]::new));
    }

    @SuppressWarnings("unchecked")
    private static Batch<LogicalPlan> localOperators() {
        return localBatch(operators(), new ReplaceStringCasingWithInsensitiveRegexMatch());
    }

    @SuppressWarnings("unchecked")
    private static Batch<LogicalPlan> localCleanup() {
        return localBatch(cleanup());
    }

    @SuppressWarnings("unchecked")
    private static Batch<LogicalPlan> localBatch(Batch<LogicalPlan> batch, Rule<?, LogicalPlan>... additionalRules) {
        Rule<?, LogicalPlan>[] rules = batch.rules();

        List<Rule<?, LogicalPlan>> newRules = new ArrayList<>(rules.length);
        for (Rule<?, LogicalPlan> r : rules) {
            if (r instanceof OptimizerRules.LocalAware<?> localAware) {
                Rule<?, LogicalPlan> local = localAware.local();
                if (local != null) {
                    newRules.add(local);
                }
            } else {
                newRules.add(r);
            }
        }

        newRules.addAll(Arrays.asList(additionalRules));

        return batch.with(newRules.toArray(Rule[]::new));
    }

    public LogicalPlan localOptimize(LogicalPlan plan) {
        LogicalPlan optimized = execute(plan);
        Failures failures = verifier.verify(optimized, plan.output());
        if (failures.hasFailures()) {
            throw new VerificationException(failures);
        }
        return optimized;
    }

}
