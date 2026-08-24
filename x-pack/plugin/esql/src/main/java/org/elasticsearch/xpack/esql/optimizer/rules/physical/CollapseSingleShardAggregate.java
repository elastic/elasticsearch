/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical;

import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.SampledAggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;

/**
 * Collapses the two-phase INITIAL/FINAL aggregation into a single SINGLE-mode pass when the query
 * touches exactly one shard (as reported by field-caps).
 *
 * <p>When there is only one shard feeding an aggregation, the coordinator FINAL merge buys nothing:
 * the data node already holds the complete dataset. For high-cardinality {@code GROUP BY} queries
 * the extra serialisation and re-hashing on the coordinator is pure overhead. This rule removes it.
 *
 * <p>The rewrite drops the coordinator {@code AggregateExec[FINAL]} and sets
 * {@link FragmentExec#singlePassAgg()} to {@code true} on the inner fragment so that the data
 * node's {@code LocalMapper} can map the {@code Aggregate} logical node to
 * {@link AggregatorMode#SINGLE} instead of {@link AggregatorMode#INITIAL}.
 *
 * <p>Restrictions:
 * <ul>
 *   <li>Only fires when the {@code esql.single_shard_single_pass_aggregation} cluster setting is
 *       enabled.</li>
 *   <li>Only applies to grouped aggregations — ungrouped {@code STATS} queries omit the
 *       coordinator FINAL agg that emits the default row when all shards are skipped by
 *       can-match, which would produce zero rows instead of one.</li>
 *   <li>Time-series ({@link TimeSeriesAggregateExec}) and sampled ({@link SampledAggregateExec})
 *       aggregations are explicitly excluded.</li>
 *   <li>Bail out on FORK ({@link MergeExec} in the subtree) or if more than one shard is
 *       reachable.</li>
 * </ul>
 */
public class CollapseSingleShardAggregate extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    AggregateExec,
    PhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(AggregateExec aggregateExec, PhysicalOptimizerContext context) {
        // Feature gate: setting must be enabled.
        if (context.allowSinglePassAgg() == false) {
            return aggregateExec;
        }

        // Only the coordinator FINAL aggregate is the entry point for this rewrite.
        if (aggregateExec.getMode() != AggregatorMode.FINAL) {
            return aggregateExec;
        }

        // Skip sub-classes: TimeSeriesAggregateExec and SampledAggregateExec are out of scope.
        if (aggregateExec.getClass() != AggregateExec.class) {
            return aggregateExec;
        }

        // Only grouped aggregations — ungrouped STATS relies on the coordinator FINAL to emit
        // the default row when all shards are skipped by can-match.
        if (aggregateExec.groupings().isEmpty()) {
            return aggregateExec;
        }

        // NonEvaluatableGroupingFunction (e.g. CATEGORIZE) cannot run in SINGLE mode.
        if (aggregateExec.groupings()
            .stream()
            .anyMatch(group -> group.anyMatch(expr -> expr instanceof GroupingFunction.NonEvaluatableGroupingFunction))) {
            return aggregateExec;
        }

        // Expected shape: FINAL <- ExchangeExec(inBetweenAggs=true) <- FragmentExec(Aggregate).
        if (!(aggregateExec.child() instanceof ExchangeExec exchangeExec) || exchangeExec.inBetweenAggs() == false) {
            return aggregateExec;
        }
        if (!(exchangeExec.child() instanceof FragmentExec fragmentExec)) {
            return aggregateExec;
        }

        // Bail out if any FORK (MergeExec) exists in this subtree — FORK uses multiple fragments.
        if (aggregateExec.anyMatch(p -> p instanceof MergeExec)) {
            return aggregateExec;
        }

        // Shard-count predicate: every EsRelation in this subtree must report exactly 1 shard
        // in total, with no unknown counts and no remote indices.
        int[] shardSum = { 0 };
        boolean[] unknownCount = { false };
        aggregateExec.forEachDown(FragmentExec.class, f -> f.fragment().forEachDown(EsRelation.class, r -> {
            if (r.indexMode() == IndexMode.LOOKUP) {
                return;
            }
            var props = r.indexProperties();
            if (props.isEmpty()) {
                unknownCount[0] = true;
                return;
            }
            for (var p : props.values()) {
                if (p.numberOfShards() <= 0) {
                    unknownCount[0] = true;
                    return;
                }
                shardSum[0] += p.numberOfShards();
            }
        }));

        if (unknownCount[0] || shardSum[0] != 1) {
            return aggregateExec;
        }

        // Rewrite: drop the coordinator FINAL agg; mark the fragment for single-pass execution.
        FragmentExec newFragment = fragmentExec.withSinglePassAgg(true);
        return new ExchangeExec(exchangeExec.source(), aggregateExec.output(), false, newFragment);
    }
}
