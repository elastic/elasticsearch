/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.xpack.esql.optimizer.GoldenTestCase;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TimeSeriesAggregateExec;
import org.elasticsearch.xpack.esql.querydsl.query.TsidPrefixRangeQueryBuilder;

import java.util.EnumSet;
import java.util.List;

/**
 * The plan of a {@code rate} query split into two {@code _tsid} ranges. Each branch reduces its own range, and the outer
 * aggregation runs once over the fork. The golden indices carry no created version, so the test selects the reduction directly.
 */
public class TimeSeriesSplitPlannerGoldenTests extends GoldenTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        return goldenModes();
    }

    public TimeSeriesSplitPlannerGoldenTests(@Name("mode") String mode) {
        super(mode);
    }

    public void testRateSplitByTsidRange() {
        builder("""
            TS k8s
            | STATS rate = SUM(RATE(network.total_bytes_in)) BY cluster, bucket = TBUCKET(1 hour)
            """).physicalOptimizer(SplittingOptimizer::new)
            .stages(EnumSet.of(Stage.PHYSICAL_OPTIMIZATION, Stage.LOCAL_PHYSICAL_OPTIMIZATION))
            .since(TsidPrefixRangeQueryBuilder.TSID_PREFIX_RANGE_QUERY)
            .run();
    }

    private static class SplittingOptimizer extends PhysicalPlanOptimizer {
        SplittingOptimizer(PhysicalOptimizerContext context) {
            super(context);
        }

        @Override
        public PhysicalPlan optimize(PhysicalPlan plan) {
            PhysicalPlan optimized = super.optimize(plan);
            List<TimeSeriesAggregateExec> reductions = optimized.collect(TimeSeriesAggregateExec.class);
            assertEquals(1, reductions.size());
            TimeSeriesAggregateExec reduction = reductions.getFirst();
            ExchangeExec exchange = asInstanceOf(ExchangeExec.class, reduction.child());
            FragmentExec fragment = asInstanceOf(FragmentExec.class, exchange.child());
            TimeSeriesAggregate aggregate = asInstanceOf(TimeSeriesAggregate.class, fragment.fragment());
            EsRelation relation = aggregate.collect(EsRelation.class).getFirst();
            var target = new TimeSeriesSplitPlanner.Nodes(reduction, exchange, fragment, aggregate, relation);
            List<TsidPrefixRange> splits = List.of(new TsidPrefixRange(8, 0, 128), new TsidPrefixRange(8, 128, 256));
            return TimeSeriesSplitPlanner.split(optimized, target, splits);
        }
    }
}
