/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.BreakerTestCase;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.aggregation.BlockHash;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class HashAggregationOperatorTests extends BreakerTestCase {
    @Override
    protected void assertSimple(BigArrays bigArrays) {
        BigArrays breakingBigArrays = bigArrays.withCircuitBreaking();
        HashAggregationOperator.HashAggregationOperatorFactory factory = new HashAggregationOperator.HashAggregationOperatorFactory(
            0,
            List.of(
                new GroupingAggregator.GroupingAggregatorFactory(
                    breakingBigArrays,
                    GroupingAggregatorFunction.avg,
                    AggregatorMode.SINGLE,
                    1
                ),
                new GroupingAggregator.GroupingAggregatorFactory(
                    breakingBigArrays,
                    GroupingAggregatorFunction.max,
                    AggregatorMode.SINGLE,
                    1
                )
            ),
            () -> BlockHash.newLongHash(breakingBigArrays),
            AggregatorMode.SINGLE
        );
        Page page;
        try (Operator agg = factory.get()) {
            long[] groupOn = new long[] { 0, 1, 2, 1, 2, 3 };
            double[] values = new double[] { 1, 2, 3, 4, 5, 6 };
            agg.addInput(new Page(new LongArrayBlock(groupOn, groupOn.length), new DoubleArrayBlock(values, values.length)));
            agg.finish();
            page = agg.getOutput();
        }
        Block keys = page.getBlock(0);
        assertThat(keys.getLong(0), equalTo(0L));
        assertThat(keys.getLong(1), equalTo(1L));
        assertThat(keys.getLong(2), equalTo(2L));
        assertThat(keys.getLong(3), equalTo(3L));

        Block avgs = page.getBlock(1);
        assertThat(avgs.getDouble(0), equalTo(1.0));
        assertThat(avgs.getDouble(1), equalTo(3.0));
        assertThat(avgs.getDouble(2), equalTo(4.0));
        assertThat(avgs.getDouble(3), equalTo(6.0));

        Block maxs = page.getBlock(2);
        assertThat(maxs.getDouble(0), equalTo(1.0));
        assertThat(maxs.getDouble(1), equalTo(4.0));
        assertThat(maxs.getDouble(2), equalTo(5.0));
        assertThat(maxs.getDouble(3), equalTo(6.0));
    }
}
