/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.BreakerTestCase;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleArrayBlock;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.Page;

import static org.hamcrest.Matchers.equalTo;

public class GroupingMinAggregatorTests extends BreakerTestCase {
    @Override
    protected void assertSimple(BigArrays bigArrays) {
        Block mins;
        try (GroupingMinAggregator agg = GroupingMinAggregator.create(bigArrays.withCircuitBreaking(), 0)) {
            int[] groups = new int[] { 0, 1, 2, 1, 2, 3 };
            double[] values = new double[] { 1, 2, 3, 4, 5, 6 };
            agg.addRawInput(new IntArrayBlock(groups, groups.length), new Page(new DoubleArrayBlock(values, values.length)));
            mins = agg.evaluateFinal();
        }
        assertThat(mins.getDouble(0), equalTo(1.0));
        assertThat(mins.getDouble(1), equalTo(2.0));
        assertThat(mins.getDouble(2), equalTo(3.0));
        assertThat(mins.getDouble(3), equalTo(6.0));
    }
}
