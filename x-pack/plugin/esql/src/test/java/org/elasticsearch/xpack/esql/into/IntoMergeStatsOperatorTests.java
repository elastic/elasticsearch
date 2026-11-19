/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.elasticsearch.xpack.esql.into.IntoTestUtils.stats;
import static org.hamcrest.Matchers.equalTo;

public class IntoMergeStatsOperatorTests extends ComputeTestCase {
    public void testZeroInputPagesEmitsZeroZero() {
        BlockFactory bf = blockFactory();
        IntoMergeStatsOperator op = new IntoMergeStatsOperator(bf);
        op.finish();
        assertTrue(op.canProduceMoreDataWithoutExtraInput());
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(0L, 0L)));
        assertTrue(op.isFinished());
    }

    public void testMultiplePagesAreSummed() {
        BlockFactory bf = blockFactory();
        IntoMergeStatsOperator op = new IntoMergeStatsOperator(bf);
        op.addInput(new IntoStats(3L, 2L).toPage(bf));
        op.addInput(new IntoStats(5L, 4L).toPage(bf));
        op.addInput(new IntoStats(1L, 1L).toPage(bf));
        op.finish();
        assertThat(stats(op.getOutput()), equalTo(new IntoStats(9L, 7L)));
        assertTrue(op.isFinished());
    }
}
