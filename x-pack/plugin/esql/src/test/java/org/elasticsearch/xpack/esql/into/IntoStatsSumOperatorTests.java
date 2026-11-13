/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.equalTo;

public class IntoStatsSumOperatorTests extends ComputeTestCase {

    private IntoStatsSumOperator makeOperator() {
        BlockFactory bf = blockFactory();
        return new IntoStatsSumOperator(bf);
    }

    public void testZeroInputPagesEmitsZeroZero() {
        IntoStatsSumOperator op = makeOperator();
        op.finish();
        assertTrue(op.canProduceMoreDataWithoutExtraInput());
        Page out = op.getOutput();
        assertNotNull(out);
        assertThat(out.getPositionCount(), equalTo(1));
        assertThat(((LongBlock) out.getBlock(0)).getLong(0), equalTo(0L));
        assertThat(((LongBlock) out.getBlock(1)).getLong(0), equalTo(0L));
        assertTrue(op.isFinished());
        out.releaseBlocks();
    }

    public void testMultiplePagesAreSummed() {
        BlockFactory bf = blockFactory();
        IntoStatsSumOperator op = new IntoStatsSumOperator(bf);

        op.addInput(statsPage(bf, 3L, 2L));
        op.addInput(statsPage(bf, 5L, 4L));
        op.addInput(statsPage(bf, 1L, 1L));
        op.finish();

        Page out = op.getOutput();
        assertNotNull(out);
        assertThat(((LongBlock) out.getBlock(0)).getLong(0), equalTo(9L));
        assertThat(((LongBlock) out.getBlock(1)).getLong(0), equalTo(7L));
        assertTrue(op.isFinished());
        out.releaseBlocks();
    }

    private static Page statsPage(BlockFactory bf, long total, long created) {
        return new IntoStats(total, created).toPage(bf);
    }
}
