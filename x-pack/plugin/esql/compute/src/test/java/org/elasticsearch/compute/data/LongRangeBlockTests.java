/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class LongRangeBlockTests extends ESTestCase {

    public void testGetLongRangeMutatesScratchAcrossValueIndices() {
        BlockFactory blockFactory = BlockFactoryTests.blockFactory(ByteSizeValue.ofMb(16));
        try (LongRangeBlockBuilder builder = blockFactory.newLongRangeBlockBuilder(3)) {
            // Position 0: single-valued [10, 20)
            builder.appendLongRange(10L, 20L);
            // Position 1: multi-valued [30, 40), [50, 60), [70, 80)
            builder.from().beginPositionEntry();
            builder.from().appendLong(30L);
            builder.from().appendLong(50L);
            builder.from().appendLong(70L);
            builder.from().endPositionEntry();
            builder.to().beginPositionEntry();
            builder.to().appendLong(40L);
            builder.to().appendLong(60L);
            builder.to().appendLong(80L);
            builder.to().endPositionEntry();
            // Position 2: single-valued [100, 200)
            builder.appendLongRange(100L, 200L);

            try (LongRangeBlock block = builder.build()) {
                LongRangeBlockBuilder.LongRange scratch = new LongRangeBlockBuilder.LongRange();

                LongRangeBlockBuilder.LongRange got = block.getLongRange(block.getFirstValueIndex(0), scratch);
                assertThat("accessor must reuse the supplied scratch", got, sameInstance(scratch));
                assertThat(got.from(), equalTo(10L));
                assertThat(got.to(), equalTo(20L));

                // Multi-valued position: read each value-index in turn and check the scratch is overwritten.
                int firstMv = block.getFirstValueIndex(1);
                got = block.getLongRange(firstMv, scratch);
                assertThat(got.from(), equalTo(30L));
                assertThat(got.to(), equalTo(40L));
                got = block.getLongRange(firstMv + 1, scratch);
                assertThat(got.from(), equalTo(50L));
                assertThat(got.to(), equalTo(60L));
                got = block.getLongRange(firstMv + 2, scratch);
                assertThat(got.from(), equalTo(70L));
                assertThat(got.to(), equalTo(80L));

                got = block.getLongRange(block.getFirstValueIndex(2), scratch);
                assertThat(got.from(), equalTo(100L));
                assertThat(got.to(), equalTo(200L));
            }
        }
    }

    public void testLongRangeValueSemantics() {
        var a = new LongRangeBlockBuilder.LongRange(1L, 2L);
        var b = new LongRangeBlockBuilder.LongRange(1L, 2L);
        var c = new LongRangeBlockBuilder.LongRange(1L, 3L);

        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), equalTo(b.hashCode()));
        assertThat(a, not(equalTo(c)));
        assertThat(a.toString(), equalTo("LongRange[from=1, to=2]"));

        var ret = a.reset(7L, 9L);
        assertThat(ret, sameInstance(a));
        assertThat(a.from(), equalTo(7L));
        assertThat(a.to(), equalTo(9L));
        assertThat(a, not(equalTo(b)));
    }
}
