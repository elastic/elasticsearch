/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramCircuitBreaker;

import java.util.List;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramBlockEqualityTests extends ComputeTestCase {

    public void testEmptyBlock() {
        List<Block> blocks = List.of(
            blockFactory().newConstantNullBlock(0),
            blockFactory().newExponentialHistogramBlockBuilder(0).build(),
            filterAndRelease(blockFactory().newExponentialHistogramBlockBuilder(0).appendNull().build()),
            filterAndRelease(blockFactory().newExponentialHistogramBlockBuilder(0).append(ExponentialHistogram.empty()).build())
        );
        for (Block a : blocks) {
            for (Block b : blocks) {
                assertThat(a, equalTo(b));
                assertThat(a.hashCode(), equalTo(b.hashCode()));
            }
        }
        Releasables.close(blocks);
    }

    public void testNullValuesEquality() {
        List<Block> blocks = List.of(
            blockFactory().newConstantNullBlock(2),
            blockFactory().newExponentialHistogramBlockBuilder(0).appendNull().appendNull().build()
        );
        for (Block a : blocks) {
            for (Block b : blocks) {
                assertThat(a, equalTo(b));
                assertThat(a.hashCode(), equalTo(b.hashCode()));
            }
        }
        Releasables.close(blocks);
    }

    public void testPopulatedBlockEquality() {
        ExponentialHistogram histo1 = ExponentialHistogram.create(2, ExponentialHistogramCircuitBreaker.noop(), 1, 2, 3, 4);
        ExponentialHistogram histo2 = ExponentialHistogram.empty();

        Block block1 = blockFactory().newExponentialHistogramBlockBuilder(0).append(histo1).append(histo1).append(histo2).build();

        Block block2 = blockFactory().newExponentialHistogramBlockBuilder(0).append(histo1).append(histo2).append(histo2).build();

        Block block1Filtered = block1.filter(1, 2);
        Block block2Filtered = block2.filter(0, 1);

        assertThat(block1, not(equalTo(block2)));
        assertThat(block1, not(equalTo(block1Filtered)));
        assertThat(block1, not(equalTo(block2Filtered)));
        assertThat(block2, not(equalTo(block1)));
        assertThat(block2, not(equalTo(block1Filtered)));
        assertThat(block2, not(equalTo(block2Filtered)));

        assertThat(block1Filtered, equalTo(block2Filtered));
        assertThat(block1Filtered.hashCode(), equalTo(block2Filtered.hashCode()));

        Releasables.close(block1, block2, block1Filtered, block2Filtered);
    }

    private static Block filterAndRelease(Block toFilterAndRelease) {
        Block filtered = toFilterAndRelease.filter();
        toFilterAndRelease.close();
        return filtered;
    }
}
