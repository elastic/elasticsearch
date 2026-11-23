/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.hamcrest.Matchers.lessThan;

public class BlockHashRowInTableLookupTests extends ComputeTestCase {

    public void testSmall() {
        BlockFactory blockFactory = blockFactory();
        try (Block keys = blockFactory.newIntArrayVector(new int[] { 3, 1, 2 }, 3).asBlock()) {
            long before = blockFactory.breaker().getUsed();
            try (BlockHashRowInTableLookup lookup = new BlockHashRowInTableLookup(blockFactory, new Block[] { keys })) {
                assertThat(blockFactory.breaker().getUsed() - before, lessThan(ByteSizeValue.ofKb(100).getBytes()));
            }
        }
    }
}
