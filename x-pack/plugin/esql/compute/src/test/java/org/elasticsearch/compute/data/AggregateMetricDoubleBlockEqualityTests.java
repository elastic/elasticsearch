/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;

import java.util.List;

public class AggregateMetricDoubleBlockEqualityTests extends ComputeTestCase {

    static final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    // TODO: Add additional tests

    public void testEmptyBlock() {
        // all these "empty" blocks should be equivalent
        var partialMetricBuilder = blockFactory.newAggregateMetricDoubleBlockBuilder(0);
        for (var subBuilder : List.of(
            partialMetricBuilder.min(),
            partialMetricBuilder.max(),
            partialMetricBuilder.sum(),
            partialMetricBuilder.count()
        )) {
            if (randomBoolean()) {
                subBuilder.appendNull();
            } else {
                if (subBuilder instanceof DoubleBlockBuilder doubleBlockBuilder) {
                    doubleBlockBuilder.appendDouble(0.0);
                } else if (subBuilder instanceof IntBlockBuilder intBlockBuilder) {
                    intBlockBuilder.appendInt(0);
                }
            }
        }

        List<AggregateMetricDoubleBlock> blocks = List.of(
            blockFactory.newAggregateMetricDoubleBlockBuilder(0).build(),
            blockFactory.newAggregateMetricDoubleBlockBuilder(0).appendNull().build().filter(),
            partialMetricBuilder.build().filter(),
            blockFactory.newConstantAggregateMetricDoubleBlock(
                new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(0.0, 0.0, 0.0, 0),
                0
            ).filter(),
            (ConstantNullBlock) blockFactory.newConstantNullBlock(0)
        );

        assertAllEquals(blocks);
        Releasables.close(blocks);
    }

    public void testSimpleBlockWithManyNulls() {
        int positions = randomIntBetween(1, 256);
        boolean grow = randomBoolean();
        AggregateMetricDoubleBlockBuilder builder1 = blockFactory.newAggregateMetricDoubleBlockBuilder(grow ? 0 : positions);
        AggregateMetricDoubleBlockBuilder builder2 = blockFactory.newAggregateMetricDoubleBlockBuilder(grow ? 0 : positions);
        ConstantNullBlock.Builder builder3 = new ConstantNullBlock.Builder(blockFactory);
        for (int p = 0; p < positions; p++) {
            builder1.appendNull();
            builder2.appendNull();
            builder3.appendNull();
        }
        AggregateMetricDoubleBlock block1 = builder1.build();
        AggregateMetricDoubleBlock block2 = builder2.build();
        Block block3 = builder3.build();
        assertEquals(positions, block1.getPositionCount());
        assertTrue(block1.mayHaveNulls());
        assertTrue(block1.isNull(0));

        List<Block> blocks = List.of(block1, block2, block3);
        assertAllEquals(blocks);
    }

    static void assertAllEquals(List<?> objs) {
        for (Object obj1 : objs) {
            for (Object obj2 : objs) {
                assertEquals(obj1, obj2);
                assertEquals(obj2, obj1);
                // equal objects must generate the same hash code
                assertEquals(obj1.hashCode(), obj2.hashCode());
            }
        }
    }
}
