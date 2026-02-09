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
            blockFactory.newAggregateMetricDoubleBlockBuilder(0).appendNull().build().filter(false),
            partialMetricBuilder.build().filter(false),
            blockFactory.newConstantAggregateMetricDoubleBlock(
                new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(0.0, 0.0, 0.0, 0),
                0
            ).filter(false),
            (ConstantNullBlock) blockFactory.newConstantNullBlock(0)
        );

        assertAllEquals(blocks);
        Releasables.close(blocks);
    }

    public void testBlockEqualityRegularBlocks() {
        // all these blocks should be equivalent
        AggregateMetricDoubleBlockBuilder builder1 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        appendValues(builder1, 1.23, 68392.1, 99999.1, 5);
        appendValues(builder1, 582.1, 10942, 209301.4, 25);
        appendValues(builder1, 8952.564, 30921.23, 18592950.14, 1000);
        AggregateMetricDoubleBlockBuilder builder2 = blockFactory.newAggregateMetricDoubleBlockBuilder(6);
        appendValues(builder2, 1.23, 68392.1, 99999.1, 5);
        appendValues(builder2, 1.23, 68392.1, 99999.1, 10);
        appendValues(builder2, 582.1, 10942, 209301.4, 25);
        appendValues(builder2, 999.1, 10942, 209301.4, 50);
        appendValues(builder2, 10000.564, 30921.23, 18592950.14, 2000);
        appendValues(builder2, 8952.564, 30921.23, 18592950.14, 1000);
        AggregateMetricDoubleBlockBuilder builder3 = blockFactory.newAggregateMetricDoubleBlockBuilder(5);
        builder3.appendNull();
        builder3.appendNull();
        appendValues(builder3, 1.23, 68392.1, 99999.1, 5);
        appendValues(builder3, 582.1, 10942, 209301.4, 25);
        appendValues(builder3, 8952.564, 30921.23, 18592950.14, 1000);
        builder3.appendNull();

        List<AggregateMetricDoubleBlock> blocks = List.of(
            builder1.build(),
            builder2.build().filter(false, 0, 2, 5),
            builder3.build().filter(false, 2, 3, 4)
        );
        assertAllEquals(blocks);
    }

    public void testBlockEqualityConstantLikeBlocks() {
        // all these blocks should be equivalent
        AggregateMetricDoubleBlockBuilder builder1 = blockFactory.newAggregateMetricDoubleBlockBuilder(2);
        appendValues(builder1, 12.3, 987.6, 4821.3, 6);
        appendValues(builder1, 12.3, 987.6, 4821.3, 6);
        AggregateMetricDoubleBlockBuilder builder2 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        appendValues(builder2, 12.3, 987.6, 4821.3, 6);
        appendValues(builder2, 95.2, 10852.2, 20000.5, 5);
        appendValues(builder2, 12.3, 987.6, 4821.3, 6);
        appendValues(builder2, 1.1, 2.2, 3.3, 2);
        DoubleBlock min1 = blockFactory.newDoubleBlockBuilder(4).appendDouble(12.3).appendDouble(12.3).build();
        DoubleBlock max1 = blockFactory.newDoubleBlockBuilder(4).appendDouble(987.6).appendDouble(987.6).build();
        DoubleBlock sum1 = blockFactory.newDoubleBlockBuilder(4).appendDouble(4821.3).appendDouble(4821.3).build();
        IntBlock count1 = blockFactory.newIntBlockBuilder(4).appendInt(6).appendInt(6).build();
        CompositeBlock compositeBlock1 = new CompositeBlock(new Block[] { min1, max1, sum1, count1 });
        DoubleBlock min2 = blockFactory.newDoubleBlockBuilder(4)
            .appendDouble(591.1)
            .appendDouble(11.1)
            .appendDouble(12.3)
            .appendDouble(12.3)
            .build();
        DoubleBlock max2 = blockFactory.newDoubleBlockBuilder(4)
            .appendDouble(198441.1)
            .appendDouble(89235982.1)
            .appendDouble(987.6)
            .appendDouble(987.6)
            .build();
        DoubleBlock sum2 = blockFactory.newDoubleBlockBuilder(4)
            .appendDouble(13498198.2)
            .appendDouble(4901245982.1)
            .appendDouble(4821.3)
            .appendDouble(4821.3)
            .build();
        IntBlock count2 = blockFactory.newIntBlockBuilder(4).appendInt(100).appendInt(200).appendInt(6).appendInt(6).build();
        CompositeBlock compositeBlock2 = new CompositeBlock(new Block[] { min2, max2, sum2, count2 });

        List<AggregateMetricDoubleBlock> moreBlocks = List.of(
            builder1.build(),
            builder2.build().filter(false, 0, 2),
            AggregateMetricDoubleArrayBlock.fromCompositeBlock(compositeBlock1),
            AggregateMetricDoubleArrayBlock.fromCompositeBlock(compositeBlock2).filter(false, 2, 3),
            blockFactory.newConstantAggregateMetricDoubleBlock(
                new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(12.3, 987.6, 4821.3, 6),
                4
            ).filter(false, 1, 3),
            blockFactory.newConstantAggregateMetricDoubleBlock(
                new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(12.3, 987.6, 4821.3, 6),
                2
            )
        );
        assertAllEquals(moreBlocks);
    }

    public void testBlockEqualityPartialSubmetrics() {
        // blocks with partial submetrics appended in different orders (column vs row)
        AggregateMetricDoubleBlockBuilder builder1 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        builder1.min().appendDouble(1.23).appendDouble(582.1).appendNull();
        builder1.max().appendDouble(68392.1).appendNull();
        builder1.max().appendDouble(30921.23);
        builder1.sum().appendNull();
        builder1.sum().appendDouble(99999.1).appendNull();
        builder1.count().appendNull().appendNull();
        builder1.count().appendInt(1000);

        AggregateMetricDoubleBlockBuilder builder2 = blockFactory.newAggregateMetricDoubleBlockBuilder(6);
        builder2.appendNull();
        builder2.appendNull();
        builder2.min().appendDouble(1.23);
        builder2.max().appendDouble(68392.1);
        builder2.sum().appendNull();
        builder2.count().appendNull();
        builder2.min().appendDouble(582.1);
        builder2.max().appendNull();
        builder2.sum().appendDouble(99999.1);
        builder2.count().appendNull();
        builder2.appendNull();
        builder2.min().appendNull();
        builder2.max().appendDouble(30921.23);
        builder2.sum().appendNull();
        builder2.count().appendInt(1000);

        List<AggregateMetricDoubleBlock> evenMoreBlocks = List.of(builder1.build(), builder2.build().filter(false, 2, 3, 5));
        assertAllEquals(evenMoreBlocks);
    }

    public void testBlockInequality() {
        // all these blocks should NOT be equivalent
        AggregateMetricDoubleBlockBuilder builder1 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        appendValues(builder1, 1.1, 6.1, 7.2, 2);
        builder1.appendNull();
        appendValues(builder1, 1.3, 6.3, 11.9, 3);
        // same values as builder1, in a different order
        AggregateMetricDoubleBlockBuilder builder2 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        appendValues(builder2, 1.3, 6.3, 11.9, 3);
        builder2.appendNull();
        appendValues(builder2, 1.1, 6.1, 7.2, 2);
        // first 2/3 values match builder1, last 2/3 values match builder2
        AggregateMetricDoubleBlockBuilder builder3 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        appendValues(builder3, 1.1, 6.1, 7.2, 2);
        builder3.appendNull();
        appendValues(builder3, 1.1, 6.1, 7.2, 2);
        // matches the first 2 values of builder1
        AggregateMetricDoubleBlockBuilder builder4 = blockFactory.newAggregateMetricDoubleBlockBuilder(2);
        appendValues(builder4, 1.1, 6.1, 7.2, 2);
        builder4.appendNull();
        // like builder1 without null value
        AggregateMetricDoubleBlockBuilder builder5 = blockFactory.newAggregateMetricDoubleBlockBuilder(2);
        appendValues(builder5, 1.1, 6.1, 7.2, 2);
        appendValues(builder5, 1.3, 6.3, 11.9, 3);
        // like builder1 but first value's sum is null
        AggregateMetricDoubleBlockBuilder builder6 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        builder6.min().appendDouble(1.1);
        builder6.max().appendDouble(6.1);
        builder6.sum().appendNull();
        builder6.count().appendInt(2);
        builder6.appendNull();
        appendValues(builder6, 1.3, 6.3, 11.9, 3);
        // like builder6 but max and sum are swapped
        AggregateMetricDoubleBlockBuilder builder7 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        builder7.min().appendDouble(1.1);
        builder7.max().appendNull();
        builder7.sum().appendDouble(6.1);
        builder7.count().appendInt(2);
        builder7.appendNull();
        appendValues(builder7, 1.3, 6.3, 11.9, 3);

        List<AggregateMetricDoubleBlock> notEqualBlocks = List.of(
            builder1.build(),
            builder2.build(),
            builder3.build(),
            builder4.build(),
            builder5.build(),
            builder6.build(),
            builder7.build(),
            blockFactory.newConstantAggregateMetricDoubleBlock(
                new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(1.1, 6.1, 7.2, 2),
                1
            ),
            blockFactory.newConstantAggregateMetricDoubleBlock(
                new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(1.1, 6.1, 7.2, 2),
                3
            )
        );
        assertAllNotEquals(notEqualBlocks);
    }

    public void testSimpleBlockWithSingleNull() {
        AggregateMetricDoubleBlockBuilder builder1 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        appendValues(builder1, 1.1, 6.1, 7.2, 2);
        builder1.appendNull();
        appendValues(builder1, 1.3, 6.3, 11.9, 3);
        AggregateMetricDoubleBlockBuilder builder2 = blockFactory.newAggregateMetricDoubleBlockBuilder(3);
        appendValues(builder2, 1.1, 6.1, 7.2, 2);
        builder2.appendNull();
        appendValues(builder2, 1.3, 6.3, 11.9, 3);

        List<AggregateMetricDoubleBlock> blocks = List.of(builder1.build(), builder2.build());

        assertEquals(3, blocks.get(0).getPositionCount());
        assertTrue(blocks.get(0).isNull(1));
        assertAllEquals(blocks);
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

    static void assertAllNotEquals(List<?> objs) {
        for (Object obj1 : objs) {
            for (Object obj2 : objs) {
                if (obj1 == obj2) {
                    continue; // skip self
                }
                assertNotEquals(obj1, obj2);
                assertNotEquals(obj2, obj1);
                assertNotEquals(obj1.hashCode(), obj2.hashCode());
            }
        }
    }

    static void appendValues(AggregateMetricDoubleBlockBuilder builder, double min, double max, double sum, int count) {
        builder.min().appendDouble(min);
        builder.max().appendDouble(max);
        builder.sum().appendDouble(sum);
        builder.count().appendInt(count);
    }
}
