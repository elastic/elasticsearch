/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.operator.ComputeTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BlockBuilderAppendBlockTests extends ComputeTestCase {

    public void testBasic() {
        BlockFactory blockFactory = blockFactory();
        IntBlock src = blockFactory.newIntBlockBuilder(10)
            .appendInt(1)
            .appendNull()
            .beginPositionEntry()
            .appendInt(4)
            .appendInt(6)
            .endPositionEntry()
            .appendInt(10)
            .appendInt(20)
            .appendInt(30)
            .appendNull()
            .beginPositionEntry()
            .appendInt(1)
            .endPositionEntry()
            .build();
        // copy position by position
        try (IntBlock.Builder dst = blockFactory.newIntBlockBuilder(randomIntBetween(1, 20))) {
            for (int i = 0; i < src.getPositionCount(); i++) {
                try (IntBlock filter = src.filter(i)) {
                    dst.appendAllValuesToCurrentPosition(filter);
                }
            }
            try (IntBlock block = dst.build()) {
                assertThat(block, equalTo(src));
            }
        }
        // copy all block
        try (IntBlock.Builder dst = blockFactory.newIntBlockBuilder(randomIntBetween(1, 20))) {
            try (IntBlock block = dst.appendAllValuesToCurrentPosition(src).build()) {
                assertThat(block.getPositionCount(), equalTo(1));
                assertThat(BlockUtils.toJavaObject(block, 0), equalTo(List.of(1, 4, 6, 10, 20, 30, 1)));
            }
        }
        try (Block dst = randomlyDivideAndMerge(src)) {
            assertThat(dst.getPositionCount(), equalTo(1));
            assertThat(BlockUtils.toJavaObject(dst, 0), equalTo(List.of(1, 4, 6, 10, 20, 30, 1)));
        }
    }

    public void testRandomNullBlock() {
        BlockFactory blockFactory = blockFactory();
        IntBlock.Builder src = blockFactory.newIntBlockBuilder(10);
        try (var nullBlock = blockFactory.newConstantNullBlock(between(1, 100))) {
            src.appendAllValuesToCurrentPosition(nullBlock);
        }
        src.appendInt(101);
        try (var nullBlock = blockFactory.newConstantNullBlock(between(1, 100))) {
            src.appendAllValuesToCurrentPosition(nullBlock);
        }
        IntBlock block = src.build();
        assertThat(block.getPositionCount(), equalTo(3));
        assertTrue(block.isNull(0));
        assertThat(block.getInt(1), equalTo(101));
        assertTrue(block.isNull(2));
        try (Block flatten = randomlyDivideAndMerge(block)) {
            assertThat(flatten.getPositionCount(), equalTo(1));
            assertThat(BlockUtils.toJavaObject(flatten, 0), equalTo(101));
        }
    }

    public void testRandom() {
        ElementType elementType = randomFrom(ElementType.INT, ElementType.BYTES_REF, ElementType.BOOLEAN);
        Block block = BasicBlockTests.randomBlock(
            elementType,
            randomIntBetween(1, 1024),
            randomBoolean(),
            0,
            between(1, 16),
            0,
            between(0, 16)
        ).block();

        block = randomlyDivideAndMerge(block);
        block.close();
    }

    private Block randomlyDivideAndMerge(Block block) {
        while (block.getPositionCount() > 1 || randomBoolean()) {
            int positionCount = block.getPositionCount();
            int offset = 0;
            Block.Builder builder = block.elementType()
                .newBlockBuilder(randomIntBetween(1, 100), TestBlockFactory.getNonBreakingInstance());
            List<Object> expected = new ArrayList<>();
            while (offset < positionCount) {
                int length = randomIntBetween(1, positionCount - offset);
                int[] positions = new int[length];
                for (int i = 0; i < length; i++) {
                    positions[i] = offset + i;
                }
                offset += length;
                Block sub = block.filter(positions);
                expected.add(extractAndFlattenBlockValues(sub));
                builder.appendAllValuesToCurrentPosition(sub);
                sub.close();
            }
            block.close();
            block = builder.build();
            assertThat(block.getPositionCount(), equalTo(expected.size()));
            for (int i = 0; i < block.getPositionCount(); i++) {
                assertThat(BlockUtils.toJavaObject(block, i), equalTo(expected.get(i)));
            }
        }
        return block;
    }

    static Object extractAndFlattenBlockValues(Block block) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < block.getPositionCount(); i++) {
            Object v = BlockUtils.toJavaObject(block, i);
            if (v == null) {
                continue;
            }
            if (v instanceof List<?> l) {
                values.addAll(l);
            } else {
                values.add(v);
            }
        }
        if (values.isEmpty()) {
            return null;
        } else if (values.size() == 1) {
            return values.get(0);
        } else {
            return values;
        }
    }
}
