/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class BlockBuilderAppendBlockTests extends ESTestCase {

    public void testBasic() {
        IntBlock src = new IntBlockBuilder(10).appendInt(1)
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
        {
            IntBlock.Builder dst = IntBlock.newBlockBuilder(randomIntBetween(1, 20));
            for (int i = 0; i < src.getPositionCount(); i++) {
                dst.appendAllValuesToCurrentPosition(src.filter(i));
            }
            assertThat(dst.build(), equalTo(src));
        }
        // copy all block
        {
            IntBlock.Builder dst = IntBlock.newBlockBuilder(randomIntBetween(1, 20));
            IntBlock block = dst.appendAllValuesToCurrentPosition(src).build();
            assertThat(block.getPositionCount(), equalTo(1));
            assertThat(BlockUtils.toJavaObject(block, 0), equalTo(List.of(1, 4, 6, 10, 20, 30, 1)));
        }
        {
            Block dst = randomlyDivideAndMerge(src);
            assertThat(dst.getPositionCount(), equalTo(1));
            assertThat(BlockUtils.toJavaObject(dst, 0), equalTo(List.of(1, 4, 6, 10, 20, 30, 1)));
        }
    }

    public void testRandomNullBlock() {
        IntBlock.Builder src = IntBlock.newBlockBuilder(10);
        src.appendAllValuesToCurrentPosition(new ConstantNullBlock(between(1, 100)));
        src.appendInt(101);
        src.appendAllValuesToCurrentPosition(new ConstantNullBlock(between(1, 100)));
        IntBlock block = src.build();
        assertThat(block.getPositionCount(), equalTo(3));
        assertTrue(block.isNull(0));
        assertThat(block.getInt(1), equalTo(101));
        assertTrue(block.isNull(2));
        Block flatten = randomlyDivideAndMerge(block);
        assertThat(flatten.getPositionCount(), equalTo(1));
        assertThat(BlockUtils.toJavaObject(flatten, 0), equalTo(101));
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
        randomlyDivideAndMerge(block);
    }

    private Block randomlyDivideAndMerge(Block block) {
        while (block.getPositionCount() > 1 || randomBoolean()) {
            int positionCount = block.getPositionCount();
            int offset = 0;
            Block.Builder builder = block.elementType().newBlockBuilder(randomIntBetween(1, 100));
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
            }
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
