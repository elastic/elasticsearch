/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class BlockBuilderTests extends ESTestCase {

    public void testAllNullsInt() {
        for (int numEntries : List.of(1, randomIntBetween(1, 100))) {
            testAllNullsImpl(IntBlock.newBlockBuilder(0), numEntries);
            testAllNullsImpl(IntBlock.newBlockBuilder(100), numEntries);
            testAllNullsImpl(IntBlock.newBlockBuilder(1000), numEntries);
            testAllNullsImpl(IntBlock.newBlockBuilder(randomIntBetween(0, 100)), numEntries);
        }
    }

    public void testAllNullsLong() {
        for (int numEntries : List.of(1, randomIntBetween(1, 100))) {
            testAllNullsImpl(LongBlock.newBlockBuilder(0), numEntries);
            testAllNullsImpl(LongBlock.newBlockBuilder(100), numEntries);
            testAllNullsImpl(LongBlock.newBlockBuilder(1000), numEntries);
            testAllNullsImpl(LongBlock.newBlockBuilder(randomIntBetween(0, 100)), numEntries);
        }
    }

    public void testAllNullsDouble() {
        for (int numEntries : List.of(1, randomIntBetween(1, 100))) {
            testAllNullsImpl(DoubleBlock.newBlockBuilder(0), numEntries);
            testAllNullsImpl(DoubleBlock.newBlockBuilder(100), numEntries);
            testAllNullsImpl(DoubleBlock.newBlockBuilder(1000), numEntries);
            testAllNullsImpl(DoubleBlock.newBlockBuilder(randomIntBetween(0, 100)), numEntries);
        }
    }

    public void testAllNullsBytesRef() {
        for (int numEntries : List.of(1, randomIntBetween(1, 100))) {
            testAllNullsImpl(BytesRefBlock.newBlockBuilder(0), numEntries);
            testAllNullsImpl(BytesRefBlock.newBlockBuilder(100), numEntries);
            testAllNullsImpl(BytesRefBlock.newBlockBuilder(1000), numEntries);
            testAllNullsImpl(BytesRefBlock.newBlockBuilder(randomIntBetween(0, 100)), numEntries);
        }
    }

    private void testAllNullsImpl(Block.Builder builder, int numEntries) {
        for (int i = 0; i < numEntries; i++) {
            builder.appendNull();
        }
        Block block = builder.build();
        assertThat(block.getPositionCount(), is(numEntries));
        assertThat(block.isNull(0), is(true));
        assertThat(block.isNull(numEntries - 1), is(true));
        assertThat(block.isNull(randomPosition(numEntries)), is(true));
    }

    static int randomPosition(int positionCount) {
        return positionCount == 1 ? 0 : randomIntBetween(0, positionCount - 1);
    }
}
