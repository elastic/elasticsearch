/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.test.ESTestCase;

import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.is;

public class BasicPageTests extends ESTestCase {

    static final Class<NullPointerException> NPE = NullPointerException.class;
    static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;
    static final Class<AssertionError> AE = AssertionError.class;

    public void testExceptions() {
        expectThrows(NPE, () -> new Page((Block[]) null));

        expectThrows(IAE, () -> new Page());
        expectThrows(IAE, () -> new Page(new Block[] {}));

        // Temporarily disable, until the intermediate state of grouping aggs is resolved.
        // Intermediate state consists of a Page with two blocks: one of size N with the groups, the
        // other has a single entry containing the serialized binary state.
        // expectThrows(AE, () -> new Page(new Block[] { new IntArrayBlock(new int[] { 1, 2 }, 2), new ConstantIntBlock(1, 1) }));
    }

    public void testBasic() {
        int positions = randomInt(1024);
        Page page = new Page(new IntArrayVector(IntStream.range(0, positions).toArray(), positions).asBlock());
        assertThat(1, is(page.getBlockCount()));
        assertThat(positions, is(page.getPositionCount()));
        IntBlock block = page.getBlock(0);
        IntStream.range(0, positions).forEach(i -> assertThat(i, is(block.getInt(i))));
    }

    public void testAppend() {
        Page page1 = new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock());
        Page page2 = page1.appendBlock(new LongArrayVector(LongStream.range(0, 10).toArray(), 10).asBlock());
        assertThat(1, is(page1.getBlockCount()));
        assertThat(2, is(page2.getBlockCount()));
        IntBlock block1 = page2.getBlock(0);
        IntStream.range(0, 10).forEach(i -> assertThat(i, is(block1.getInt(i))));
        LongBlock block2 = page2.getBlock(1);
        IntStream.range(0, 10).forEach(i -> assertThat((long) i, is(block2.getLong(i))));
    }

    public void testReplace() {
        Page page1 = new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock());
        Page page2 = page1.replaceBlock(0, new LongArrayVector(LongStream.range(0, 10).toArray(), 10).asBlock());
        assertThat(1, is(page1.getBlockCount()));
        assertThat(1, is(page2.getBlockCount()));
        LongBlock block = page2.getBlock(0);
        IntStream.range(0, 10).forEach(i -> assertThat((long) i, is(block.getLong(i))));
    }

}
