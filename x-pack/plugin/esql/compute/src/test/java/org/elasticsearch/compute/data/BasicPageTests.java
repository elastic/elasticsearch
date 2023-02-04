/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

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

    public void testEqualityAndHashCodeSmallInput() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(0, new Block[] {}),
            page -> new Page(0, new Block[] {}),
            page -> new Page(1, new Block[1])
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(new int[] {}, 0).asBlock()),
            page -> new Page(new IntArrayVector(new int[] {}, 0).asBlock()),
            page -> new Page(new IntArrayVector(new int[] { 1 }, 1).asBlock())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(new int[] { 1 }, 0).asBlock()),
            page -> new Page(new IntArrayVector(new int[] { 1 }, 0).asBlock()),
            page -> new Page(new IntArrayVector(new int[] { 1 }, 1).asBlock())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock()),
            page -> new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock()),
            page -> new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 9).asBlock())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(IntStream.range(0, 100).toArray(), 100).asBlock()),
            page -> new Page(new IntArrayVector(IntStream.range(0, 100).toArray(), 100).asBlock()),
            page -> new Page(new LongArrayVector(LongStream.range(0, 100).toArray(), 100).asBlock())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(new int[] { 1 }, 1).asBlock()),
            page -> new Page(1, page.getBlock(0)),
            page -> new Page(new IntArrayVector(new int[] { 1 }, 1).asBlock(), new IntArrayVector(new int[] { 1 }, 1).asBlock())
        );
    }

    public void testEqualityAndHashCode() {
        final EqualsHashCodeTestUtils.CopyFunction<Page> copyPageFunction = page -> {
            Block[] blocks = new Block[page.getBlockCount()];
            for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
                blocks[blockIndex] = page.getBlock(blockIndex);
            }
            return new Page(page.getPositionCount(), blocks);
        };

        final EqualsHashCodeTestUtils.MutateFunction<Page> mutatePageFunction = page -> {
            Block[] blocks = new Block[page.getBlockCount()];
            for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
                blocks[blockIndex] = page.getBlock(blockIndex);
            }
            assert page.getPositionCount() > 0;
            return new Page(randomInt(page.getPositionCount() - 1), blocks);
        };

        int positions = randomIntBetween(1, 512);
        int blockCount = randomIntBetween(1, 256);
        Block[] blocks = new Block[blockCount];
        for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
            blocks[blockIndex] = switch (randomInt(6)) {
                case 0 -> new IntArrayVector(randomInts(positions).toArray(), positions).asBlock();
                case 1 -> new LongArrayVector(randomLongs(positions).toArray(), positions).asBlock();
                case 2 -> new DoubleArrayVector(randomDoubles(positions).toArray(), positions).asBlock();
                case 3 -> IntBlock.newConstantBlockWith(randomInt(), positions);
                case 4 -> LongBlock.newConstantBlockWith(randomLong(), positions);
                case 5 -> DoubleBlock.newConstantBlockWith(randomDouble(), positions);
                case 6 -> BytesRefBlock.newConstantBlockWith(new BytesRef(Integer.toHexString(randomInt())), positions);
                default -> throw new AssertionError();
            };
        }
        Page page = new Page(positions, blocks);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(page, copyPageFunction, mutatePageFunction);
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
