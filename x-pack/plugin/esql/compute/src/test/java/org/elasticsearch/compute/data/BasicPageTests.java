/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.is;

public class BasicPageTests extends SerializationTestCase {

    static final Class<NullPointerException> NPE = NullPointerException.class;
    static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;
    static final Class<AssertionError> AE = AssertionError.class;

    public void testExceptions() {
        expectThrows(NPE, () -> new Page((BlockRef[]) null));

        expectThrows(IAE, () -> new Page(new BlockRef[] {}));

        // Temporarily disable, until the intermediate state of grouping aggs is resolved.
        // Intermediate state consists of a Page with two blocks: one of size N with the groups, the
        // other has a single entry containing the serialized binary state.
        // expectThrows(AE, () -> new Page(new Block[] { new IntArrayBlock(new int[] { 1, 2 }, 2), new ConstantIntBlock(1, 1) }));
    }

    public void testEqualityAndHashCodeSmallInput() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(0, new BlockRef[] {}),
            page -> new Page(0, new BlockRef[] {}),
            page -> new Page(1, IntBlock.newConstantBlockWith(1, 1).asRef())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(new int[] {}, 0).asBlock().asRef()),
            page -> new Page(new IntArrayVector(new int[] {}, 0).asBlock().asRef()),
            page -> new Page(new IntArrayVector(new int[] { 1 }, 1).asBlock().asRef())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(new int[] { 1 }, 0).asBlock().asRef()),
            page -> new Page(new IntArrayVector(new int[] { 1 }, 0).asBlock().asRef()),
            page -> new Page(new IntArrayVector(new int[] { 1 }, 1).asBlock().asRef())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(new int[] { 1, 1, 1 }, 3).asBlock().asRef()),
            page -> new Page(IntBlock.newConstantBlockWith(1, 3).asRef()),
            page -> new Page(IntBlock.newConstantBlockWith(1, 2).asRef())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock().asRef()),
            page -> new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock().asRef()),
            page -> new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 9).asBlock().asRef())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(IntStream.range(0, 100).toArray(), 100).asBlock().asRef()),
            page -> new Page(new IntArrayVector(IntStream.range(0, 100).toArray(), 100).asBlock().asRef()),
            page -> new Page(new LongArrayVector(LongStream.range(0, 100).toArray(), 100).asBlock().asRef())
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            new Page(new IntArrayVector(new int[] { 1 }, 1).asBlock().asRef()),
            page -> new Page(1, page.getBlockRef(0).shallowCopy()),
            page -> new Page(
                new IntArrayVector(new int[] { 1 }, 1).asBlock().asRef(),
                new IntArrayVector(new int[] { 1 }, 1).asBlock().asRef()
            )
        );
    }

    public void testEqualityAndHashCode() throws IOException {
        final EqualsHashCodeTestUtils.CopyFunction<Page> copyPageFunction = page -> {
            BlockRef[] blocks = new BlockRef[page.getBlockCount()];
            for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
                blocks[blockIndex] = page.getBlockRef(blockIndex).shallowCopy();
            }
            return new Page(page.getPositionCount(), blocks);
        };

        final EqualsHashCodeTestUtils.MutateFunction<Page> mutatePageFunction = page -> {
            assert page.getPositionCount() > 0;
            BlockRef[] blocks = new BlockRef[page.getBlockCount()];
            int positions = randomInt(page.getPositionCount() - 1);
            for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
                Block block = page.getBlockRef(blockIndex).get();
                blocks[blockIndex] = block.elementType()
                    .newBlockBuilder(positions)
                    .copyFrom(block, 0, page.getPositionCount() - 1)
                    .build()
                    .asRef();
            }
            return new Page(blocks);
        };

        int positions = randomIntBetween(1, 512);
        int blockCount = randomIntBetween(1, 256);
        BlockRef[] blocks = new BlockRef[blockCount];
        for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
            blocks[blockIndex] = switch (randomInt(6)) {
                case 0 -> new IntArrayVector(randomInts(positions).toArray(), positions).asBlock().asRef();
                case 1 -> new LongArrayVector(randomLongs(positions).toArray(), positions).asBlock().asRef();
                case 2 -> new DoubleArrayVector(randomDoubles(positions).toArray(), positions).asBlock().asRef();
                case 3 -> IntBlock.newConstantBlockWith(randomInt(), positions).asRef();
                case 4 -> LongBlock.newConstantBlockWith(randomLong(), positions).asRef();
                case 5 -> DoubleBlock.newConstantBlockWith(randomDouble(), positions).asRef();
                case 6 -> BytesRefBlock.newConstantBlockWith(new BytesRef(Integer.toHexString(randomInt())), positions).asRef();
                default -> throw new AssertionError();
            };
        }
        Page page = new Page(positions, blocks);
        try {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(page, copyPageFunction, mutatePageFunction);

            EqualsHashCodeTestUtils.checkEqualsAndHashCode(page, this::serializeDeserializePage, null, Page::releaseBlocks);
        } finally {
            page.releaseBlocks();
        }
    }

    public void testBasic() {
        int positions = randomInt(1024);
        Page page = new Page(new IntArrayVector(IntStream.range(0, positions).toArray(), positions).asBlock().asRef());
        assertThat(1, is(page.getBlockCount()));
        assertThat(positions, is(page.getPositionCount()));
        IntBlock block = page.getBlockRef(0).get();
        IntStream.range(0, positions).forEach(i -> assertThat(i, is(block.getInt(i))));
    }

    public void testAppend() {
        Page page1 = new Page(new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock().asRef());
        Page page2 = page1.appendBlock(new LongArrayVector(LongStream.range(0, 10).toArray(), 10).asBlock().asRef());
        assertThat(1, is(page1.getBlockCount()));
        assertThat(2, is(page2.getBlockCount()));
        IntBlock block1 = page2.getBlockRef(0).get();
        IntStream.range(0, 10).forEach(i -> assertThat(i, is(block1.getInt(i))));
        LongBlock block2 = page2.getBlockRef(1).get();
        IntStream.range(0, 10).forEach(i -> assertThat((long) i, is(block2.getLong(i))));
    }

    public void testPageSerializationSimple() throws IOException {
        Page origPage = new Page(
            new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock().asRef(),
            new LongArrayVector(LongStream.range(10, 20).toArray(), 10).asBlock().asRef(),
            new DoubleArrayVector(LongStream.range(30, 40).mapToDouble(i -> i).toArray(), 10).asBlock().asRef(),
            new BytesRefArrayVector(bytesRefArrayOf("0a", "1b", "2c", "3d", "4e", "5f", "6g", "7h", "8i", "9j"), 10).asBlock().asRef(),
            IntBlock.newConstantBlockWith(randomInt(), 10).asRef(),
            LongBlock.newConstantBlockWith(randomInt(), 10).asRef(),
            DoubleBlock.newConstantBlockWith(randomInt(), 10).asRef(),
            BytesRefBlock.newConstantBlockWith(new BytesRef(Integer.toHexString(randomInt())), 10).asRef(),
            new IntArrayVector(IntStream.range(0, 20).toArray(), 20).filter(5, 6, 7, 8, 9, 10, 11, 12, 13, 14).asBlock().asRef()
        );
        try {
            Page deserPage = serializeDeserializePage(origPage);
            try {
                EqualsHashCodeTestUtils.checkEqualsAndHashCode(origPage, unused -> deserPage);

                for (int i = 0; i < origPage.getBlockCount(); i++) {
                    Vector vector = origPage.getBlockRef(i).get().asVector();
                    if (vector != null) {
                        assertEquals(vector.isConstant(), deserPage.getBlockRef(i).get().asVector().isConstant());
                    }
                }
            } finally {
                deserPage.releaseBlocks();
            }
        } finally {
            origPage.releaseBlocks();
        }
    }

    public void testSerializationListPages() throws IOException {
        final int positions = randomIntBetween(1, 64);
        List<Page> origPages = List.of(
            new Page(new IntArrayVector(randomInts(positions).toArray(), positions).asBlock().asRef()),
            new Page(
                new LongArrayVector(randomLongs(positions).toArray(), positions).asBlock().asRef(),
                DoubleBlock.newConstantBlockWith(randomInt(), positions).asRef()
            ),
            new Page(BytesRefBlock.newConstantBlockWith(new BytesRef("Hello World"), positions).asRef())
        );
        try {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(origPages, page -> {
                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    out.writeCollection(origPages);
                    return blockStreamInput(out).readCollectionAsList(Page::new);
                }
            }, null, pages -> Releasables.close(() -> Iterators.map(pages.iterator(), p -> p::releaseBlocks)));
        } finally {
            Releasables.close(() -> Iterators.map(origPages.iterator(), p -> p::releaseBlocks));
        }
    }

    BytesRefArray bytesRefArrayOf(String... values) {
        var array = new BytesRefArray(values.length, bigArrays);
        Arrays.stream(values).map(BytesRef::new).forEach(array::append);
        return array;
    }
}
