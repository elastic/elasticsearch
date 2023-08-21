/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
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
            page -> new Page(1, IntBlock.newConstantBlockWith(1, 1))
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
            new Page(new IntArrayVector(new int[] { 1, 1, 1 }, 3).asBlock()),
            page -> new Page(IntBlock.newConstantBlockWith(1, 3)),
            page -> new Page(IntBlock.newConstantBlockWith(1, 2))
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

    public void testEqualityAndHashCode() throws IOException {
        final EqualsHashCodeTestUtils.CopyFunction<Page> copyPageFunction = page -> {
            Block[] blocks = new Block[page.getBlockCount()];
            for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
                blocks[blockIndex] = page.getBlock(blockIndex);
            }
            return new Page(page.getPositionCount(), blocks);
        };

        final EqualsHashCodeTestUtils.MutateFunction<Page> mutatePageFunction = page -> {
            assert page.getPositionCount() > 0;
            Block[] blocks = new Block[page.getBlockCount()];
            int positions = randomInt(page.getPositionCount() - 1);
            for (int blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
                Block block = page.getBlock(blockIndex);
                blocks[blockIndex] = block.elementType().newBlockBuilder(positions).copyFrom(block, 0, page.getPositionCount() - 1).build();
            }
            return new Page(blocks);
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

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(page, unused -> serializeDeserializePage(page));
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

    public void testPageSerializationSimple() throws IOException {
        try (var bytesRefArray = bytesRefArrayOf("0a", "1b", "2c", "3d", "4e", "5f", "6g", "7h", "8i", "9j")) {
            final BytesStreamOutput out = new BytesStreamOutput();
            Page origPage = new Page(
                new IntArrayVector(IntStream.range(0, 10).toArray(), 10).asBlock(),
                new LongArrayVector(LongStream.range(10, 20).toArray(), 10).asBlock(),
                new DoubleArrayVector(LongStream.range(30, 40).mapToDouble(i -> i).toArray(), 10).asBlock(),
                new BytesRefArrayVector(bytesRefArray, 10).asBlock(),
                IntBlock.newConstantBlockWith(randomInt(), 10),
                LongBlock.newConstantBlockWith(randomInt(), 10),
                DoubleBlock.newConstantBlockWith(randomInt(), 10),
                BytesRefBlock.newConstantBlockWith(new BytesRef(Integer.toHexString(randomInt())), 10),
                new IntArrayVector(IntStream.range(0, 20).toArray(), 20).filter(5, 6, 7, 8, 9, 10, 11, 12, 13, 14).asBlock()
            );
            Page deserPage = serializeDeserializePage(origPage);
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(origPage, unused -> deserPage);

            for (int i = 0; i < origPage.getBlockCount(); i++) {
                Vector vector = origPage.getBlock(i).asVector();
                if (vector != null) {
                    assertEquals(vector.isConstant(), deserPage.getBlock(i).asVector().isConstant());
                }
            }
        }
    }

    public void testSerializationListPages() throws IOException {
        final int positions = randomIntBetween(1, 64);
        List<Page> origPages = List.of(
            new Page(new IntArrayVector(randomInts(positions).toArray(), positions).asBlock()),
            new Page(
                new LongArrayVector(randomLongs(positions).toArray(), positions).asBlock(),
                DoubleBlock.newConstantBlockWith(randomInt(), positions)
            ),
            new Page(BytesRefBlock.newConstantBlockWith(new BytesRef("Hello World"), positions))
        );
        final BytesStreamOutput out = new BytesStreamOutput();
        out.writeList(origPages);
        StreamInput in = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())), registry);

        List<Page> deserPages = in.readList(new Page.PageReader());
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origPages, unused -> deserPages);
    }

    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService());

    BytesRefArray bytesRefArrayOf(String... values) {
        var array = new BytesRefArray(values.length, bigArrays);
        Arrays.stream(values).map(BytesRef::new).forEach(array::append);
        return array;
    }
}
