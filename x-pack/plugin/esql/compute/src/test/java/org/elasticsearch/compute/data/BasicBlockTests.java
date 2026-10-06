/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.test.BlockTestUtils;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static org.elasticsearch.compute.test.BlockTestUtils.valuesAtPositions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class BasicBlockTests extends ESTestCase {
    final CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
    final CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
    final BlockFactory blockFactory = BlockFactory.builder(new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService))
        .build();

    @Before
    @After
    public void checkBreaker() {
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testEmpty() {
        assertZeroPositionsAndRelease(blockFactory.newAggregateMetricDoubleBlockBuilder(0).build());
    }

    public void testSmallSingleValueDenseGrowthAggregateMetricDouble() {
        // AggregateMetricDouble has no Vector view and a composite getTotalValueCount(), so it cannot
        // use assertSingleValueDenseBlock. This only checks builder growth + basic dense properties.
        for (int initialSize : List.of(0, 1, 2, 3, 4, 5)) {
            try (var blockBuilder = blockFactory.newAggregateMetricDoubleBlockBuilder(initialSize)) {
                IntStream.range(0, 10)
                    .forEach(
                        i -> blockBuilder.appendLiteral(
                            new AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral(
                                (double) i,
                                (double) i + 1,
                                (double) (i * 2),
                                i
                            )
                        )
                    );
                try (AggregateMetricDoubleBlock block = blockBuilder.build()) {
                    assertThat(block.getPositionCount(), is(10));
                    assertThat(block.asVector(), nullValue());
                    assertThat(block.mayHaveNulls(), is(false));
                    assertThat(block.areAllValuesNull(), is(false));
                    assertThat(block.mayHaveMultivaluedFields(), is(false));
                    assertThat(block.doesHaveMultivaluedFields(), is(false));
                    for (int p = 0; p < 10; p++) {
                        assertThat(block.isNull(p), is(false));
                        assertThat(block.getValueCount(p), is(1));
                        assertThat(block.minBlock().getDouble(p), is((double) p));
                        assertThat(block.maxBlock().getDouble(p), is((double) p + 1));
                        assertThat(block.sumBlock().getDouble(p), is((double) (p * 2)));
                        assertThat(block.countBlock().getInt(p), is(p));
                    }
                    assertDeepCopy(block);
                    try (Block filtered = block.filter(false)) {
                        assertThat(filtered.getPositionCount(), is(0));
                    }
                    try (Block sliced = block.slice(0, 10)) {
                        assertThat(sliced.getPositionCount(), is(10));
                        for (int p = 0; p < 10; p++) {
                            assertEquals(BlockUtils.toJavaObject(block, p), BlockUtils.toJavaObject(sliced, p));
                        }
                    }
                }
            }
        }
    }

    static void assertSingleValueDenseBlock(Block initialBlock) {
        final int positionCount = initialBlock.getPositionCount();
        int depth = randomIntBetween(1, 5);
        for (int d = 0; d < depth; d++) {
            Block block = initialBlock;
            assertValueCounts(block);
            assertThat(block.getTotalValueCount(), is(positionCount));
            assertThat(block.getPositionCount(), is(positionCount));
            for (int j = 0; j < 10; j++) {
                int pos = randomPosition(positionCount);
                assertThat(block.getFirstValueIndex(pos), is(pos));
                assertThat(block.getValueCount(pos), is(1));
                assertThat(block.isNull(pos), is(false));
            }
            assertThat(block.asVector().getPositionCount(), is(positionCount));
            assertThat(block.asVector().asBlock().getTotalValueCount(), is(positionCount));
            assertThat(block.asVector().asBlock().getPositionCount(), is(positionCount));
            assertThat(block.mayHaveNulls(), is(false));
            assertThat(block.areAllValuesNull(), is(false));
            assertThat(block.mayHaveMultivaluedFields(), is(false));
            assertThat(block.doesHaveMultivaluedFields(), is(false));

            initialBlock = block.asVector().asBlock();
        }
        assertKeepMask(initialBlock);
        assertKeepMask(initialBlock.asVector());
        assertFilter(initialBlock);
        assertFilter(initialBlock.asVector());
        assertSlice(initialBlock);
        assertSlice(initialBlock.asVector());
        assertDeepCopy(initialBlock);
    }

    public void testConstantNullBlock() {
        for (int i = 0; i < 100; i++) {
            assertThat(breaker.getUsed(), is(0L));
            int positionCount = randomIntBetween(1, 16 * 1024);
            Block block = blockFactory.newConstantNullBlock(positionCount);
            assertTrue(block.areAllValuesNull());
            assertThat(block, instanceOf(BooleanBlock.class));
            assertThat(block, instanceOf(IntBlock.class));
            assertThat(block, instanceOf(LongBlock.class));
            assertThat(block, instanceOf(DoubleBlock.class));
            assertThat(block, instanceOf(BytesRefBlock.class));
            assertNull(block.asVector());
            if (randomBoolean()) {
                Block orig = block;
                block = (new ConstantNullBlock.Builder(blockFactory)).copyFrom(block, 0, block.getPositionCount()).build();
                orig.close();
            }
            assertThat(positionCount, is(block.getPositionCount()));
            assertThat(block.getPositionCount(), is(positionCount));
            assertThat(block.isNull(randomPosition(positionCount)), is(true));
            if (positionCount > 2) {
                List<List<Object>> expected = new ArrayList<>();
                expected.add(null);
                expected.add(null);
                expected.add(null);
                assertLookup(
                    block,
                    positions(blockFactory, 1, 2, new int[] { 1, 2 }),
                    expected,
                    b -> assertThat(b, instanceOf(ConstantNullBlock.class))
                );
            }
            assertLookup(
                block,
                positions(blockFactory, positionCount + 1000),
                singletonList(null),
                b -> assertThat(b, instanceOf(ConstantNullBlock.class))
            );
            assertThat(((IntBlock) block).valueMaxByteSize(), equalTo(0));
            assertInsertNulls(block);
            assertDeepCopy(block);
            releaseAndAssertBreaker(block);
        }
    }

    void assertZeroPositionsAndRelease(BooleanBlock block) {
        assertToMaskZeroPositions(block);
        assertZeroPositionsAndRelease((Block) block);
    }

    void assertZeroPositionsAndRelease(Block block) {
        assertThat(block.getPositionCount(), is(0));
        assertValueCounts(block);
        assertKeepMaskEmpty(block);
        assertInsertNulls(block);
        releaseAndAssertBreaker(block);
    }

    void assertZeroPositionsAndRelease(BooleanVector vector) {
        assertToMask(vector);
        assertZeroPositionsAndRelease((Vector) vector);
    }

    void assertZeroPositionsAndRelease(Vector vector) {
        assertThat(vector.getPositionCount(), is(0));
        assertKeepMaskEmpty(vector);
        releaseAndAssertBreaker(vector);
    }

    static void assertKeepMaskEmpty(Block block) {
        try (BooleanVector mask = randomMask(between(0, 1000)); Block masked = block.keepMask(mask)) {
            if (false == (masked == block || masked.asVector() == block.asVector())) {
                fail("should return original block or vector");
            }
        }
    }

    static void assertKeepMaskEmpty(Vector vector) {
        try (BooleanVector mask = randomMask(between(0, 1000)); Block masked = vector.keepMask(mask)) {
            assertThat(masked.asVector(), sameInstance(vector));
        }
    }

    static void assertToMaskZeroPositions(BooleanBlock block) {
        try (ToMask mask = block.toMask()) {
            assertThat(mask.mask().getPositionCount(), equalTo(0));
            assertThat(mask.hadMultivaluedFields(), equalTo(false));
        }
    }

    static void assertToMask(BooleanVector vector) {
        try (ToMask mask = vector.asBlock().toMask()) {
            assertThat(mask.mask(), sameInstance(vector));
            assertThat(mask.hadMultivaluedFields(), equalTo(false));
        }
    }

    static void assertInsertNulls(Block block) {
        int maxNulls = Math.min(1000, block.getPositionCount() * 5);
        List<Object> orig = new ArrayList<>(block.getPositionCount());
        BlockTestUtils.readInto(orig, block);

        int nullCount = 0;
        try (IntVector.Builder beforeBuilder = block.blockFactory().newIntVectorBuilder(block.getPositionCount())) {
            List<Object> expected = new ArrayList<>(block.getPositionCount());
            for (int p = 0; p < block.getPositionCount(); p++) {
                while (nullCount < maxNulls && randomBoolean()) {
                    expected.add(null);
                    beforeBuilder.appendInt(p);
                    nullCount++;
                }
                expected.add(orig.get(p));
            }
            while (nullCount == 0 || (nullCount < maxNulls && randomBoolean())) {
                expected.add(null);
                beforeBuilder.appendInt(block.getPositionCount());
                nullCount++;
            }

            try (IntVector before = beforeBuilder.build(); Block withNulls = block.insertNulls(before)) {
                List<Object> actual = new ArrayList<>(block.getPositionCount());
                BlockTestUtils.readInto(actual, withNulls);
                assertThat(actual, equalTo(expected));
            }
        }
    }

    void releaseAndAssertBreaker(Block... blocks) {
        assertThat(breaker.getUsed(), greaterThan(0L));
        Page[] pages = Arrays.stream(blocks).map(Page::new).toArray(Page[]::new);

        /*
         * Deep copy the block into the non-breaking instance to make
         * sure that works and that we can read from the deep copy after
         * this has been released.
         */
        Block[] deepCopies = new Block[blocks.length];
        for (int b = 0; b < blocks.length; b++) {
            Block copiedOutOfBreaker = blocks[b].deepCopy(TestBlockFactory.getNonBreakingInstance());
            assertThat(copiedOutOfBreaker, equalTo(blocks[b]));
            deepCopies[b] = copiedOutOfBreaker;
        }

        Releasables.closeExpectNoException(blocks);
        Arrays.stream(blocks).forEach(block -> assertThat(block.isReleased(), is(true)));
        Arrays.stream(blocks).forEach(BasicBlockTests::assertCannotDoubleRelease);
        Arrays.stream(pages).forEach(BasicBlockTests::assertCannotReadFromPage);
        Arrays.stream(blocks).forEach(BasicBlockTests::assertCannotAddToPage);
        assertThat(breaker.getUsed(), is(0L));

        for (int b = 0; b < deepCopies.length; b++) {
            BlockTestUtils.readInto(new ArrayList<>(), deepCopies[b]);
        }
    }

    void releaseAndAssertBreaker(Vector vector) {
        assertThat(breaker.getUsed(), greaterThan(0L));

        /*
         * Deep copy the vector into the non-breaking instance to make
         * sure that works and that we can read from the deep copy after
         * this has been released.
         */
        Vector copiedOutOfBreaker = vector.deepCopy(TestBlockFactory.getNonBreakingInstance());
        assertThat(copiedOutOfBreaker, equalTo(vector));

        Releasables.closeExpectNoException(vector);
        assertThat(breaker.getUsed(), is(0L));

        BlockTestUtils.readInto(new ArrayList<>(), copiedOutOfBreaker.asBlock());
    }

    static void assertCannotDoubleRelease(Block block) {
        var ex = expectThrows(IllegalStateException.class, () -> block.close());
        assertThat(ex.getMessage(), containsString("can't release already released object"));
    }

    static void assertCannotReadFromPage(Page page) {
        var e = expectThrows(IllegalStateException.class, () -> page.getBlock(0));
        assertThat(e.getMessage(), containsString("can't read released block"));
    }

    static void assertCannotAddToPage(Block block) {
        var e = expectThrows(IllegalArgumentException.class, () -> new Page(block));
        assertThat(e.getMessage(), containsString("can't build page out of released blocks but"));
    }

    static int randomPosition(int positionCount) {
        return positionCount == 1 ? 0 : randomIntBetween(0, positionCount - 1);
    }

    static Block.MvOrdering randomOrdering() {
        return randomFrom(Block.MvOrdering.values());
    }

    public void testRefCountingArrayBlock() {
        Block block = randomArrayBlock();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingBigArrayBlock() {
        Block block = randomBigArrayBlock();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingConstantNullBlock() {
        Block block = blockFactory.newConstantNullBlock(10);
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingDocBlock() {
        int positionCount = randomIntBetween(0, 100);
        DocBlock block = new DocVector(
            AlwaysReferencedIndexedByShardId.INSTANCE,
            intVector(positionCount),
            intVector(positionCount),
            intVector(positionCount),
            DocVector.config().singleSegmentNonDecreasing(true)
        ).asBlock();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingVectorBlock() {
        Block block = randomConstantVector().asBlock();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(block);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingArrayVector() {
        Vector vector = randomArrayVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingBigArrayVector() {
        Vector vector = randomBigArrayVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingConstantVector() {
        Vector vector = randomConstantVector();
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    public void testRefCountingDocVector() {
        int positionCount = randomIntBetween(0, 100);
        DocVector vector = new DocVector(
            AlwaysReferencedIndexedByShardId.INSTANCE,
            intVector(positionCount),
            intVector(positionCount),
            intVector(positionCount),
            DocVector.config().singleSegmentNonDecreasing(true)
        );
        assertThat(breaker.getUsed(), greaterThan(0L));
        assertRefCountingBehavior(vector);
        assertThat(breaker.getUsed(), is(0L));
    }

    /**
     * Take an object with exactly 1 reference and assert that ref counting works fine.
     * Assumes that {@link Releasable#close()} and {@link RefCounted#decRef()} are equivalent.
     */
    static <T extends RefCounted & Releasable> void assertRefCountingBehavior(T object) {
        assertTrue(object.hasReferences());
        int numShallowCopies = randomIntBetween(0, 15);
        for (int i = 0; i < numShallowCopies; i++) {
            if (randomBoolean()) {
                object.incRef();
            } else {
                assertTrue(object.tryIncRef());
            }
        }

        for (int i = 0; i < numShallowCopies; i++) {
            if (randomBoolean()) {
                object.close();
            } else {
                // closing and decRef'ing must be equivalent
                assertFalse(object.decRef());
            }
            assertTrue(object.hasReferences());
        }

        if (randomBoolean()) {
            object.close();
        } else {
            assertTrue(object.decRef());
        }

        assertFalse(object.hasReferences());
        assertFalse(object.tryIncRef());

        expectThrows(IllegalStateException.class, object::close);
        expectThrows(IllegalStateException.class, object::incRef);
    }

    private IntVector intVector(int positionCount) {
        return blockFactory.newIntArrayVector(IntStream.range(0, positionCount).toArray(), positionCount);
    }

    private Vector randomArrayVector() {
        int positionCount = randomIntBetween(0, 100);
        int vectorType = randomIntBetween(0, 4);

        return switch (vectorType) {
            case 0 -> {
                boolean[] values = new boolean[positionCount];
                Arrays.fill(values, randomBoolean());
                yield blockFactory.newBooleanArrayVector(values, positionCount);
            }
            case 1 -> {
                BytesRefArray values = new BytesRefArray(positionCount, BigArrays.NON_RECYCLING_INSTANCE);
                for (int i = 0; i < positionCount; i++) {
                    values.append(new BytesRef(randomByteArrayOfLength(between(1, 20))));
                }

                yield blockFactory.newBytesRefArrayVector(values, positionCount);
            }
            case 2 -> {
                double[] values = new double[positionCount];
                Arrays.fill(values, 1.0);

                yield blockFactory.newDoubleArrayVector(values, positionCount);
            }
            case 3 -> {
                int[] values = new int[positionCount];
                Arrays.fill(values, 1);

                yield blockFactory.newIntArrayVector(values, positionCount);
            }
            default -> {
                long[] values = new long[positionCount];
                Arrays.fill(values, 1L);

                yield blockFactory.newLongArrayVector(values, positionCount);
            }
        };
    }

    private Vector randomBigArrayVector() {
        int positionCount = randomIntBetween(0, 10000);
        int arrayType = randomIntBetween(0, 3);

        return switch (arrayType) {
            case 0 -> {
                BitArray values = new BitArray(positionCount, blockFactory.bigArrays());
                for (int i = 0; i < positionCount; i++) {
                    if (randomBoolean()) {
                        values.set(positionCount);
                    }
                }

                yield new BooleanBigArrayVector(values, positionCount, blockFactory);
            }
            case 1 -> {
                DoubleArray values = blockFactory.bigArrays().newDoubleArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomDouble());
                }

                yield new DoubleBigArrayVector(values, positionCount, blockFactory);
            }
            case 2 -> {
                IntArray values = blockFactory.bigArrays().newIntArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomInt());
                }

                yield new IntBigArrayVector(values, positionCount, blockFactory);
            }
            default -> {
                LongArray values = blockFactory.bigArrays().newLongArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomLong());
                }

                yield new LongBigArrayVector(values, positionCount, blockFactory);
            }
        };
    }

    private Vector randomConstantVector() {
        int positionCount = randomIntBetween(0, 100);
        int vectorType = randomIntBetween(0, 4);

        return switch (vectorType) {
            case 0 -> blockFactory.newConstantBooleanVector(true, positionCount);
            case 1 -> blockFactory.newConstantBytesRefVector(new BytesRef(), positionCount);
            case 2 -> blockFactory.newConstantDoubleVector(1.0, positionCount);
            case 3 -> blockFactory.newConstantIntVector(1, positionCount);
            default -> blockFactory.newConstantLongVector(1L, positionCount);
        };
    }

    private Block randomArrayBlock() {
        int positionCount = randomIntBetween(0, 100);
        int arrayType = randomIntBetween(0, 4);
        int[] firstValueIndexes = IntStream.range(0, positionCount + 1).toArray();

        return switch (arrayType) {
            case 0 -> {
                boolean[] values = new boolean[positionCount];
                Arrays.fill(values, randomBoolean());

                yield blockFactory.newBooleanArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
            case 1 -> {
                BytesRefArray values = new BytesRefArray(positionCount, BigArrays.NON_RECYCLING_INSTANCE);
                for (int i = 0; i < positionCount; i++) {
                    values.append(new BytesRef(randomByteArrayOfLength(between(1, 20))));
                }

                yield blockFactory.newBytesRefArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
            case 2 -> {
                double[] values = new double[positionCount];
                Arrays.fill(values, 1.0);

                yield blockFactory.newDoubleArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
            case 3 -> {
                int[] values = new int[positionCount];
                Arrays.fill(values, 1);

                yield blockFactory.newIntArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
            default -> {
                long[] values = new long[positionCount];
                Arrays.fill(values, 1L);

                yield blockFactory.newLongArrayBlock(values, positionCount, firstValueIndexes, new BitSet(), randomOrdering());
            }
        };
    }

    private Block randomBigArrayBlock() {
        int positionCount = randomIntBetween(0, 10000);
        int arrayType = randomIntBetween(0, 3);

        return switch (arrayType) {
            case 0 -> {
                BitArray values = new BitArray(positionCount, blockFactory.bigArrays());
                for (int i = 0; i < positionCount; i++) {
                    if (randomBoolean()) {
                        values.set(positionCount);
                    }
                }

                yield new BooleanBigArrayBlock(values, positionCount, null, new BitSet(), Block.MvOrdering.UNORDERED, blockFactory);
            }
            case 1 -> {
                DoubleArray values = blockFactory.bigArrays().newDoubleArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomDouble());
                }

                yield new DoubleBigArrayBlock(values, positionCount, null, new BitSet(), Block.MvOrdering.UNORDERED, blockFactory);
            }
            case 2 -> {
                IntArray values = blockFactory.bigArrays().newIntArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomInt());
                }

                yield new IntBigArrayBlock(values, positionCount, null, new BitSet(), Block.MvOrdering.UNORDERED, blockFactory);
            }
            default -> {
                LongArray values = blockFactory.bigArrays().newLongArray(positionCount, false);
                for (int i = 0; i < positionCount; i++) {
                    values.set(i, randomLong());
                }

                yield new LongBigArrayBlock(values, positionCount, null, new BitSet(), Block.MvOrdering.UNORDERED, blockFactory);
            }
        };
    }

    static IntBlock positions(BlockFactory blockFactory, Object... positions) {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(positions.length)) {
            for (Object p : positions) {
                if (p instanceof int[] mv) {
                    builder.beginPositionEntry();
                    for (int v : mv) {
                        builder.appendInt(v);
                    }
                    builder.endPositionEntry();
                    continue;
                }
                if (p instanceof Integer v) {
                    builder.appendInt(v);
                    continue;
                }
                throw new IllegalArgumentException("invalid position: " + p + "(" + p.getClass().getName() + ")");
            }
            return builder.build();
        }
    }

    static void assertEmptyLookup(BlockFactory blockFactory, Block block) {
        try (
            IntBlock positions = positions(blockFactory);
            ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))
        ) {
            assertThat(lookup.hasNext(), equalTo(false));
        }
    }

    static void assertLookup(Block block, IntBlock positions, List<List<Object>> expected) {
        assertLookup(block, positions, expected, l -> {});
    }

    static void assertLookup(Block block, IntBlock positions, List<List<Object>> expected, Consumer<Block> extra) {
        try (positions; ReleasableIterator<? extends Block> lookup = block.lookup(positions, ByteSizeValue.ofKb(100))) {
            assertThat(lookup.hasNext(), equalTo(true));
            try (Block b = lookup.next()) {
                assertThat(valuesAtPositions(b, 0, b.getPositionCount()), equalTo(expected));
                assertThat(b.blockFactory(), sameInstance(positions.blockFactory()));
                extra.accept(b);
            }
            assertThat(lookup.hasNext(), equalTo(false));
        }
    }

    public static Block assertDeepCopy(Block block) {
        CircuitBreakerService breakerService = newLimitedBreakerService(ByteSizeValue.ofGb(1));
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService);
        BlockFactory into = BlockFactory.builder(bigArrays).build();
        try (Block deepCopy = block.deepCopy(into)) {
            assertThat(deepCopy, equalTo(block));

            if (block.asVector() != null && block.asVector().isConstant()) {
                /*
                 * If we were a constant, we will still be one. If we were not a constant,
                 * deepCopy might make a constant in the rare case that we have a single element array.
                 */
                assertThat(deepCopy.asVector() != null && deepCopy.asVector().isConstant(), equalTo(true));
            }
        }
        Block untracked = block.deepCopy(TestBlockFactory.getNonBreakingInstance());
        assertThat(untracked, equalTo(block));
        // untracked doesn't need to be released
        return untracked;
    }

    public static void assertValueCounts(Block block) {
        int totalValueCount = 0;
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (block.isNull(p)) {
                assertThat(block.getValueCount(p), equalTo(0));
            }
            totalValueCount += block.getValueCount(p);
        }
        assertThat(block.getTotalValueCount(), equalTo(totalValueCount));
        for (int p = 0; p + 1 < block.getPositionCount(); p++) {
            if (block.isNull(p) == false) {
                assertThat(block.getValueCount(p), equalTo(block.getFirstValueIndex(p + 1) - block.getFirstValueIndex(p)));
            }
        }
    }

    public static void assertKeepMask(Vector vector) {
        int maskPositions = vector.getPositionCount();
        if (randomBoolean()) {
            maskPositions += between(1, 1000);
        }
        try (
            BooleanVector mask = TestBlockFactory.getNonBreakingInstance().newConstantBooleanVector(true, maskPositions);
            Block masked = vector.keepMask(mask)
        ) {
            assertThat(masked.asVector(), sameInstance(vector));
        }
        try (
            BooleanVector mask = TestBlockFactory.getNonBreakingInstance().newConstantBooleanVector(false, maskPositions);
            Block masked = vector.keepMask(mask)
        ) {
            assertThat(masked.getPositionCount(), equalTo(vector.getPositionCount()));
            assertValueCounts(masked);
            for (int p = 0; p < vector.getPositionCount(); p++) {
                assertTrue(masked.isNull(p));
            }
        }
        try (BooleanVector mask = randomMask(maskPositions); Block masked = vector.keepMask(mask)) {
            assertThat(masked.getPositionCount(), equalTo(vector.getPositionCount()));
            assertValueCounts(masked);
            for (int p = 0; p < vector.getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    assertFalse(masked.isNull(p));
                    assertEquals(1, masked.getValueCount(p));
                    assertEquals(BlockUtils.toJavaObject(vector.asBlock(), p), BlockUtils.toJavaObject(masked, p));
                } else {
                    assertTrue(masked.isNull(p));
                }
            }
        }
    }

    public static void assertKeepMask(Block block) {
        int maskPositions = block.getPositionCount();
        if (randomBoolean()) {
            maskPositions += between(1, 1000);
        }
        try (
            BooleanVector mask = TestBlockFactory.getNonBreakingInstance().newConstantBooleanVector(true, maskPositions);
            Block masked = block.keepMask(mask)
        ) {
            if (false == (masked == block || masked.asVector() == block.asVector())) {
                fail("should return original block or vector");
            }
        }
        try (
            BooleanVector mask = TestBlockFactory.getNonBreakingInstance().newConstantBooleanVector(false, maskPositions);
            Block masked = block.keepMask(mask)
        ) {
            assertThat(masked.getPositionCount(), equalTo(block.getPositionCount()));
            assertValueCounts(masked);
            for (int p = 0; p < block.getPositionCount(); p++) {
                assertTrue(masked.isNull(p));
            }
        }
        try (BooleanVector mask = randomMask(maskPositions); Block masked = block.keepMask(mask)) {
            assertThat(masked.getPositionCount(), equalTo(block.getPositionCount()));
            assertValueCounts(masked);
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (mask.getBoolean(p) && false == block.isNull(p)) {
                    assertFalse(masked.isNull(p));
                    assertEquals(block.getValueCount(p), masked.getValueCount(p));
                    assertEquals(BlockUtils.toJavaObject(block, p), BlockUtils.toJavaObject(masked, p));
                } else {
                    assertTrue(masked.isNull(p));
                }
            }
        }
    }

    /**
     * Asserts the behavior of {@link Block#filter} with random filters.
     */
    public static void assertFilter(Block block) {
        int positionCount = block.getPositionCount();
        try (Block filtered = block.filter(false)) {
            assertThat(filtered.getPositionCount(), equalTo(0));
            assertValueCounts(filtered);
        }
        if (positionCount == 0) {
            return;
        }
        int[] allPositions = IntStream.range(0, positionCount).toArray();
        try (Block filtered = block.filter(false, allPositions)) {
            assertThat(filtered.getPositionCount(), equalTo(positionCount));
            assertValueCounts(filtered);
            for (int p = 0; p < positionCount; p++) {
                assertEquals(BlockUtils.toJavaObject(block, p), BlockUtils.toJavaObject(filtered, p));
            }
        }
        int[] subsetPositions = randomSubsetOf(between(1, positionCount), IntStream.range(0, positionCount).boxed().toList()).stream()
            .mapToInt(Integer::intValue)
            .toArray();
        try (Block filtered = block.filter(false, subsetPositions)) {
            assertThat(filtered.getPositionCount(), equalTo(subsetPositions.length));
            assertValueCounts(filtered);
            for (int p = 0; p < subsetPositions.length; p++) {
                assertEquals(BlockUtils.toJavaObject(block, subsetPositions[p]), BlockUtils.toJavaObject(filtered, p));
            }
        }
        int[] subsetWithRepeats = randomList(1, 1000, () -> between(0, positionCount - 1)).stream().mapToInt(i -> i).toArray();
        try (Block filtered = block.filter(true, subsetWithRepeats)) {
            assertThat(filtered.getPositionCount(), equalTo(subsetWithRepeats.length));
            assertValueCounts(filtered);
            for (int p = 0; p < subsetWithRepeats.length; p++) {
                assertEquals(BlockUtils.toJavaObject(block, subsetWithRepeats[p]), BlockUtils.toJavaObject(filtered, p));
            }
        }
    }

    /**
     * Asserts the behavior of {@link Vector#filter} with random filter.
     */
    public static void assertFilter(Vector vector) {
        int positionCount = vector.getPositionCount();
        try (Vector filtered = vector.filter(false)) {
            assertThat(filtered.getPositionCount(), equalTo(0));
        }
        if (positionCount == 0) {
            return;
        }
        int[] allPositions = IntStream.range(0, positionCount).toArray();
        try (Vector filtered = vector.filter(false, allPositions)) {
            assertThat(filtered.getPositionCount(), equalTo(positionCount));
            for (int p = 0; p < positionCount; p++) {
                assertEquals(BlockUtils.toJavaObject(vector.asBlock(), p), BlockUtils.toJavaObject(filtered.asBlock(), p));
            }
        }
        int[] subsetPositions = randomSubsetOf(between(1, positionCount), IntStream.range(0, positionCount).boxed().toList()).stream()
            .mapToInt(Integer::intValue)
            .toArray();
        Arrays.sort(subsetPositions);
        try (Vector filtered = vector.filter(false, subsetPositions)) {
            assertThat(filtered.getPositionCount(), equalTo(subsetPositions.length));
            for (int p = 0; p < subsetPositions.length; p++) {
                assertEquals(BlockUtils.toJavaObject(vector.asBlock(), subsetPositions[p]), BlockUtils.toJavaObject(filtered.asBlock(), p));
            }
        }
        int[] subsetWithRepeats = randomList(1, 1000, () -> between(0, positionCount - 1)).stream().mapToInt(i -> i).toArray();
        try (Vector filtered = vector.filter(true, subsetWithRepeats)) {
            assertThat(filtered.getPositionCount(), equalTo(subsetWithRepeats.length));
            for (int p = 0; p < subsetWithRepeats.length; p++) {
                assertEquals(
                    BlockUtils.toJavaObject(vector.asBlock(), subsetWithRepeats[p]),
                    BlockUtils.toJavaObject(filtered.asBlock(), p)
                );
            }
        }
    }

    /**
     * Asserts the behavior of {@link Block#slice} with random slices.
     */
    public static void assertSlice(Block block) {
        int positionCount = block.getPositionCount();
        assertSliceFullRange(block);
        if (positionCount == 0) {
            return;
        }
        try (Block sliced = block.slice(0, positionCount)) {
            assertThat(sliced.getPositionCount(), equalTo(positionCount));
            assertValueCounts(sliced);
            for (int p = 0; p < positionCount; p++) {
                assertEquals(BlockUtils.toJavaObject(block, p), BlockUtils.toJavaObject(sliced, p));
            }
        }
        int begin = between(0, positionCount);
        int end = between(begin, positionCount);
        try (Block sliced = block.slice(begin, end)) {
            assertThat(sliced.getPositionCount(), equalTo(end - begin));
            assertValueCounts(sliced);
            for (int p = 0; p < end - begin; p++) {
                assertEquals(BlockUtils.toJavaObject(block, begin + p), BlockUtils.toJavaObject(sliced, p));
            }
        }
    }

    /**
     * Asserts the behavior of {@link Vector#slice} with random slices.
     */
    public static void assertSlice(Vector vector) {
        int positionCount = vector.getPositionCount();
        assertSliceFullRange(vector);
        if (positionCount == 0) {
            return;
        }
        try (Vector sliced = vector.slice(0, positionCount)) {
            assertThat(sliced.getPositionCount(), equalTo(positionCount));
            for (int p = 0; p < positionCount; p++) {
                assertEquals(BlockUtils.toJavaObject(vector.asBlock(), p), BlockUtils.toJavaObject(sliced.asBlock(), p));
            }
        }
        int begin = between(0, positionCount);
        int end = between(begin, positionCount);
        try (Vector sliced = vector.slice(begin, end)) {
            assertThat(sliced.getPositionCount(), equalTo(end - begin));
            for (int p = 0; p < end - begin; p++) {
                assertEquals(BlockUtils.toJavaObject(vector.asBlock(), begin + p), BlockUtils.toJavaObject(sliced.asBlock(), p));
            }
        }
    }

    private static void assertSliceFullRange(Block block) {
        try (var sliced = block.slice(0, block.getPositionCount())) {
            assertThat(sliced, sameInstance(sliced));
        }
    }

    private static void assertSliceFullRange(Vector vector) {
        try (var sliced = vector.slice(0, vector.getPositionCount())) {
            assertThat(sliced, sameInstance(vector));
        }
    }

    /**
     * Build a random valid "mask" of single valued boolean fields that.
     */
    static BooleanVector randomMask(int positions) {
        try (BooleanVector.Builder builder = TestBlockFactory.getNonBreakingInstance().newBooleanVectorFixedBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                builder.appendBoolean(randomBoolean());
            }
            return builder.build();
        }
    }
}
