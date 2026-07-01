/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit coverage for {@link BlockChunks#concat}, the shared null-safe concatenation helper used by
 * the sparse decode paths. The randomized test builds an arbitrary sequence of chunks for one
 * element type, randomly replacing whole chunks with {@code ConstantNullBlock}s (the shape the
 * decoder emits for an all-null run), and asserts the concatenated output equals a reference block
 * assembled by appending every chunk's positions in order.
 *
 * <p>Directly reproduces the null-leading-run crash (#152592): before the fix, resolving the
 * builder element type from the first chunk turned a leading all-null run into a
 * {@code ConstantNullBlock.Builder}, which then rejected the next non-null chunk's {@code copyFrom}.
 */
public class BlockChunksTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * Explicit regression for the reported crash: the first surviving run is all-null, a later run
     * has values. Pre-fix this threw "can't append non-null values to a null block".
     */
    public void testNullLeadingRun() {
        for (ElementType type : List.of(
            ElementType.INT,
            ElementType.LONG,
            ElementType.DOUBLE,
            ElementType.BOOLEAN,
            ElementType.BYTES_REF
        )) {
            List<Object[]> expected = new ArrayList<>();
            List<Block> chunks = new ArrayList<>();
            chunks.add(nullChunk(2, expected));
            chunks.add(valueChunk(type, 3, false, expected));
            assertConcatMatches(type, chunks, expected);
        }
    }

    public void testRandom() {
        int iters = scaledRandomIntBetween(50, 300);
        for (int iter = 0; iter < iters; iter++) {
            ElementType type = randomFrom(
                ElementType.INT,
                ElementType.LONG,
                ElementType.DOUBLE,
                ElementType.BOOLEAN,
                ElementType.BYTES_REF
            );
            int numChunks = between(1, 6);
            List<Object[]> expected = new ArrayList<>();
            List<Block> chunks = new ArrayList<>(numChunks);
            for (int c = 0; c < numChunks; c++) {
                int positions = between(1, 8);
                if (randomBoolean()) {
                    // Whole all-null run, as ConstantNullBlock — the shape the decoder emits.
                    chunks.add(nullChunk(positions, expected));
                } else {
                    // Typed chunk, possibly carrying interior nulls of its own.
                    chunks.add(valueChunk(type, positions, randomBoolean(), expected));
                }
            }
            assertConcatMatches(type, chunks, expected);
        }
    }

    /**
     * Ownership contract on the failure path: when concat throws (here via the mixed-element-type
     * fail-fast assert) it must NOT close any input chunk — the caller's catch/finally owns their
     * release. Verifies the chunks are still live after the throw, then releases them.
     */
    public void testThrowLeavesChunksForCaller() {
        List<Object[]> ignored = new ArrayList<>();
        Block a = valueChunk(ElementType.INT, 3, false, ignored);
        Block b = valueChunk(ElementType.LONG, 2, false, ignored);
        List<Block> chunks = List.of(a, b);
        // Two genuinely different non-NULL element types trip the fail-fast type-agreement assert.
        AssertionError e = expectThrows(AssertionError.class, () -> BlockChunks.concat(chunks, blockFactory));
        assertTrue(e.getMessage().contains("mixed element types"));
        assertFalse("chunk must be left open on throw", a.isReleased());
        assertFalse("chunk must be left open on throw", b.isReleased());
        Releasables.closeExpectNoException(a, b);
    }

    /**
     * All-null branch failure contract: if {@code newConstantNullBlock} trips the breaker, concat
     * must not have closed any chunk (it allocates the result before closing). A zero-limit breaker
     * forces the trip; the input chunks (built on an unbounded factory) must survive for the caller.
     */
    public void testAllNullBranchBreakerTripLeavesChunksForCaller() {
        List<Object[]> ignored = new ArrayList<>();
        Block a = nullChunk(2, ignored);
        Block b = nullChunk(3, ignored);
        List<Block> chunks = List.of(a, b);
        BlockFactory tripping = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new LimitedBreaker("test", ByteSizeValue.ofBytes(0)))
            .build();
        expectThrows(CircuitBreakingException.class, () -> BlockChunks.concat(chunks, tripping));
        assertFalse("chunk must be left open when the all-null allocation trips", a.isReleased());
        assertFalse("chunk must be left open when the all-null allocation trips", b.isReleased());
        Releasables.closeExpectNoException(a, b);
    }

    public void testAllNullYieldsConstantNullBlock() {
        int numChunks = between(1, 5);
        List<Object[]> expected = new ArrayList<>();
        List<Block> chunks = new ArrayList<>(numChunks);
        for (int c = 0; c < numChunks; c++) {
            chunks.add(nullChunk(between(1, 6), expected));
        }
        Block[] snapshot = chunks.toArray(Block[]::new);
        Block result = BlockChunks.concat(chunks, blockFactory);
        try {
            assertEquals(ElementType.NULL, result.elementType());
            assertEquals(expected.size(), result.getPositionCount());
            assertTrue(result.areAllValuesNull());
            for (Block c : snapshot) {
                assertTrue("all-null input chunk must be released", c.isReleased());
            }
        } finally {
            result.close();
        }
    }

    private void assertConcatMatches(ElementType type, List<Block> chunks, List<Object[]> expected) {
        Block[] snapshot = chunks.toArray(Block[]::new);
        Block result = BlockChunks.concat(chunks, blockFactory);
        try {
            assertEquals(expected.size(), result.getPositionCount());
            for (int i = 0; i < expected.size(); i++) {
                Object[] cell = expected.get(i);
                if (cell == null) {
                    assertTrue("position " + i + " should be null", result.isNull(i));
                    continue;
                }
                assertFalse("position " + i + " should be non-null", result.isNull(i));
                assertValueEquals(type, result, i, cell[0]);
            }
            for (Block c : snapshot) {
                assertTrue("input chunk must be released after concat", c.isReleased());
            }
        } finally {
            result.close();
        }
    }

    private static void assertValueEquals(ElementType type, Block block, int pos, Object expected) {
        switch (type) {
            case INT -> assertEquals(expected, ((IntBlock) block).getInt(block.getFirstValueIndex(pos)));
            case LONG -> assertEquals(expected, ((LongBlock) block).getLong(block.getFirstValueIndex(pos)));
            case DOUBLE -> assertEquals((double) expected, ((DoubleBlock) block).getDouble(block.getFirstValueIndex(pos)), 0.0);
            case BOOLEAN -> assertEquals(expected, ((BooleanBlock) block).getBoolean(block.getFirstValueIndex(pos)));
            case BYTES_REF -> assertEquals(expected, ((BytesRefBlock) block).getBytesRef(block.getFirstValueIndex(pos), new BytesRef()));
            default -> throw new AssertionError("unexpected element type " + type);
        }
    }

    private Block nullChunk(int positions, List<Object[]> expected) {
        for (int i = 0; i < positions; i++) {
            expected.add(null);
        }
        return blockFactory.newConstantNullBlock(positions);
    }

    private Block valueChunk(ElementType type, int positions, boolean withNulls, List<Object[]> expected) {
        try (Block.Builder builder = type.newBlockBuilder(positions, blockFactory)) {
            for (int i = 0; i < positions; i++) {
                if (withNulls && randomBoolean()) {
                    builder.appendNull();
                    expected.add(null);
                } else {
                    Object value = appendRandomValue(type, builder);
                    expected.add(new Object[] { value });
                }
            }
            return builder.build();
        }
    }

    private Object appendRandomValue(ElementType type, Block.Builder builder) {
        switch (type) {
            case INT -> {
                int v = randomInt();
                ((IntBlock.Builder) builder).appendInt(v);
                return v;
            }
            case LONG -> {
                long v = randomLong();
                ((LongBlock.Builder) builder).appendLong(v);
                return v;
            }
            case DOUBLE -> {
                double v = randomDouble();
                ((DoubleBlock.Builder) builder).appendDouble(v);
                return v;
            }
            case BOOLEAN -> {
                boolean v = randomBoolean();
                ((BooleanBlock.Builder) builder).appendBoolean(v);
                return v;
            }
            case BYTES_REF -> {
                BytesRef v = new BytesRef(randomAlphaOfLengthBetween(0, 12));
                ((BytesRefBlock.Builder) builder).appendBytesRef(v);
                return v;
            }
            default -> throw new AssertionError("unexpected element type " + type);
        }
    }
}
