/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Targeted tests for the variable-width bulk path introduced for vector pages with at least
 * one {@code BYTES_REF} group column ({@code VariableWidthBatchWork}). The general correctness
 * envelope is covered by {@link BlockHashTests} and {@link BlockHashRandomizedTests}; these
 * tests pin down behaviours those suites don't reliably hit on every seed.
 */
public class PackedValuesBlockHashVariableWidthTests extends ESTestCase {

    /**
     * Build a vector-only page that mixes fixed-width and {@code BYTES_REF} columns, hash it
     * twice through fresh {@link PackedValuesBlockHash} instances — once through the public
     * {@code add(Page, AddInput)} entry (which dispatches to the bulk variable-width path)
     * and once directly through {@code add(Page, AddInput, batchSize)} (the slow {@code AddWork}
     * path) — and assert the resulting group ids are identical position-by-position.
     *
     * <p>This is the key correctness guarantee of the bulk path: it must produce byte-identical
     * keys to the slow path so that group ids from either entry collide correctly and
     * {@code getKeysWithNulls} (which uses {@code BatchEncoder.Decoder}) keeps working.
     */
    public void testBulkVsSlowPathAgreement() {
        BlockFactory factory = TestBlockFactory.getNonBreakingInstance();
        // Mix of repeated and unique 3-tuples so we exercise hash inserts and lookups.
        int[] userIds = { 0, 1, 0, 1, 0, 1, 2, 2, 0, 3, 3, 0 };
        long[] sessions = { 100L, 200L, 100L, 200L, 100L, 200L, 300L, 300L, 100L, 400L, 400L, 100L };
        String[] phrases = { "cat", "cat", "cat", "dog", "dog", "dog", "fish", "fish", "fish", "", "dog", "cat" };
        final int positions = userIds.length;

        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.INT),
            new BlockHash.GroupSpec(1, ElementType.LONG),
            new BlockHash.GroupSpec(2, ElementType.BYTES_REF)
        );

        try (
            IntVector.Builder ib = factory.newIntVectorBuilder(positions);
            LongVector.Builder lb = factory.newLongVectorBuilder(positions);
            BytesRefVector.Builder bb = factory.newBytesRefVectorBuilder(positions)
        ) {
            for (int i = 0; i < positions; i++) {
                ib.appendInt(userIds[i]);
                lb.appendLong(sessions[i]);
                bb.appendBytesRef(new BytesRef(phrases[i]));
            }
            try (IntVector iv = ib.build(); LongVector lv = lb.build(); BytesRefVector brv = bb.build()) {
                Page page = new Page(iv.asBlock(), lv.asBlock(), brv.asBlock());

                int[] bulkOrds;
                try (PackedValuesBlockHash bulk = new PackedValuesBlockHash(specs, factory, 64)) {
                    bulkOrds = collectOrds(positions, ai -> bulk.add(page, ai));
                }
                int[] slowOrds;
                try (PackedValuesBlockHash slow = new PackedValuesBlockHash(specs, factory, 64)) {
                    slowOrds = collectOrds(positions, ai -> slow.add(page, ai, 1024));
                }
                assertArrayEquals("bulk path and slow path must assign identical group ids", slowOrds, bulkOrds);
            }
        }
    }

    /**
     * Build a page whose total encoded payload exceeds {@code VariableWidthBatchWork}'s
     * 256 KB {@code CHUNK_SOFT_CAP} within a single emit batch, forcing {@code bulkAdd}
     * to run multiple chunks per emit batch. Verifies that:
     * <ul>
     *   <li>group ids still agree with the slow path across chunk boundaries, and</li>
     *   <li>breaker accounting returns to zero after both hashes close (i.e.&nbsp;keyBuf
     *       growth and its release stay in balance).</li>
     * </ul>
     * Three distinct payload lengths cycle through the rows so the per-row encoder
     * cursors see non-uniform stride across chunks.
     */
    public void testChunkingAcrossSoftCap() {
        final int positions = 200;
        final byte[] base = new byte[4096];
        Arrays.fill(base, (byte) 'x');

        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(10));
        BlockFactory factory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        List<BlockHash.GroupSpec> specs = List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF));
        BytesRef scratch = new BytesRef();
        scratch.bytes = base;
        scratch.offset = 0;

        try (BytesRefVector.Builder bb = factory.newBytesRefVectorBuilder(positions)) {
            for (int i = 0; i < positions; i++) {
                scratch.length = base.length - (i % 3);
                bb.appendBytesRef(scratch);
            }
            try (BytesRefVector brv = bb.build()) {
                Page page = new Page(brv.asBlock());
                // emitBatchSize > positions so all rows land in one emit batch; chunking inside
                // bulkAdd must then break on the 256 KB soft cap rather than on the emit boundary.
                int[] bulkOrds;
                try (PackedValuesBlockHash bulk = new PackedValuesBlockHash(specs, factory, 256)) {
                    bulkOrds = collectOrds(positions, ai -> bulk.add(page, ai));
                }
                int[] slowOrds;
                try (PackedValuesBlockHash slow = new PackedValuesBlockHash(specs, factory, 256)) {
                    slowOrds = collectOrds(positions, ai -> slow.add(page, ai, 1024));
                }
                assertArrayEquals("multi-chunk bulk path must agree with slow path", slowOrds, bulkOrds);
            }
        }
        assertThat("breaker must return to zero after both hashes close", breaker.getUsed(), equalTo(0L));
    }

    /**
     * A single row larger than {@code VariableWidthBatchWork}'s 256 KB {@code CHUNK_SOFT_CAP}.
     * The {@code rows > 0} guard in {@code collectRowSizes} is what stops {@code bulkAdd} from
     * looping forever here — without it, every chunk would reject the row for being too big.
     * Also confirms that {@code ensureKeyBuf} grows past the initial 8 KB and that the breaker
     * is balanced once the hash is closed.
     */
    public void testOversizedRow() {
        final int oversize = 512 * 1024;
        byte[] payload = new byte[oversize];
        Arrays.fill(payload, (byte) 'q');

        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(4));
        BlockFactory factory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        List<BlockHash.GroupSpec> specs = List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF));

        try (BytesRefVector.Builder bb = factory.newBytesRefVectorBuilder(1)) {
            bb.appendBytesRef(new BytesRef(payload));
            try (BytesRefVector brv = bb.build()) {
                Page page = new Page(brv.asBlock());
                int[] ords;
                try (PackedValuesBlockHash bulk = new PackedValuesBlockHash(specs, factory, 32)) {
                    ords = collectOrds(1, ai -> bulk.add(page, ai));
                }
                assertEquals("oversized single row must hash to the first group", 0, ords[0]);
            }
        }
        assertThat("breaker must return to zero after close", breaker.getUsed(), equalTo(0L));
    }

    /**
     * Force {@code VariableWidthBatchWork.ensureKeyBuf} to need a buffer larger than the breaker
     * allows, by feeding a single oversized {@code BYTES_REF} row. The breaker has enough room
     * for the hash's fixed initial allocation but not for the {@code keyBuf} grow; we assert
     * {@link CircuitBreakingException} propagates out of {@code add} and that the breaker is
     * back to zero after the hash is closed.
     */
    public void testKeyBufBreakerRollback() {
        // 200 KB is enough for the initial fixed allocation (~10 KB) plus the swiss-hash internal
        // pages, but well below the 1 MB+ needed once keyBuf grows to fit the oversized row.
        CircuitBreaker hashBreaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofKb(200));
        BlockFactory hashFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(hashBreaker).build();
        // Build the input page with a separate non-breaking factory so the breaker only sees
        // allocations made by the hash itself.
        BlockFactory dataFactory = TestBlockFactory.getNonBreakingInstance();

        byte[] big = new byte[1024 * 1024];
        Arrays.fill(big, (byte) 'a');

        List<BlockHash.GroupSpec> specs = List.of(new BlockHash.GroupSpec(0, ElementType.BYTES_REF));

        try (BytesRefVector.Builder bb = dataFactory.newBytesRefVectorBuilder(1)) {
            bb.appendBytesRef(new BytesRef(big));
            try (BytesRefVector brv = bb.build()) {
                Page page = new Page(brv.asBlock());
                try (
                    PackedValuesBlockHash hash = new PackedValuesBlockHash(
                        specs,
                        hashFactory,
                        new NoopCircuitBreaker("test-bytes-builder"),
                        32
                    )
                ) {
                    CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> hash.add(page, new NoopAddInput()));
                    // Sanity-check the failure came from breaker accounting, not some other path.
                    assertThat(e.getMessage(), is(MockBigArrays.ERROR_MESSAGE));
                }
            }
        }
        // Hash is closed; everything it adjusted on the breaker must be released.
        assertThat("breaker must be zero after close", hashBreaker.getUsed(), equalTo(0L));
    }

    private static int[] collectOrds(int positions, java.util.function.Consumer<GroupingAggregatorFunction.AddInput> driver) {
        int[] out = new int[positions];
        Arrays.fill(out, Integer.MIN_VALUE);
        driver.accept(new CapturingAddInput(out));
        for (int p = 0; p < positions; p++) {
            assertNotEquals("no group id emitted for position " + p, Integer.MIN_VALUE, out[p]);
        }
        return out;
    }

    private static final class CapturingAddInput implements GroupingAggregatorFunction.AddInput {
        private final int[] out;

        CapturingAddInput(int[] out) {
            this.out = out;
        }

        private void copy(int positionOffset, IntBlock groupIds) {
            for (int p = 0; p < groupIds.getPositionCount(); p++) {
                assertEquals("expected single-value group id", 1, groupIds.getValueCount(p));
                out[positionOffset + p] = groupIds.getInt(groupIds.getFirstValueIndex(p));
            }
        }

        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
            copy(positionOffset, groupIds);
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
            copy(positionOffset, groupIds);
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            copy(positionOffset, groupIds.asBlock());
        }

        @Override
        public void close() {}
    }

    private static final class NoopAddInput implements GroupingAggregatorFunction.AddInput {
        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {}

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {}

        @Override
        public void add(int positionOffset, IntVector groupIds) {}

        @Override
        public void close() {}
    }
}
