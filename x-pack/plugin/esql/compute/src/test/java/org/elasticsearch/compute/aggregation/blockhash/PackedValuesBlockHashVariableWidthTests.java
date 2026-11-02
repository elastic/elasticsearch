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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
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
                try (PackedValuesBlockHash hash = new PackedValuesBlockHash(specs, hashFactory, NoopCircuitBreaker.INSTANCE, 32)) {
                    CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> hash.add(page, new NoopAddInput()));
                    // Sanity-check the failure came from breaker accounting, not some other path.
                    assertThat(e.getMessage(), is(MockBigArrays.ERROR_MESSAGE));
                }
            }
        }
        // Hash is closed; everything it adjusted on the breaker must be released.
        assertThat("breaker must be zero after close", hashBreaker.getUsed(), equalTo(0L));
    }

    /**
     * A single-valued page holding nulls, which the bulk path also takes. A null adds no bytes to the key, only
     * its bit in the null-tracking prefix, so both the group ids and the keys {@code getKeys} reads back from
     * those bytes must match the slow path.
     */
    public void testNullsAgreeWithSlowPath() {
        BlockFactory factory = TestBlockFactory.getNonBreakingInstance();
        // Every present/null combination across the three columns, with values repeating under differing null
        // patterns, so a row differing only in which column is null takes its own group.
        Integer[] userIds = { 0, null, 0, null, 1, null, 1, 0, null, 0, null, 1, null };
        Long[] sessions = { 100L, 100L, null, null, 200L, 200L, null, 100L, 100L, null, null, 200L, null };
        String[] phrases = { "cat", "cat", "cat", null, null, "dog", "dog", "cat", "cat", null, null, "dog", null };
        final int positions = userIds.length;

        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.INT),
            new BlockHash.GroupSpec(1, ElementType.LONG),
            new BlockHash.GroupSpec(2, ElementType.BYTES_REF)
        );

        try (
            IntBlock.Builder ib = factory.newIntBlockBuilder(positions);
            LongBlock.Builder lb = factory.newLongBlockBuilder(positions);
            BytesRefBlock.Builder bb = factory.newBytesRefBlockBuilder(positions)
        ) {
            for (int i = 0; i < positions; i++) {
                if (userIds[i] == null) {
                    ib.appendNull();
                } else {
                    ib.appendInt(userIds[i]);
                }
                if (sessions[i] == null) {
                    lb.appendNull();
                } else {
                    lb.appendLong(sessions[i]);
                }
                if (phrases[i] == null) {
                    bb.appendNull();
                } else {
                    bb.appendBytesRef(new BytesRef(phrases[i]));
                }
            }
            try (IntBlock iv = ib.build(); LongBlock lv = lb.build(); BytesRefBlock brv = bb.build()) {
                Page page = new Page(iv, lv, brv);
                assertNull("the page must have no vectors, or the bulk path would not be exercising nulls", iv.asVector());

                int[] bulkOrds;
                List<String> bulkKeys;
                try (PackedValuesBlockHash bulk = new PackedValuesBlockHash(specs, factory, 64)) {
                    bulkOrds = collectOrds(positions, ai -> bulk.add(page, ai));
                    bulkKeys = renderKeys(bulk);
                }
                int[] slowOrds;
                List<String> slowKeys;
                try (PackedValuesBlockHash slow = new PackedValuesBlockHash(specs, factory, 64)) {
                    slowOrds = collectOrds(positions, ai -> slow.add(page, ai, 1024));
                    slowKeys = renderKeys(slow);
                }
                assertArrayEquals("bulk path and slow path must assign identical group ids", slowOrds, bulkOrds);
                assertThat("bulk path and slow path must read back identical keys", bulkKeys, equalTo(slowKeys));
                // Each group's key must be the row that made it, nulls included.
                for (int p = 0; p < positions; p++) {
                    assertThat(
                        "group " + bulkOrds[p] + " must hold the key of position " + p,
                        bulkKeys.get(bulkOrds[p]),
                        equalTo(renderRow(userIds[p], sessions[p], phrases[p]))
                    );
                }
            }
        }
    }

    /** Every group's key as text, group id order, so two hashes can be compared whatever blocks they build. */
    private static List<String> renderKeys(BlockHash hash) {
        IntVector selected = hash.nonEmpty();
        Block[] keys = hash.getKeys(selected);
        try {
            List<String> out = new ArrayList<>(keys[0].getPositionCount());
            for (int p = 0; p < keys[0].getPositionCount(); p++) {
                StringBuilder row = new StringBuilder();
                for (int g = 0; g < keys.length; g++) {
                    if (g > 0) {
                        row.append('|');
                    }
                    row.append(renderValue(keys[g], p));
                }
                out.add(row.toString());
            }
            return out;
        } finally {
            Releasables.close(selected);
            Releasables.close(keys);
        }
    }

    private static String renderValue(Block block, int position) {
        if (block.isNull(position)) {
            return "null";
        }
        final int i = block.getFirstValueIndex(position);
        return switch (block.elementType()) {
            case INT -> Integer.toString(((IntBlock) block).getInt(i));
            case LONG -> Long.toString(((LongBlock) block).getLong(i));
            case BYTES_REF -> ((BytesRefBlock) block).getBytesRef(i, new BytesRef()).utf8ToString();
            default -> throw new IllegalStateException("unsupported type: " + block.elementType());
        };
    }

    private static String renderRow(Integer userId, Long session, String phrase) {
        return userId + "|" + session + "|" + phrase;
    }

    /**
     * A page holding nulls whose encoded payload runs past {@code CHUNK_SOFT_CAP}, so the bulk path packs it in
     * several chunks. A row's nulls are tracked by its index within the chunk while its values are read at its
     * position in the page, so the two must stay aligned once a chunk starts at a non-zero offset.
     */
    public void testNullsAcrossChunkBoundaries() {
        final int positions = 200;
        final byte[] base = new byte[4096];
        Arrays.fill(base, (byte) 'x');

        CircuitBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(10));
        BlockFactory factory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(breaker).build();

        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.BYTES_REF),
            new BlockHash.GroupSpec(1, ElementType.LONG)
        );

        BytesRef scratch = new BytesRef();
        scratch.bytes = base;
        scratch.offset = 0;
        try (
            BytesRefBlock.Builder bb = factory.newBytesRefBlockBuilder(positions);
            LongBlock.Builder lb = factory.newLongBlockBuilder(positions)
        ) {
            for (int i = 0; i < positions; i++) {
                // Both columns hold no value at some positions, on cycles that do not divide the chunk size.
                if (i % 7 == 0) {
                    bb.appendNull();
                } else {
                    scratch.length = base.length - (i % 3);
                    bb.appendBytesRef(scratch);
                }
                if (i % 5 == 0) {
                    lb.appendNull();
                } else {
                    lb.appendLong(i % 11);
                }
            }
            try (BytesRefBlock brb = bb.build(); LongBlock lgb = lb.build()) {
                Page page = new Page(brb, lgb);
                // emitBatchSize > positions, so chunking inside bulkAdd breaks on the soft cap rather than on the
                // emit boundary.
                int[] bulkOrds;
                List<String> bulkKeys;
                try (PackedValuesBlockHash bulk = new PackedValuesBlockHash(specs, factory, 256)) {
                    bulkOrds = collectOrds(positions, ai -> bulk.add(page, ai));
                    bulkKeys = renderKeys(bulk);
                }
                int[] slowOrds;
                List<String> slowKeys;
                try (PackedValuesBlockHash slow = new PackedValuesBlockHash(specs, factory, 256)) {
                    slowOrds = collectOrds(positions, ai -> slow.add(page, ai, 1024));
                    slowKeys = renderKeys(slow);
                }
                assertArrayEquals("multi-chunk bulk path with nulls must agree with the slow path", slowOrds, bulkOrds);
                assertThat("multi-chunk keys must agree with the slow path", bulkKeys, equalTo(slowKeys));
            }
        }
        assertThat("breaker must return to zero after both hashes close", breaker.getUsed(), equalTo(0L));
    }

    /**
     * More key columns than a long has bits, with nulls in two columns exactly 64 apart. The bulk path tracks a
     * row's nulls in one long, so it declines a page this wide and the encoders pack it instead.
     */
    public void testMoreColumnsThanNullMaskBits() {
        final int columns = Long.SIZE + 6;
        final int positions = 8;
        BlockFactory factory = TestBlockFactory.getNonBreakingInstance();

        List<BlockHash.GroupSpec> specs = new ArrayList<>(columns);
        for (int g = 0; g < columns - 1; g++) {
            specs.add(new BlockHash.GroupSpec(g, ElementType.INT));
        }
        specs.add(new BlockHash.GroupSpec(columns - 1, ElementType.BYTES_REF));

        Block[] blocks = new Block[columns];
        for (int g = 0; g < columns - 1; g++) {
            try (IntBlock.Builder b = factory.newIntBlockBuilder(positions)) {
                for (int p = 0; p < positions; p++) {
                    // Column 1 holds no value on even positions, column 65 on odd ones.
                    if ((g == 1 && p % 2 == 0) || (g == Long.SIZE + 1 && p % 2 == 1)) {
                        b.appendNull();
                    } else {
                        b.appendInt(g * 100);
                    }
                }
                blocks[g] = b.build();
            }
        }
        try (BytesRefBlock.Builder b = factory.newBytesRefBlockBuilder(positions)) {
            for (int p = 0; p < positions; p++) {
                b.appendBytesRef(new BytesRef("same"));
            }
            blocks[columns - 1] = b.build();
        }

        Page page = new Page(blocks);
        try {
            int[] bulkOrds;
            List<String> bulkKeys;
            try (PackedValuesBlockHash bulk = new PackedValuesBlockHash(specs, factory, 64)) {
                bulkOrds = collectOrds(positions, ai -> bulk.add(page, ai));
                bulkKeys = renderKeys(bulk);
            }
            int[] slowOrds;
            List<String> slowKeys;
            try (PackedValuesBlockHash slow = new PackedValuesBlockHash(specs, factory, 64)) {
                slowOrds = collectOrds(positions, ai -> slow.add(page, ai, 1024));
                slowKeys = renderKeys(slow);
            }
            assertArrayEquals("group ids must agree beyond the null mask width", slowOrds, bulkOrds);
            assertThat("keys must agree beyond the null mask width", bulkKeys, equalTo(slowKeys));
            // The two null patterns are different keys, so the positions must not all collapse together.
            assertThat(bulkKeys.size(), equalTo(2));
        } finally {
            Releasables.close(blocks);
        }
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
