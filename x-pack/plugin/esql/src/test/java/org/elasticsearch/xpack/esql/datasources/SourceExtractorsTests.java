/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link SourceExtractors}: the encoding scheme, registration lifecycle, and the
 * batched {@link SourceExtractors#materialize} dispatch that fans positions out to the right
 * per-file extractor and stitches results back into caller order.
 */
public class SourceExtractorsTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testEncodeDecodeRoundTrip() {
        int[] ids = new int[] { 0, 1, 7, 1024, SourceExtractors.MAX_EXTRACTOR_ID };
        long[] positions = new long[] { 0L, 1L, 1234L, 1L << 30, SourceExtractors.MAX_LOCAL_POSITION };
        for (int id : ids) {
            for (long pos : positions) {
                long encoded = SourceExtractors.encode(id, pos);
                assertThat("encoded must be non-negative for sortable ordering", encoded, org.hamcrest.Matchers.greaterThanOrEqualTo(0L));
                assertEquals(id, SourceExtractors.decodeExtractorId(encoded));
                assertEquals(pos, SourceExtractors.decodeLocalPosition(encoded));
            }
        }
    }

    public void testEncodeRejectsOutOfRange() {
        expectThrows(IllegalArgumentException.class, () -> SourceExtractors.encode(-1, 0));
        expectThrows(IllegalArgumentException.class, () -> SourceExtractors.encode(SourceExtractors.MAX_EXTRACTOR_ID + 1, 0));
        expectThrows(IllegalArgumentException.class, () -> SourceExtractors.encode(0, -1));
        expectThrows(IllegalArgumentException.class, () -> SourceExtractors.encode(0, SourceExtractors.MAX_LOCAL_POSITION + 1));
    }

    public void testEncodingIsMonotonicAndNonNegative() {
        // Exhaustive sweep across a sample of the (id, pos) space; every encoded value must be
        // strictly increasing as we walk the lexicographic order of (id, pos), and never negative.
        // This is the property that lets ESQL's TopN signed-long ordering be a stable tiebreaker.
        long previous = -1L;
        int[] ids = new int[] { 0, 1, 2, 100, SourceExtractors.MAX_EXTRACTOR_ID };
        long[] positions = new long[] { 0L, 1L, 100L, 1L << 30, SourceExtractors.MAX_LOCAL_POSITION };
        for (int id : ids) {
            for (long pos : positions) {
                long encoded = SourceExtractors.encode(id, pos);
                assertThat("encoded must be non-negative", encoded, org.hamcrest.Matchers.greaterThanOrEqualTo(0L));
                assertThat("encoded must be strictly monotonic in (id, pos)", encoded, org.hamcrest.Matchers.greaterThan(previous));
                previous = encoded;
            }
        }
    }

    public void testRegisterAssignsSequentialIds() {
        try (SourceExtractors registry = new SourceExtractors()) {
            int id0 = registry.register(new IntListExtractor(new int[] { 1, 2, 3 }));
            int id1 = registry.register(new IntListExtractor(new int[] { 4, 5 }));
            int id2 = registry.register(new IntListExtractor(new int[] { 6 }));
            assertEquals(0, id0);
            assertEquals(1, id1);
            assertEquals(2, id2);
            assertEquals(3, registry.size());
            assertNotNull(registry.get(0));
            assertNotNull(registry.get(1));
            assertNotNull(registry.get(2));
        }
    }

    public void testRegisterRejectsNullAndAfterClose() {
        SourceExtractors registry = new SourceExtractors();
        expectThrows(IllegalArgumentException.class, () -> registry.register(null));
        registry.close();
        expectThrows(IllegalStateException.class, () -> registry.register(new IntListExtractor(new int[] { 1 })));
    }

    public void testGetRejectsOutOfRange() {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1 }));
            expectThrows(IllegalArgumentException.class, () -> registry.get(-1));
            expectThrows(IllegalArgumentException.class, () -> registry.get(1));
        }
    }

    public void testCloseIsIdempotent() {
        SourceExtractors registry = new SourceExtractors();
        IntListExtractor a = new IntListExtractor(new int[] { 1, 2 });
        IntListExtractor b = new IntListExtractor(new int[] { 3 });
        registry.register(a);
        registry.register(b);
        registry.close();
        assertTrue(a.closed());
        assertTrue(b.closed());
        // Second close must be a no-op (no double-close on extractors).
        registry.close();
        assertEquals("each extractor closed exactly once", 1, a.closeCount());
        assertEquals("each extractor closed exactly once", 1, b.closeCount());
    }

    public void testTrailingCloseablesRunAfterExtractors() {
        // The trailing-closeable contract is what keeps the per-query concurrency budget alive
        // for late materialization: the registry must close the extractors first (they may use
        // the trailing resource on the way down), then close the trailing resources LIFO.
        SourceExtractors registry = new SourceExtractors();
        StringBuilder order = new StringBuilder();
        IntListExtractor a = new IntListExtractor(new int[] { 1 }) {
            @Override
            public void close() {
                order.append("extractor;");
                super.close();
            }
        };
        registry.register(a);
        registry.registerTrailingCloseable(() -> order.append("trailing-1;"));
        registry.registerTrailingCloseable(() -> order.append("trailing-2;"));
        registry.close();
        // Extractors first, then LIFO over the two trailing closeables.
        assertEquals("extractor;trailing-2;trailing-1;", order.toString());
    }

    public void testTrailingCloseableRegisteredAfterCloseFiresImmediately() {
        // If the registry has already been closed, late attachers must still see the closeable
        // run — otherwise we'd leak the per-query budget when a registry was closed before any
        // extractor was registered.
        SourceExtractors registry = new SourceExtractors();
        registry.close();
        AtomicInteger calls = new AtomicInteger();
        registry.registerTrailingCloseable(() -> calls.incrementAndGet());
        assertEquals(1, calls.get());
    }

    public void testCloseIsIdempotentWithTrailingCloseables() {
        SourceExtractors registry = new SourceExtractors();
        AtomicInteger calls = new AtomicInteger();
        registry.registerTrailingCloseable(() -> calls.incrementAndGet());
        registry.close();
        registry.close();
        assertEquals("trailing closeable must run exactly once", 1, calls.get());
    }

    public void testMaterializeEmptyCount() throws IOException {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1, 2 }));
            Block[] result = registry.materialize(new long[0], 0, List.of("col"), null, blockFactory);
            try {
                assertEquals(1, result.length);
                assertEquals(0, result[0].getPositionCount());
            } finally {
                org.elasticsearch.core.Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeSingleSource() throws IOException {
        try (SourceExtractors registry = new SourceExtractors()) {
            int id = registry.register(new IntListExtractor(new int[] { 10, 20, 30, 40 }));
            // Request positions 3, 0, 2 — out of order and disjoint.
            long[] refs = new long[] { SourceExtractors.encode(id, 3), SourceExtractors.encode(id, 0), SourceExtractors.encode(id, 2) };
            Block[] result = registry.materialize(refs, refs.length, List.of("col"), null, blockFactory);
            try {
                assertEquals(1, result.length);
                IntBlock block = (IntBlock) result[0];
                assertEquals(3, block.getPositionCount());
                assertEquals(40, block.getInt(0));
                assertEquals(10, block.getInt(1));
                assertEquals(30, block.getInt(2));
            } finally {
                org.elasticsearch.core.Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeMultiSourceInterleaved() throws IOException {
        try (SourceExtractors registry = new SourceExtractors()) {
            int idA = registry.register(new IntListExtractor(new int[] { 100, 101, 102 }));
            int idB = registry.register(new IntListExtractor(new int[] { 200, 201 }));
            int idC = registry.register(new IntListExtractor(new int[] { 300, 301, 302, 303 }));
            // Interleaved: B[1], A[2], C[0], A[0], C[3], B[0]
            long[] refs = new long[] {
                SourceExtractors.encode(idB, 1),
                SourceExtractors.encode(idA, 2),
                SourceExtractors.encode(idC, 0),
                SourceExtractors.encode(idA, 0),
                SourceExtractors.encode(idC, 3),
                SourceExtractors.encode(idB, 0) };
            Block[] result = registry.materialize(refs, refs.length, List.of("col"), null, blockFactory);
            try {
                IntBlock block = (IntBlock) result[0];
                assertEquals(6, block.getPositionCount());
                assertEquals(201, block.getInt(0));
                assertEquals(102, block.getInt(1));
                assertEquals(300, block.getInt(2));
                assertEquals(100, block.getInt(3));
                assertEquals(303, block.getInt(4));
                assertEquals(200, block.getInt(5));
            } finally {
                org.elasticsearch.core.Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeBatchesPerExtractor() throws IOException {
        // The dispatch must call extract once per id per materialize() call — proving the per-id
        // batching is real, not row-by-row, and that all requested columns flow through a single
        // extract invocation so the implementation can coalesce I/O across columns (F-2).
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger maxColumnsPerCall = new AtomicInteger();
        IntListExtractor a = new IntListExtractor(new int[] { 1, 2, 3, 4, 5 }) {
            @Override
            public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory factory)
                throws IOException {
                calls.incrementAndGet();
                maxColumnsPerCall.accumulateAndGet(columnNames.length, Math::max);
                return super.extract(columnNames, targetTypes, localPositions, factory);
            }
        };
        try (SourceExtractors registry = new SourceExtractors()) {
            int id = registry.register(a);
            long[] refs = new long[] {
                SourceExtractors.encode(id, 0),
                SourceExtractors.encode(id, 4),
                SourceExtractors.encode(id, 2),
                SourceExtractors.encode(id, 1),
                SourceExtractors.encode(id, 3) };
            Block[] result = registry.materialize(refs, refs.length, List.of("col"), null, blockFactory);
            try {
                IntBlock block = (IntBlock) result[0];
                assertEquals(5, block.getPositionCount());
                assertEquals(1, block.getInt(0));
                assertEquals(5, block.getInt(1));
                assertEquals(3, block.getInt(2));
                assertEquals(2, block.getInt(3));
                assertEquals(4, block.getInt(4));
            } finally {
                org.elasticsearch.core.Releasables.closeExpectNoException(result);
            }
        }
        assertEquals("one extract() call for the single-id batch", 1, calls.get());
        assertEquals("single-column materialize must pass exactly one column to extract", 1, maxColumnsPerCall.get());
    }

    public void testMaterializeMultipleColumnsBatchesAllColumnsPerExtractor() throws IOException {
        // F-2 invariant: with N requested columns and K extractor ids, materialize() must invoke
        // each extractor's multi-column extract exactly once (passing all N columns), so the
        // implementation can issue a single coalesced I/O batch covering all N columns. A
        // regression that splits extract into N per-column calls would fail this assertion.
        AtomicInteger callsA = new AtomicInteger();
        AtomicInteger maxColsA = new AtomicInteger();
        TwoColumnExtractor a = new TwoColumnExtractor(new int[] { 1, 2, 3 }, new long[] { 10L, 20L, 30L }) {
            @Override
            public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory factory)
                throws IOException {
                callsA.incrementAndGet();
                maxColsA.accumulateAndGet(columnNames.length, Math::max);
                return super.extract(columnNames, targetTypes, localPositions, factory);
            }
        };
        try (SourceExtractors registry = new SourceExtractors()) {
            int id = registry.register(a);
            long[] refs = new long[] { SourceExtractors.encode(id, 0), SourceExtractors.encode(id, 2), SourceExtractors.encode(id, 1) };
            Block[] result = registry.materialize(refs, refs.length, List.of("ints", "longs"), null, blockFactory);
            try {
                IntBlock ints = (IntBlock) result[0];
                LongBlock longs = (LongBlock) result[1];
                assertEquals(3, ints.getPositionCount());
                assertEquals(1, ints.getInt(0));
                assertEquals(3, ints.getInt(1));
                assertEquals(2, ints.getInt(2));
                assertEquals(10L, longs.getLong(0));
                assertEquals(30L, longs.getLong(1));
                assertEquals(20L, longs.getLong(2));
            } finally {
                org.elasticsearch.core.Releasables.closeExpectNoException(result);
            }
        }
        assertEquals("exactly one extract() call per extractor regardless of column count", 1, callsA.get());
        assertEquals("multi-column materialize must pass both columns in a single extract() call", 2, maxColsA.get());
    }

    public void testMaterializeMultipleColumns() throws IOException {
        try (SourceExtractors registry = new SourceExtractors()) {
            int idA = registry.register(new TwoColumnExtractor(new int[] { 1, 2 }, new long[] { 10L, 20L }));
            int idB = registry.register(new TwoColumnExtractor(new int[] { 3, 4 }, new long[] { 30L, 40L }));
            long[] refs = new long[] { SourceExtractors.encode(idA, 0), SourceExtractors.encode(idB, 1), SourceExtractors.encode(idA, 1) };
            Block[] result = registry.materialize(refs, refs.length, List.of("ints", "longs"), null, blockFactory);
            try {
                IntBlock ints = (IntBlock) result[0];
                LongBlock longs = (LongBlock) result[1];
                assertEquals(3, ints.getPositionCount());
                assertEquals(3, longs.getPositionCount());
                assertEquals(1, ints.getInt(0));
                assertEquals(4, ints.getInt(1));
                assertEquals(2, ints.getInt(2));
                assertEquals(10L, longs.getLong(0));
                assertEquals(40L, longs.getLong(1));
                assertEquals(20L, longs.getLong(2));
            } finally {
                org.elasticsearch.core.Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeNullThenTypedWithDeclaredType() throws IOException {
        // Lowest extractor id emits ConstantNullBlock; a later id emits values. Pre-fix Phase 5
        // picked ElementType.NULL from the first Java-non-null per-id block and threw
        // "can't append non-null values to a null block" on copyFrom of the typed id.
        try (SourceExtractors registry = new SourceExtractors()) {
            int idNull = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { null, null })));
            int idTyped = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { 10, 20 })));
            long[] refs = new long[] {
                SourceExtractors.encode(idNull, 0),
                SourceExtractors.encode(idTyped, 0),
                SourceExtractors.encode(idNull, 1),
                SourceExtractors.encode(idTyped, 1) };
            Block[] result = registry.materialize(refs, refs.length, List.of("col"), List.of(DataType.INTEGER), blockFactory);
            try {
                assertEquals(ElementType.INT, result[0].elementType());
                IntBlock block = (IntBlock) result[0];
                assertEquals(4, block.getPositionCount());
                assertTrue(block.isNull(0));
                assertEquals(10, block.getInt(1));
                assertTrue(block.isNull(2));
                assertEquals(20, block.getInt(3));
            } finally {
                Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeNullThenTypedWithoutTargetTypes() throws IOException {
        // Same mixed-nullness as the declared-type regression, but {@code targetTypes == null}
        // so Phase 5 takes the scan fallback. That is the BlockChunks analog: skip
        // {@code ElementType.NULL} and build from the first typed per-id block.
        try (SourceExtractors registry = new SourceExtractors()) {
            int idNull = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { null, null })));
            int idTyped = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { 10, 20 })));
            long[] refs = new long[] {
                SourceExtractors.encode(idNull, 0),
                SourceExtractors.encode(idTyped, 0),
                SourceExtractors.encode(idNull, 1),
                SourceExtractors.encode(idTyped, 1) };
            Block[] result = registry.materialize(refs, refs.length, List.of("col"), null, blockFactory);
            try {
                assertEquals(ElementType.INT, result[0].elementType());
                IntBlock block = (IntBlock) result[0];
                assertEquals(4, block.getPositionCount());
                assertTrue(block.isNull(0));
                assertEquals(10, block.getInt(1));
                assertTrue(block.isNull(2));
                assertEquals(20, block.getInt(3));
            } finally {
                Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeTypedThenNullWithDeclaredType() throws IOException {
        try (SourceExtractors registry = new SourceExtractors()) {
            int idTyped = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { 10, 20 })));
            int idNull = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { null, null })));
            long[] refs = new long[] {
                SourceExtractors.encode(idTyped, 0),
                SourceExtractors.encode(idNull, 0),
                SourceExtractors.encode(idTyped, 1),
                SourceExtractors.encode(idNull, 1) };
            Block[] result = registry.materialize(refs, refs.length, List.of("col"), List.of(DataType.INTEGER), blockFactory);
            try {
                assertEquals(ElementType.INT, result[0].elementType());
                IntBlock block = (IntBlock) result[0];
                assertEquals(4, block.getPositionCount());
                assertEquals(10, block.getInt(0));
                assertTrue(block.isNull(1));
                assertEquals(20, block.getInt(2));
                assertTrue(block.isNull(3));
            } finally {
                Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeAllNullWithDeclaredTypeIsConstantNull() throws IOException {
        // All per-id blocks are ConstantNullBlock: keep that, even with a declared type. A typed
        // builder of size {@code count} would only copy nulls.
        try (SourceExtractors registry = new SourceExtractors()) {
            int idA = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { null, null })));
            int idB = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { null })));
            long[] refs = new long[] { SourceExtractors.encode(idA, 0), SourceExtractors.encode(idB, 0), SourceExtractors.encode(idA, 1) };
            Block[] result = registry.materialize(refs, refs.length, List.of("col"), List.of(DataType.INTEGER), blockFactory);
            try {
                assertEquals(ElementType.NULL, result[0].elementType());
                assertTrue(result[0].areAllValuesNull());
                assertEquals(3, result[0].getPositionCount());
                assertTrue(result[0].isNull(0));
                assertTrue(result[0].isNull(1));
                assertTrue(result[0].isNull(2));
            } finally {
                Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeAllNullWithoutTargetTypesIsConstantNull() throws IOException {
        // Null targetTypes disables declared-type coercion; all-null per-id blocks fall through
        // to ConstantNullBlock, matching the BlockChunks all-null arm.
        try (SourceExtractors registry = new SourceExtractors()) {
            int idA = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { null, null })));
            int idB = registry.register(new NullableIntColumnsExtractor(Map.of("col", new Integer[] { null })));
            long[] refs = new long[] { SourceExtractors.encode(idA, 0), SourceExtractors.encode(idB, 0) };
            Block[] result = registry.materialize(refs, refs.length, List.of("col"), null, blockFactory);
            try {
                assertEquals(ElementType.NULL, result[0].elementType());
                assertTrue(result[0].areAllValuesNull());
                assertEquals(2, result[0].getPositionCount());
            } finally {
                Releasables.closeExpectNoException(result);
            }
        }
    }

    public void testMaterializeMixedNullnessOnOneOfTwoColumns() throws IOException {
        // Per-column builder selection: {@code ints} is dense on both ids; only {@code flag} is
        // mixed (ConstantNullBlock then IntBlock). The dense column must stay typed even if the
        // mixed column used to poison a null builder.
        try (SourceExtractors registry = new SourceExtractors()) {
            int idNullFlag = registry.register(
                new NullableIntColumnsExtractor(Map.of("ints", new Integer[] { 1, 2 }, "flag", new Integer[] { null, null }))
            );
            int idTypedFlag = registry.register(
                new NullableIntColumnsExtractor(Map.of("ints", new Integer[] { 3, 4 }, "flag", new Integer[] { 100, 200 }))
            );
            long[] refs = new long[] {
                SourceExtractors.encode(idNullFlag, 0),
                SourceExtractors.encode(idTypedFlag, 1),
                SourceExtractors.encode(idNullFlag, 1) };
            Block[] result = registry.materialize(
                refs,
                refs.length,
                List.of("ints", "flag"),
                List.of(DataType.INTEGER, DataType.INTEGER),
                blockFactory
            );
            try {
                assertEquals(ElementType.INT, result[0].elementType());
                assertEquals(ElementType.INT, result[1].elementType());
                IntBlock ints = (IntBlock) result[0];
                IntBlock flag = (IntBlock) result[1];
                assertEquals(3, ints.getPositionCount());
                assertEquals(1, ints.getInt(0));
                assertEquals(4, ints.getInt(1));
                assertEquals(2, ints.getInt(2));
                assertTrue(flag.isNull(0));
                assertEquals(200, flag.getInt(1));
                assertTrue(flag.isNull(2));
            } finally {
                Releasables.closeExpectNoException(result);
            }
        }
    }

    /**
     * {@link SourceExtractors#materialize} must rethrow the extractor's {@link IOException} as the
     * same instance. Wrapping it (for example in {@code UncheckedIOException}) would hide the
     * checked type from {@code ExternalFailures.classify} at the operator boundary.
     */
    public void testMaterializeRethrowsExtractIoFailure() {
        IOException failure = new IOException("simulated extract I/O failure");
        try (SourceExtractors registry = new SourceExtractors()) {
            int successfulId = registry.register(new IntListExtractor(new int[] { 10 }));
            int failingId = registry.register(new IntListExtractor(new int[] { 20 }) {
                @Override
                public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory factory)
                    throws IOException {
                    throw failure;
                }
            });
            long[] refs = new long[] { SourceExtractors.encode(successfulId, 0), SourceExtractors.encode(failingId, 0) };
            IOException thrown = expectThrows(
                IOException.class,
                () -> registry.materialize(refs, refs.length, List.of("col"), null, blockFactory)
            );
            assertSame(failure, thrown);
        }
    }

    public void testMaterializeRejectsUnknownExtractorId() {
        try (SourceExtractors registry = new SourceExtractors()) {
            registry.register(new IntListExtractor(new int[] { 1, 2 }));
            long[] refs = new long[] { SourceExtractors.encode(7, 0) };
            expectThrows(IllegalArgumentException.class, () -> registry.materialize(refs, refs.length, List.of("col"), null, blockFactory));
        }
    }

    public void testMaterializeAfterCloseFails() {
        SourceExtractors registry = new SourceExtractors();
        registry.register(new IntListExtractor(new int[] { 1 }));
        registry.close();
        long[] refs = new long[] { SourceExtractors.encode(0, 0) };
        expectThrows(IllegalStateException.class, () -> registry.materialize(refs, refs.length, List.of("col"), null, blockFactory));
    }

    public void testRegisterIdSpaceExhaustion() {
        // Walk past MAX_EXTRACTOR_ID by registering one stub per slot. ArrayList growth is the
        // dominant cost; 32 768 small objects is well within unit-test budgets.
        SourceExtractors registry = new SourceExtractors();
        try {
            // Simulate by directly registering near the cap. Use the same lightweight stub each
            // time; ArrayList growth is the only real cost.
            for (int i = 0; i <= SourceExtractors.MAX_EXTRACTOR_ID; i++) {
                registry.register(new IntListExtractor(new int[0]));
            }
            assertEquals(SourceExtractors.MAX_EXTRACTOR_ID + 1, registry.size());
            expectThrows(IllegalStateException.class, () -> registry.register(new IntListExtractor(new int[0])));
        } finally {
            registry.close();
        }
    }

    /**
     * Minimal {@link ColumnExtractor} that materialises a single int column from an in-memory
     * array. Used as a stand-in for parquet/csv-backed extractors in tests that focus on the
     * registry plumbing.
     */
    private static class IntListExtractor implements ColumnExtractor {
        private final int[] values;
        private final AtomicInteger closeCount = new AtomicInteger();

        IntListExtractor(int[] values) {
            this.values = values;
        }

        @Override
        public long rowCount() {
            return values.length;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory blockFactory)
            throws IOException {
            Block[] result = new Block[columnNames.length];
            boolean built = false;
            try {
                for (int c = 0; c < columnNames.length; c++) {
                    result[c] = extractOne(columnNames[c], localPositions, blockFactory);
                }
                built = true;
                return result;
            } finally {
                if (built == false) Releasables.closeExpectNoException(result);
            }
        }

        Block extractOne(String columnName, long[] localPositions, BlockFactory blockFactory) {
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(localPositions.length)) {
                for (long pos : localPositions) {
                    builder.appendInt(values[Math.toIntExact(pos)]);
                }
                return builder.build();
            }
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        boolean closed() {
            return closeCount.get() > 0;
        }

        int closeCount() {
            return closeCount.get();
        }
    }

    /**
     * Two-column extractor: {@code ints} and {@code longs}. Used to assert that materialize fans
     * out per-(column, id) and reassembles output blocks in caller order without mixing values
     * across columns.
     */
    private static class TwoColumnExtractor implements ColumnExtractor {
        private final int[] ints;
        private final long[] longs;

        TwoColumnExtractor(int[] ints, long[] longs) {
            assert ints.length == longs.length;
            this.ints = ints;
            this.longs = longs;
        }

        @Override
        public long rowCount() {
            return ints.length;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory blockFactory)
            throws IOException {
            Block[] result = new Block[columnNames.length];
            boolean built = false;
            try {
                for (int c = 0; c < columnNames.length; c++) {
                    result[c] = extractOne(columnNames[c], localPositions, blockFactory);
                }
                built = true;
                return result;
            } finally {
                if (built == false) Releasables.closeExpectNoException(result);
            }
        }

        private Block extractOne(String columnName, long[] localPositions, BlockFactory blockFactory) {
            switch (columnName) {
                case "ints" -> {
                    try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(localPositions.length)) {
                        for (long pos : localPositions) {
                            b.appendInt(ints[Math.toIntExact(pos)]);
                        }
                        return b.build();
                    }
                }
                case "longs" -> {
                    try (LongBlock.Builder b = blockFactory.newLongBlockBuilder(localPositions.length)) {
                        for (long pos : localPositions) {
                            b.appendLong(longs[Math.toIntExact(pos)]);
                        }
                        return b.build();
                    }
                }
                default -> throw new IllegalArgumentException("unknown column: " + columnName);
            }
        }

        @Override
        public void close() {}
    }

    /**
     * Per-column int extractor that emits {@code ConstantNullBlock} when every requested cell is
     * null, otherwise an {@code IntBlock}. Ignores {@code targetTypes} on purpose: parquet still
     * returns {@code ConstantNullBlock} for an all-null optional INT even when the planner type is
     * {@code INTEGER} (identity coerce is skipped). Used to reproduce Phase 5 mixed-nullness
     * across extractor ids (all-null first split vs typed later split).
     */
    private static class NullableIntColumnsExtractor implements ColumnExtractor {
        private final Map<String, Integer[]> columns;

        NullableIntColumnsExtractor(Map<String, Integer[]> columns) {
            this.columns = columns;
        }

        @Override
        public long rowCount() {
            return columns.values().iterator().next().length;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory blockFactory)
            throws IOException {
            Block[] result = new Block[columnNames.length];
            boolean built = false;
            try {
                for (int c = 0; c < columnNames.length; c++) {
                    result[c] = extractOne(columnNames[c], localPositions, blockFactory);
                }
                built = true;
                return result;
            } finally {
                if (built == false) Releasables.closeExpectNoException(result);
            }
        }

        private Block extractOne(String columnName, long[] localPositions, BlockFactory blockFactory) {
            Integer[] values = columns.get(columnName);
            if (values == null) {
                throw new IllegalArgumentException("unknown column: " + columnName);
            }
            boolean anyValue = false;
            for (long pos : localPositions) {
                if (values[Math.toIntExact(pos)] != null) {
                    anyValue = true;
                    break;
                }
            }
            if (anyValue == false) {
                return blockFactory.newConstantNullBlock(localPositions.length);
            }
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(localPositions.length)) {
                for (long pos : localPositions) {
                    Integer v = values[Math.toIntExact(pos)];
                    if (v == null) {
                        builder.appendNull();
                    } else {
                        builder.appendInt(v);
                    }
                }
                return builder.build();
            }
        }

        @Override
        public void close() {}
    }
}
