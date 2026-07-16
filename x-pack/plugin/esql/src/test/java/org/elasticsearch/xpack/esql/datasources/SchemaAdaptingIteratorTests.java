/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SchemaAdaptingIteratorTests extends ESTestCase {

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    public void testIdentityPassThrough() {
        List<Attribute> schema = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, null);

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(42, 3);
        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("hello"), 3);
        Page inputPage = new Page(3, new Block[] { aBlock, bBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), schema, mapping, blockFactory)) {
            assertTrue(iter.hasNext());
            Page result = iter.next();
            assertThat(result.getPositionCount(), equalTo(3));
            assertThat(result.getBlockCount(), equalTo(2));

            IntBlock resultA = result.getBlock(0);
            assertThat(resultA.getInt(0), equalTo(42));

            assertFalse(iter.hasNext());
        }
    }

    public void testColumnReorder() {
        List<Attribute> unified = List.of(attr("b", DataType.KEYWORD), attr("a", DataType.INTEGER));
        ColumnMapping mapping = new ColumnMapping(new int[] { 1, 0 }, null);

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(10, 2);
        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("x"), 2);
        Page inputPage = new Page(2, new Block[] { aBlock, bBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(2));

            IntBlock resultA = result.getBlock(1);
            assertThat(resultA.getInt(0), equalTo(10));
        }
    }

    public void testMissingColumnNullFill() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER), attr("missing", DataType.LONG), attr("b", DataType.KEYWORD));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, -1, 1 }, null);

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(1, 4);
        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("v"), 4);
        Page inputPage = new Page(4, new Block[] { aBlock, bBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(3));
            assertThat(result.getPositionCount(), equalTo(4));

            Block nullBlock = result.getBlock(1);
            assertTrue(nullBlock.isNull(0));
            assertTrue(nullBlock.isNull(3));
        }
    }

    public void testCastIntToLong() {
        List<Attribute> unified = List.of(attr("val", DataType.LONG));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.LONG });

        IntBlock intBlock = blockFactory.newConstantIntBlockWith(123, 2);
        Page inputPage = new Page(2, new Block[] { intBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            LongBlock longBlock = result.getBlock(0);
            assertThat(longBlock.getLong(0), equalTo(123L));
            assertThat(longBlock.getLong(1), equalTo(123L));
        }
    }

    public void testCastIntToDouble() {
        List<Attribute> unified = List.of(attr("val", DataType.DOUBLE));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.DOUBLE });

        IntBlock intBlock = blockFactory.newConstantIntBlockWith(42, 3);
        Page inputPage = new Page(3, new Block[] { intBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            DoubleBlock doubleBlock = result.getBlock(0);
            assertThat(doubleBlock.getDouble(0), equalTo(42.0));
            assertThat(doubleBlock.getDouble(2), equalTo(42.0));
        }
    }

    public void testCastDatetimeToDateNanos() {
        List<Attribute> unified = List.of(attr("ts", DataType.DATE_NANOS));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.DATE_NANOS });

        long millisValue = 1711800000000L;
        LongBlock datetimeBlock = blockFactory.newConstantLongBlockWith(millisValue, 2);
        Page inputPage = new Page(2, new Block[] { datetimeBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            LongBlock nanosBlock = result.getBlock(0);
            assertThat(nanosBlock.getLong(0), equalTo(millisValue * 1_000_000L));
            assertThat(nanosBlock.getLong(1), equalTo(millisValue * 1_000_000L));
        }
    }

    // --- UNION_BY_NAME explicit-scenario tests ---
    //
    // In UBN, the planner builds a unified schema across all files (e.g. [a, b, c]) and each file
    // has its own local physical schema (e.g. [a, b]). The reader honors the file-local schema
    // verbatim and emits a page in that local shape; the SchemaAdaptingIterator then applies the
    // ColumnMapping recipe to produce a page in the unified shape. The three UBN-specific recipe
    // operations are: null-fill for columns missing from the file, reorder when the file's column
    // order differs from unified, and widening cast when the file's local type is narrower than
    // unified. The tests below name each scenario explicitly, plus one combined scenario.

    /**
     * UBN scenario — missing column. File-local schema is [a, b]; unified is [a, b, c].
     * ColumnMapping marks c as missing (localIndex = -1). Adapter null-fills c.
     */
    public void testUnionByNameMissingColumn() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD), attr("c", DataType.LONG));
        // Reader emits a page in file-local layout [a, b]; mapping says c is missing.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1, -1 }, null);

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(7, 3);
        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("hello"), 3);
        Page inputPage = new Page(3, new Block[] { aBlock, bBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(3));
            assertThat(result.getPositionCount(), equalTo(3));

            IntBlock resultA = result.getBlock(0);
            assertThat(resultA.getInt(0), equalTo(7));
            Block resultC = result.getBlock(2);
            for (int i = 0; i < 3; i++) {
                assertTrue("missing column c must be null at position " + i, resultC.isNull(i));
            }
        }
    }

    /**
     * UBN scenario — reordered columns. File-local schema is [b, a]; unified is [a, b].
     * ColumnMapping says a is at local index 1, b is at local index 0.
     */
    public void testUnionByNameReorderedColumns() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));
        // File emits [b, a]; mapping reorders to [a, b] for unified output.
        ColumnMapping mapping = new ColumnMapping(new int[] { 1, 0 }, null);

        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("greetings"), 2);
        IntBlock aBlock = blockFactory.newConstantIntBlockWith(99, 2);
        Page inputPage = new Page(2, new Block[] { bBlock, aBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(2));
            IntBlock resultA = result.getBlock(0);
            assertThat(resultA.getInt(0), equalTo(99));
            assertThat(resultA.getInt(1), equalTo(99));
        }
    }

    /**
     * UBN scenario — widening cast. File-local has a:INTEGER; unified has a:LONG.
     * ColumnMapping casts INT → LONG.
     */
    public void testUnionByNameWideningIntToLong() {
        List<Attribute> unified = List.of(attr("a", DataType.LONG));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, new DataType[] { DataType.LONG });

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(2_000_000_001, 2);
        Page inputPage = new Page(2, new Block[] { aBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            LongBlock longA = result.getBlock(0);
            assertThat(longA.getLong(0), equalTo(2_000_000_001L));
            assertThat(longA.getLong(1), equalTo(2_000_000_001L));
        }
    }

    /**
     * UBN scenario — all three recipe operations in one file. File-local schema is [b, a_int];
     * unified is [a:LONG, b:KEYWORD, c:KEYWORD]. Mapping reorders (a at local 1, b at local 0),
     * widens (a INT → LONG), and null-fills (c missing).
     */
    public void testUnionByNameMixedReorderMissingAndWidening() {
        List<Attribute> unified = List.of(attr("a", DataType.LONG), attr("b", DataType.KEYWORD), attr("c", DataType.KEYWORD));
        // unified[0]=a → local 1 with cast to LONG; unified[1]=b → local 0, no cast; unified[2]=c → missing.
        ColumnMapping mapping = new ColumnMapping(new int[] { 1, 0, -1 }, new DataType[] { DataType.LONG, null, null });

        Block bBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("x"), 2);
        IntBlock aBlock = blockFactory.newConstantIntBlockWith(123_456, 2);
        Page inputPage = new Page(2, new Block[] { bBlock, aBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(3));
            assertThat(result.getPositionCount(), equalTo(2));

            // a widened from INT to LONG, reordered from local position 1 to unified position 0.
            LongBlock resultA = result.getBlock(0);
            assertThat(resultA.getLong(0), equalTo(123_456L));

            // b kept as KEYWORD, reordered from local position 0 to unified position 1.
            Block resultB = result.getBlock(1);
            assertFalse(resultB.isNull(0));

            // c missing from file, null-filled at unified position 2.
            Block resultC = result.getBlock(2);
            assertTrue(resultC.isNull(0));
            assertTrue(resultC.isNull(1));
        }
    }

    public void testEmptyPage() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER), attr("b", DataType.LONG));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, -1 }, null);

        IntBlock emptyBlock = blockFactory.newConstantIntBlockWith(0, 0);
        Page inputPage = new Page(0, new Block[] { emptyBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getPositionCount(), equalTo(0));
            assertThat(result.getBlockCount(), equalTo(2));
        }
    }

    public void testMemoryCleanupOnFailure() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER), attr("b", DataType.LONG));
        // VERSION is the cast target because this test only needs *a* failing cast: it pins that the blocks built
        // before the failure are released, not any particular pair's support status. VERSION is not a declarable
        // type and has no coercion arm, so it cannot drift into the supported set and quietly stop failing — which
        // is exactly what happened to the INTEGER -> DATE_NANOS pair this used to rely on.
        ColumnMapping mapping = new ColumnMapping(new int[] { 0, 1 }, new DataType[] { null, DataType.VERSION });

        IntBlock intBlock1 = blockFactory.newConstantIntBlockWith(1, 2);
        IntBlock intBlock2 = blockFactory.newConstantIntBlockWith(2, 2);
        Page inputPage = new Page(2, new Block[] { intBlock1, intBlock2 });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, blockFactory)) {
            RuntimeException e = expectThrows(RuntimeException.class, iter::next);
            assertThat(e.getMessage(), containsString("Failed to map page"));
        }
    }

    public void testCloseDelegation() {
        List<Attribute> unified = List.of(attr("a", DataType.INTEGER));
        ColumnMapping mapping = new ColumnMapping(new int[] { 0 }, null);

        AtomicBoolean closed = new AtomicBoolean(false);
        CloseableIterator<Page> delegate = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        SchemaAdaptingIterator iter = new SchemaAdaptingIterator(delegate, unified, mapping, blockFactory);
        assertFalse(closed.get());
        iter.close();
        assertTrue(closed.get());
    }

    /**
     * Mirrors production usage: full attributes include partition columns appended after
     * data columns, but only the data prefix is passed to SchemaAdaptingIterator (callers
     * derive the data-attribute view from their own attribute list, not from the mapping).
     */
    public void testDataColumnSubListWithPartitionSuffix() {
        List<Attribute> dataColumns = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        ColumnMapping mapping = new ColumnMapping(new int[] { 1, 0 }, null);

        IntBlock idBlock = blockFactory.newConstantIntBlockWith(7, 2);
        Block nameBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("Alice"), 2);
        Page inputPage = new Page(2, new Block[] { idBlock, nameBlock });

        List<Attribute> fullAttributes = List.of(
            attr("id", DataType.INTEGER),
            attr("name", DataType.KEYWORD),
            attr("year", DataType.INTEGER)
        );
        List<Attribute> subList = fullAttributes.subList(0, dataColumns.size());
        assertThat(subList.size(), equalTo(2));

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), subList, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(2));
            assertThat(result.getPositionCount(), equalTo(2));

            IntBlock resultId = result.getBlock(1);
            assertThat(resultId.getInt(0), equalTo(7));
        }
    }

    /**
     * Collision regression: a Hive partition key ({@code year}) shadows a same-named physical
     * column. On the data node the unified attributes are [id, value, year] with {@code year} the
     * appended partition column; the file-backed mapping is data-only (width 2). The factory must
     * pass the data-only view ({@code dataAttributesOf(attrs, {"year"})}, width 2) — not the full
     * width-3 attribute list — so the size-vs-width guard does not misfire. Pairs with
     * {@link #testConstructorRejectsMismatchedSchemaSize}, the negative case.
     */
    public void testCollisionDataOnlySchemaMatchesMappingWidth() {
        List<Attribute> fullAttributes = List.of(
            attr("id", DataType.INTEGER),
            attr("value", DataType.KEYWORD),
            attr("year", DataType.INTEGER) // partition column appended at tail; shadows a physical 'year'
        );
        // Mirrors AsyncExternalSourceOperatorFactory#queryDataSchema: data-only, partition excluded.
        List<Attribute> dataOnly = ExternalSchema.dataAttributesOf(fullAttributes, java.util.Set.of("year")).attributes();
        assertThat(dataOnly.size(), equalTo(2));

        // Non-identity mapping (reorder) so adaptSchema does not short-circuit; width matches data-only.
        ColumnMapping mapping = new ColumnMapping(new int[] { 1, 0 }, null);

        Block valueBlock = blockFactory.newConstantBytesRefBlockWith(new org.apache.lucene.util.BytesRef("alpha"), 2);
        IntBlock idBlock = blockFactory.newConstantIntBlockWith(7, 2);
        // File-natural order is [value, id]; mapping reorders to the unified [id, value].
        Page inputPage = new Page(2, new Block[] { valueBlock, idBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), dataOnly, mapping, blockFactory)) {
            Page result = iter.next();
            assertThat(result.getBlockCount(), equalTo(2));
            assertThat(result.getPositionCount(), equalTo(2));
            IntBlock resultId = result.getBlock(0);
            assertThat(resultId.getInt(0), equalTo(7));
        }
    }

    /**
     * Verifies the constructor rejects a schema whose size doesn't match the mapping's
     * column count. This guards against accidentally passing the full attributes list
     * (including partition columns) instead of just the data column prefix.
     */
    public void testConstructorRejectsMismatchedSchemaSize() {
        List<Attribute> threeColumnSchema = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD), attr("c", DataType.LONG));
        ColumnMapping twoColumnMapping = new ColumnMapping(new int[] { 0, 1 }, null);

        CloseableIterator<Page> emptyIter = new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Page next() {
                throw new NoSuchElementException();
            }

            @Override
            public void close() {}
        };

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new SchemaAdaptingIterator(emptyIter, threeColumnSchema, twoColumnMapping, blockFactory)
        );
        assertThat(ex.getMessage(), containsString("output schema size [3] does not match mapping width [2]"));
        assertThat(ex.getMessage(), containsString("partition columns"));
    }

    /**
     * Regression test for a latent thread-affinity bug. {@link SchemaAdaptingIterator} runs on
     * the producer side of {@link AsyncExternalSourceBuffer} (a generic-pool thread draining
     * pages from the format reader); the driver pins its own thread on the driver-local
     * {@link LocalCircuitBreaker} via {@link LocalCircuitBreaker#assertBeginRunLoop()}. If the
     * iterator were given the driver-local {@link BlockFactory}, its null-fill allocations
     * would trip {@link LocalCircuitBreaker#assertSingleThread()} (debug builds) and silently
     * corrupt the breaker's reserved-bytes accounting (production: assertions stripped).
     * <p>
     * The test pins a {@link LocalCircuitBreaker} to a sentinel "driver" thread, then runs
     * {@link SchemaAdaptingIterator#next()} from the test thread:
     * <ul>
     *   <li>Passing the driver-local factory must trip {@code assertSingleThread} — proving the
     *   pattern is dangerous and the test catches accidental regressions.</li>
     *   <li>Passing the root factory must succeed — proving the production wiring in
     *   {@link AsyncExternalSourceOperatorFactory#adaptSchema} is exactly what's needed.</li>
     * </ul>
     */
    public void testNullFillFromProducerThreadRequiresRootBlockFactory() throws Exception {
        assumeTrue("requires assertions enabled (-ea) to detect the producer-thread race", assertionsEnabled());

        // Tracking BigArrays (asserts no leaks at close), root factory shared across both cases.
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        CircuitBreaker rootBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory rootFactory = BlockFactory.builder(bigArrays).breaker(rootBreaker).build();
        LocalCircuitBreaker driverLocalBreaker = new LocalCircuitBreaker(rootBreaker, 0, 0);
        BlockFactory driverFactory = rootFactory.newChildFactory(driverLocalBreaker);

        // Pin the driver-local breaker to a "driver" thread that simulates Driver.run(). We
        // call assertBeginRunLoop on a short-lived worker; activeThread is set to that worker
        // and never cleared, so it stays != this test thread — exactly the production race
        // shape (consumer/driver thread != producer thread).
        Thread setup = new Thread(() -> assertTrue(driverLocalBreaker.assertBeginRunLoop()), "setup-pin-driver-breaker");
        setup.start();
        setup.join();

        try {
            // 1. Buggy wiring: driver-local factory on a thread != the pinned active thread must
            // trip assertSingleThread on the very first allocation (the null-fill).
            AssertionError ae = expectThrows(AssertionError.class, () -> runNullFillOnCurrentThread(rootFactory, driverFactory));
            assertThat(ae.getMessage(), containsString("Local breaker must be accessed by a single thread"));

            // 2. Fixed wiring: root factory has no thread affinity, so the same flow succeeds.
            Page result = runNullFillOnCurrentThread(rootFactory, rootFactory);
            try {
                assertThat(result.getBlockCount(), equalTo(2));
                assertThat(result.getPositionCount(), equalTo(4));
                Block nullBlock = result.getBlock(0);
                for (int i = 0; i < 4; i++) {
                    assertTrue("missing column must be null at position " + i, nullBlock.isNull(i));
                }
            } finally {
                result.releaseBlocks();
            }
        } finally {
            // assertEndRunLoop just clears activeThread; close() then sees null and is happy.
            assertTrue(driverLocalBreaker.assertEndRunLoop());
            driverLocalBreaker.close();
            assertThat("root breaker must reset to zero after release", rootBreaker.getUsed(), equalTo(0L));
        }
    }

    /**
     * Builds a fresh input page using {@code readerFactory} and runs the adapter's {@code
     * next()} on the current thread, with {@code adapterFactory} for null-fill. Re-throws any
     * {@link AssertionError} the iterator surfaces (including ones the iterator's own
     * {@code catch (Exception)} would not catch — they propagate naturally).
     */
    private static Page runNullFillOnCurrentThread(BlockFactory readerFactory, BlockFactory adapterFactory) {
        // File-local schema is [a]; unified is [missing, a]. The null-fill at unified index 0
        // must be allocated BEFORE the incRef at unified index 1, so when assertSingleThread
        // trips on the very first allocation no partial state has been built up — the outer
        // finally simply releases filePage and we exit cleanly with no ref-counted leak.
        List<Attribute> unified = List.of(attr("missing", DataType.LONG), attr("a", DataType.INTEGER));
        ColumnMapping mapping = new ColumnMapping(new int[] { -1, 0 }, null);

        IntBlock aBlock = readerFactory.newConstantIntBlockWith(1, 4);
        Page inputPage = new Page(4, new Block[] { aBlock });

        try (SchemaAdaptingIterator iter = new SchemaAdaptingIterator(singlePageIterator(inputPage), unified, mapping, adapterFactory)) {
            return iter.next();
        }
    }

    @SuppressWarnings("AssertWithSideEffects")
    private static boolean assertionsEnabled() {
        boolean enabled = false;
        assert enabled = true;
        return enabled;
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    private static CloseableIterator<Page> singlePageIterator(Page page) {
        return new CloseableIterator<>() {
            private boolean consumed = false;

            @Override
            public boolean hasNext() {
                return consumed == false;
            }

            @Override
            public Page next() {
                if (consumed) {
                    throw new NoSuchElementException();
                }
                consumed = true;
                return page;
            }

            @Override
            public void close() {}
        };
    }
}
