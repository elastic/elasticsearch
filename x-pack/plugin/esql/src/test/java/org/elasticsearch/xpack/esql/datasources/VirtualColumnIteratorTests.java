/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class VirtualColumnIteratorTests extends ESTestCase {

    private final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    public void testTwoDataTwoPartitionColumns() {
        List<Attribute> fullOutput = List.of(
            attr("emp_no", DataType.INTEGER),
            attr("name", DataType.KEYWORD),
            partAttr("year", DataType.INTEGER),
            partAttr("region", DataType.KEYWORD)
        );
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year", "region"));
        Map<String, Object> partitionValues = Map.of("year", 2024, "region", "us-east");

        VirtualColumnIterator it = newIterator(fullOutput, partitionCols, partitionValues);

        assertTrue(it.hasPartitionColumns());
        assertEquals(List.of("emp_no", "name"), it.dataColumnNames());

        IntBlock empNoBlock = blockFactory.newConstantIntBlockWith(42, 3);
        BytesRefBlock nameBlock = blockFactory.newConstantBytesRefBlockWith(new BytesRef("Alice"), 3);
        Page dataPage = new Page(3, new Block[] { empNoBlock, nameBlock });

        Page result = it.inject(dataPage);

        assertEquals(3, result.getPositionCount());
        assertEquals(4, result.getBlockCount());

        IntBlock resultEmpNo = result.getBlock(0);
        assertEquals(42, resultEmpNo.getInt(0));

        BytesRefBlock resultName = result.getBlock(1);
        assertEquals(new BytesRef("Alice"), resultName.getBytesRef(0, new BytesRef()));

        IntBlock resultYear = result.getBlock(2);
        assertEquals(2024, resultYear.getInt(0));
        assertEquals(2024, resultYear.getInt(2));

        BytesRefBlock resultRegion = result.getBlock(3);
        assertEquals(new BytesRef("us-east"), resultRegion.getBytesRef(0, new BytesRef()));
    }

    public void testAllBlockTypes() {
        List<Attribute> fullOutput = List.of(
            attr("data", DataType.INTEGER),
            partAttr("intCol", DataType.INTEGER),
            partAttr("longCol", DataType.LONG),
            partAttr("doubleCol", DataType.DOUBLE),
            partAttr("boolCol", DataType.BOOLEAN),
            partAttr("keywordCol", DataType.KEYWORD)
        );
        Set<String> partitionCols = new LinkedHashSet<>(List.of("intCol", "longCol", "doubleCol", "boolCol", "keywordCol"));
        Map<String, Object> partitionValues = Map.of(
            "intCol",
            42,
            "longCol",
            9999999999L,
            "doubleCol",
            3.14,
            "boolCol",
            true,
            "keywordCol",
            "hello"
        );

        VirtualColumnIterator it = newIterator(fullOutput, partitionCols, partitionValues);

        IntBlock dataBlock = blockFactory.newConstantIntBlockWith(1, 2);
        Page dataPage = new Page(2, new Block[] { dataBlock });

        Page result = it.inject(dataPage);

        assertEquals(2, result.getPositionCount());
        assertEquals(6, result.getBlockCount());

        IntBlock intResult = result.getBlock(1);
        assertEquals(42, intResult.getInt(0));

        LongBlock longResult = result.getBlock(2);
        assertEquals(9999999999L, longResult.getLong(0));

        DoubleBlock doubleResult = result.getBlock(3);
        assertEquals(3.14, doubleResult.getDouble(0), 0.001);

        BooleanBlock boolResult = result.getBlock(4);
        assertTrue(boolResult.getBoolean(0));

        BytesRefBlock keywordResult = result.getBlock(5);
        assertEquals(new BytesRef("hello"), keywordResult.getBytesRef(0, new BytesRef()));
    }

    public void testEmptyPageZeroPositions() {
        List<Attribute> fullOutput = List.of(attr("data", DataType.INTEGER), partAttr("year", DataType.INTEGER));
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year"));
        Map<String, Object> partitionValues = Map.of("year", 2024);

        VirtualColumnIterator it = newIterator(fullOutput, partitionCols, partitionValues);

        IntBlock emptyBlock = blockFactory.newConstantIntBlockWith(0, 0);
        Page dataPage = new Page(0, new Block[] { emptyBlock });

        Page result = it.inject(dataPage);

        assertEquals(0, result.getPositionCount());
        assertEquals(2, result.getBlockCount());
    }

    public void testNoPartitionColumnsReturnsPageUnchanged() {
        List<Attribute> fullOutput = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));
        Set<String> partitionCols = Set.of();

        VirtualColumnIterator it = newIterator(fullOutput, partitionCols, Map.of());

        assertFalse(it.hasPartitionColumns());

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(1, 2);
        BytesRefBlock bBlock = blockFactory.newConstantBytesRefBlockWith(new BytesRef("x"), 2);
        Page dataPage = new Page(2, new Block[] { aBlock, bBlock });

        Page result = it.inject(dataPage);
        assertSame(dataPage, result);
    }

    public void testColumnOrderingDataThenPartition() {
        List<Attribute> fullOutput = List.of(
            attr("a", DataType.INTEGER),
            attr("b", DataType.KEYWORD),
            partAttr("year", DataType.INTEGER),
            partAttr("month", DataType.INTEGER)
        );
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year", "month"));
        Map<String, Object> partitionValues = Map.of("year", 2024, "month", 6);

        VirtualColumnIterator it = newIterator(fullOutput, partitionCols, partitionValues);

        assertEquals(List.of("a", "b"), it.dataColumnNames());

        IntBlock aBlock = blockFactory.newConstantIntBlockWith(10, 1);
        BytesRefBlock bBlock = blockFactory.newConstantBytesRefBlockWith(new BytesRef("test"), 1);
        Page dataPage = new Page(1, new Block[] { aBlock, bBlock });

        Page result = it.inject(dataPage);

        assertEquals(4, result.getBlockCount());
        IntBlock yearBlock = result.getBlock(2);
        assertEquals(2024, yearBlock.getInt(0));
        IntBlock monthBlock = result.getBlock(3);
        assertEquals(6, monthBlock.getInt(0));
    }

    public void testConstantValuesAcrossAllPositions() {
        List<Attribute> fullOutput = List.of(attr("data", DataType.INTEGER), partAttr("year", DataType.INTEGER));
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year"));
        Map<String, Object> partitionValues = Map.of("year", 2024);

        VirtualColumnIterator it = newIterator(fullOutput, partitionCols, partitionValues);

        IntBlock dataBlock = blockFactory.newConstantIntBlockWith(1, 5);
        Page dataPage = new Page(5, new Block[] { dataBlock });

        Page result = it.inject(dataPage);

        IntBlock yearBlock = result.getBlock(1);
        for (int i = 0; i < 5; i++) {
            assertEquals(2024, yearBlock.getInt(i));
        }
    }

    /**
     * Regression: when the reader does not emit a {@code _rowPosition} column (CSV / NDJSON / ORC),
     * the iterator must synthesize {@code _id} from a per-file running counter rather than reading a
     * (non-existent) {@code _rowPosition} data block. The counter continues across pages of the
     * same file.
     */
    public void testIdRequiresRowPositionChannel() {
        // The split-local counter fallback was removed: every file reader now emits the _rowPosition
        // channel (the optimizer injects it for _id / _file.record_ref), and a split-local counter
        // would break _id repeatability across split layouts. When _id is requested but the reader
        // did not emit the channel, the iterator fails loud rather than synthesizing wrong ids.
        List<Attribute> fullOutput = List.of(attr("data", DataType.INTEGER), partAttr(ExternalMetadataColumns.ID, DataType.KEYWORD));
        Set<String> partitionCols = new LinkedHashSet<>(List.of(ExternalMetadataColumns.ID));
        BytesRef idPrefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"), 1700000000000L);

        Exception e = expectThrows(
            org.elasticsearch.xpack.esql.core.QlIllegalArgumentException.class,
            () -> new VirtualColumnIterator(emptyDelegate(), fullOutput, partitionCols, Map.of(), blockFactory, idPrefix)
        );
        assertThat(e.getMessage(), containsString("_rowPosition"));
    }

    /**
     * A declared {@code _id.path} stamps {@code _id} from the named data column's value. The id column rides the data
     * page as a normal data block (PruneColumns pins it into the reader projection); the iterator renders it to KEYWORD
     * into the {@code _id} slot. Even when the id column is NOT part of the user's output projection, it is present in
     * the data page here — the assertion checks the value is threaded row by row.
     */
    public void testIdFromColumnKeyword() {
        List<Attribute> fullOutput = List.of(
            attr("customer_id", DataType.KEYWORD),
            attr("amount", DataType.INTEGER),
            partAttr(ExternalMetadataColumns.ID, DataType.KEYWORD)
        );
        Set<String> partitionCols = new LinkedHashSet<>(List.of(ExternalMetadataColumns.ID));
        BytesRef idPrefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"), 1700000000000L);

        VirtualColumnIterator it = new VirtualColumnIterator(
            emptyDelegate(),
            fullOutput,
            partitionCols,
            Map.of(),
            blockFactory,
            idPrefix,
            "customer_id"
        );

        try (BytesRefBlock.Builder custBuilder = blockFactory.newBytesRefBlockBuilder(3)) {
            custBuilder.appendBytesRef(new BytesRef("c-1"));
            custBuilder.appendBytesRef(new BytesRef("c-2"));
            custBuilder.appendBytesRef(new BytesRef("c-3"));
            BytesRefBlock custBlock = custBuilder.build();
            IntBlock amountBlock = blockFactory.newConstantIntBlockWith(10, 3);
            Page dataPage = new Page(3, new Block[] { custBlock, amountBlock });

            Page result = it.inject(dataPage);

            assertEquals(3, result.getPositionCount());
            // fullOutput order: customer_id (0), amount (1), _id (2)
            assertEquals(3, result.getBlockCount());
            BytesRefBlock idResult = result.getBlock(2);
            assertEquals("c-1", asString(idResult, 0));
            assertEquals("c-2", asString(idResult, 1));
            assertEquals("c-3", asString(idResult, 2));
            // The id column itself is still emitted unchanged.
            BytesRefBlock custResult = result.getBlock(0);
            assertEquals("c-1", asString(custResult, 0));
            result.releaseBlocks();
        }
    }

    /** A declared {@code _id.path} column whose cell is null renders a null {@code _id} for that row. */
    public void testIdFromColumnNullCellIsNullId() {
        List<Attribute> fullOutput = List.of(attr("customer_id", DataType.KEYWORD), partAttr(ExternalMetadataColumns.ID, DataType.KEYWORD));
        Set<String> partitionCols = new LinkedHashSet<>(List.of(ExternalMetadataColumns.ID));
        BytesRef idPrefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"), 1700000000000L);

        VirtualColumnIterator it = new VirtualColumnIterator(
            emptyDelegate(),
            fullOutput,
            partitionCols,
            Map.of(),
            blockFactory,
            idPrefix,
            "customer_id"
        );

        try (BytesRefBlock.Builder custBuilder = blockFactory.newBytesRefBlockBuilder(2)) {
            custBuilder.appendBytesRef(new BytesRef("c-1"));
            custBuilder.appendNull();
            BytesRefBlock custBlock = custBuilder.build();
            Page dataPage = new Page(2, new Block[] { custBlock });

            Page result = it.inject(dataPage);

            BytesRefBlock idResult = result.getBlock(1);
            assertEquals("c-1", asString(idResult, 0));
            assertTrue(idResult.isNull(1));
            result.releaseBlocks();
        }
    }

    /**
     * A LONG {@code _id.path} column renders {@code _id} as the {@code TO_STRING} form of the value, so the stamped
     * {@code _id} reads identically to the column's own value.
     */
    public void testIdFromColumnLongRendersAsString() {
        List<Attribute> fullOutput = List.of(attr("order_id", DataType.LONG), partAttr(ExternalMetadataColumns.ID, DataType.KEYWORD));
        Set<String> partitionCols = new LinkedHashSet<>(List.of(ExternalMetadataColumns.ID));
        BytesRef idPrefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"), 1700000000000L);

        VirtualColumnIterator it = new VirtualColumnIterator(
            emptyDelegate(),
            fullOutput,
            partitionCols,
            Map.of(),
            blockFactory,
            idPrefix,
            "order_id"
        );

        LongBlock orderBlock = blockFactory.newConstantLongBlockWith(42L, 2);
        Page dataPage = new Page(2, new Block[] { orderBlock });

        Page result = it.inject(dataPage);

        BytesRefBlock idResult = result.getBlock(1);
        assertEquals("42", asString(idResult, 0));
        assertEquals("42", asString(idResult, 1));
        result.releaseBlocks();
    }

    /** A multi-valued {@code _id.path} cell is rejected — {@code _id} must be a single scalar value per row. */
    public void testIdFromColumnMultiValueRejected() {
        List<Attribute> fullOutput = List.of(attr("customer_id", DataType.KEYWORD), partAttr(ExternalMetadataColumns.ID, DataType.KEYWORD));
        Set<String> partitionCols = new LinkedHashSet<>(List.of(ExternalMetadataColumns.ID));
        BytesRef idPrefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"), 1700000000000L);

        VirtualColumnIterator it = new VirtualColumnIterator(
            emptyDelegate(),
            fullOutput,
            partitionCols,
            Map.of(),
            blockFactory,
            idPrefix,
            "customer_id"
        );

        try (BytesRefBlock.Builder custBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            custBuilder.beginPositionEntry();
            custBuilder.appendBytesRef(new BytesRef("c-1"));
            custBuilder.appendBytesRef(new BytesRef("c-2"));
            custBuilder.endPositionEntry();
            BytesRefBlock custBlock = custBuilder.build();
            Page dataPage = new Page(1, new Block[] { custBlock });

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> it.inject(dataPage));
            assertThat(e.getMessage(), containsString("customer_id"));
            assertThat(e.getMessage(), containsString("multi-valued"));
        }
    }

    /**
     * When a declared {@code _id.path} column is absent from the file's projection (the UBN carve-out), {@code _id}
     * renders as SQL NULL rather than throwing. Modeled by declaring an {@code idPath} that names no data column.
     */
    public void testIdFromColumnMissingColumnIsNullId() {
        List<Attribute> fullOutput = List.of(attr("amount", DataType.INTEGER), partAttr(ExternalMetadataColumns.ID, DataType.KEYWORD));
        Set<String> partitionCols = new LinkedHashSet<>(List.of(ExternalMetadataColumns.ID));
        BytesRef idPrefix = ExternalRowIdentity.prefix(StoragePath.of("s3://bucket/data.csv"), 1700000000000L);

        VirtualColumnIterator it = new VirtualColumnIterator(
            emptyDelegate(),
            fullOutput,
            partitionCols,
            Map.of(),
            blockFactory,
            idPrefix,
            "customer_id" // names no data column in fullOutput
        );

        IntBlock amountBlock = blockFactory.newConstantIntBlockWith(10, 2);
        Page dataPage = new Page(2, new Block[] { amountBlock });

        Page result = it.inject(dataPage);

        BytesRefBlock idResult = result.getBlock(1);
        assertTrue(idResult.isNull(0));
        assertTrue(idResult.isNull(1));
        result.releaseBlocks();
    }

    /**
     * Regression test for the ~44KB-per-query parquet circuit-breaker leak:
     * https://github.com/elastic/elasticsearch/issues/149393.
     * <p>
     * The Parquet reader, when handed an empty projection, falls back to reading the file's full
     * schema and emits pages with more blocks than the iterator's data-column projection. Without
     * a defensive surplus release in {@link VirtualColumnIterator#inject(Page)} those extra blocks
     * are silently dropped (their refcount never reaches zero) and their breaker bytes leak per
     * page. This test asserts the breaker resets to zero when the producer over-projects and the
     * iterator only references a strict subset of blocks.
     */
    public void testInjectReleasesSurplusBlocksFromOverProjectingProducer() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        CircuitBreaker rootBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory rootFactory = BlockFactory.builder(bigArrays).breaker(rootBreaker).build();

        // Iterator output is just the partition column; the producer will emit two surplus data
        // blocks the iterator never references. Mirrors the parquet "empty projection -> full
        // schema" fallback that drove the leak.
        List<Attribute> fullOutput = List.of(partAttr("year", DataType.INTEGER));
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year"));
        Map<String, Object> partitionValues = Map.of("year", 2024);
        VirtualColumnIterator it = new VirtualColumnIterator(
            new SinglePageIterator(new Page(0)),
            fullOutput,
            partitionCols,
            partitionValues,
            rootFactory
        );

        // Producer over-projects: page carries two extra blocks with non-trivial breaker bytes.
        IntBlock surplus1 = rootFactory.newConstantIntBlockWith(1, 64);
        IntBlock surplus2 = rootFactory.newConstantIntBlockWith(2, 64);
        Page overProjected = new Page(64, new Block[] { surplus1, surplus2 });
        long bytesAfterProducer = rootBreaker.getUsed();
        assertTrue("producer must reserve breaker bytes for surplus blocks", bytesAfterProducer > 0L);

        Page injected = it.inject(overProjected);
        try {
            assertEquals(64, injected.getPositionCount());
            assertEquals(1, injected.getBlockCount());
            IntBlock yearBlock = injected.getBlock(0);
            assertEquals(2024, yearBlock.getInt(0));
        } finally {
            injected.releaseBlocks();
        }
        assertEquals("iterator must release surplus blocks; breaker must return to zero", 0L, rootBreaker.getUsed());
    }

    /**
     * Same surplus contract when there are no partition columns to inject. The fast path in
     * {@link VirtualColumnIterator#inject(Page)} returns the page unchanged when block counts
     * match, but must rebuild a tightly-projected page and release the surplus when they do not.
     */
    public void testInjectReleasesSurplusBlocksWhenNoPartitionColumns() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        CircuitBreaker rootBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory rootFactory = BlockFactory.builder(bigArrays).breaker(rootBreaker).build();

        // No partition columns; iterator should pass through one data block and drop the surplus.
        List<Attribute> fullOutput = List.of(attr("data", DataType.INTEGER));
        Set<String> partitionCols = Set.of();
        VirtualColumnIterator it = new VirtualColumnIterator(
            new SinglePageIterator(new Page(0)),
            fullOutput,
            partitionCols,
            Map.of(),
            rootFactory
        );

        IntBlock dataBlock = rootFactory.newConstantIntBlockWith(7, 32);
        IntBlock surplus = rootFactory.newConstantIntBlockWith(99, 32);
        Page overProjected = new Page(32, new Block[] { dataBlock, surplus });
        assertTrue("producer must reserve breaker bytes for surplus blocks", rootBreaker.getUsed() > 0L);

        Page injected = it.inject(overProjected);
        try {
            assertEquals(32, injected.getPositionCount());
            assertEquals(1, injected.getBlockCount());
            IntBlock data = injected.getBlock(0);
            assertEquals(7, data.getInt(0));
        } finally {
            injected.releaseBlocks();
        }
        assertEquals("iterator must release surplus blocks; breaker must return to zero", 0L, rootBreaker.getUsed());
    }

    /**
     * Failure mid-partition-allocation with a surplus-emitting producer must propagate the original
     * throwable, not an {@code IllegalStateException} from double-closing a surplus block. The fix
     * defers surplus close to the success path: in the catch arm,
     * {@link Page#releaseBlocks()} on the input page closes the surplus once. An earlier shape
     * closed the surplus pre-emptively and then re-released it via {@code releaseBlocks()},
     * masking the real failure cause.
     */
    public void testInjectPropagatesOriginalThrowableWhenSurplusPresent() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        CircuitBreaker rootBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory rootFactory = BlockFactory.builder(bigArrays).breaker(rootBreaker).build();

        // Two partition columns. The second's value type is unenumerated, so createConstantBlock's
        // fail-loud default throws — a stand-in for any allocation-time failure after partial success.
        List<Attribute> fullOutput = List.of(partAttr("year", DataType.INTEGER), partAttr("tag", DataType.KEYWORD));
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year", "tag"));
        Object unenumeratedValue = new Object();
        Map<String, Object> partitionValues = Map.of("year", 2024, "tag", unenumeratedValue);
        VirtualColumnIterator it = new VirtualColumnIterator(
            new SinglePageIterator(new Page(0)),
            fullOutput,
            partitionCols,
            partitionValues,
            rootFactory
        );

        // Producer over-projects: data page carries two surplus blocks the iterator never references.
        IntBlock surplus1 = rootFactory.newConstantIntBlockWith(1, 4);
        IntBlock surplus2 = rootFactory.newConstantIntBlockWith(2, 4);
        Page overProjected = new Page(4, new Block[] { surplus1, surplus2 });
        assertTrue("producer must reserve breaker bytes", rootBreaker.getUsed() > 0L);

        RuntimeException thrown = expectThrows(RuntimeException.class, () -> it.inject(overProjected));
        assertThat(thrown.getMessage(), containsString("cannot render constant column [tag]"));
        assertEquals("breaker must return to zero — partial partition allocs + surplus all released", 0L, rootBreaker.getUsed());
    }

    public void testIteratorWrapsDelegate() {
        List<Attribute> fullOutput = List.of(attr("data", DataType.INTEGER), partAttr("year", DataType.INTEGER));
        Set<String> partitionCols = new LinkedHashSet<>(List.of("year"));
        Map<String, Object> partitionValues = Map.of("year", 2024);

        IntBlock dataBlock = blockFactory.newConstantIntBlockWith(7, 3);
        Page dataPage = new Page(3, new Block[] { dataBlock });
        SinglePageIterator delegate = new SinglePageIterator(dataPage);

        VirtualColumnIterator it = new VirtualColumnIterator(delegate, fullOutput, partitionCols, partitionValues, blockFactory);

        assertTrue(it.hasNext());
        Page result = it.next();
        assertEquals(2, result.getBlockCount());
        IntBlock yearBlock = result.getBlock(1);
        assertEquals(2024, yearBlock.getInt(0));

        assertFalse(it.hasNext());
    }

    /**
     * Regression test mirroring the latent thread-affinity bug fixed for
     * {@link SchemaAdaptingIterator}. {@link VirtualColumnIterator} runs on the producer side of
     * {@link AsyncExternalSourceBuffer}; if it were given the driver-local
     * {@link BlockFactory}, allocating a partition-column constant block would trip
     * {@link LocalCircuitBreaker#assertSingleThread()} (debug builds) and silently corrupt the
     * breaker's reserved-bytes accounting (production: assertions stripped). This test pins the
     * driver-local breaker to a sentinel thread, then runs {@link VirtualColumnIterator#inject}
     * from the test thread:
     * <ul>
     *   <li>Driver-local factory must trip the assertion.</li>
     *   <li>Root factory must succeed — proving the production wiring in
     *   {@link AsyncExternalSourceOperatorFactory#wrapWithVirtualColumns} prevents the bug.</li>
     * </ul>
     */
    public void testPartitionInjectFromProducerThreadRequiresRootBlockFactory() throws Exception {
        assumeTrue("requires assertions enabled (-ea) to detect the producer-thread race", assertionsEnabled());

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        CircuitBreaker rootBreaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        BlockFactory rootFactory = BlockFactory.builder(bigArrays).breaker(rootBreaker).build();
        LocalCircuitBreaker driverLocalBreaker = new LocalCircuitBreaker(rootBreaker, 0, 0);
        BlockFactory driverFactory = rootFactory.newChildFactory(driverLocalBreaker);

        Thread setup = new Thread(() -> assertTrue(driverLocalBreaker.assertBeginRunLoop()), "setup-pin-driver-breaker");
        setup.start();
        setup.join();

        try {
            // Schema [data, year(partition)]; produce partition value 2024.
            // Order matters: the iterator's partition slot is index 1, so the off-thread
            // allocation happens AFTER it has indexed dataPage's blocks. inject() releases the
            // input page cleanly on Throwable via its catch path (re-thrown after cleanup), so
            // we don't leak ref counts when the assertion fires.
            List<Attribute> fullOutput = List.of(attr("data", DataType.INTEGER), partAttr("year", DataType.INTEGER));
            Set<String> partitionCols = new LinkedHashSet<>(List.of("year"));
            Map<String, Object> partitionValues = Map.of("year", 2024);

            // 1. Buggy wiring: driver-local factory must trip assertSingleThread.
            AssertionError ae = expectThrows(
                AssertionError.class,
                () -> runInjectOnCurrentThread(rootFactory, driverFactory, fullOutput, partitionCols, partitionValues)
            );
            assertThat(ae.getMessage(), containsString("Local breaker must be accessed by a single thread"));

            // 2. Fixed wiring: root factory has no thread affinity.
            Page result = runInjectOnCurrentThread(rootFactory, rootFactory, fullOutput, partitionCols, partitionValues);
            try {
                assertThat(result.getBlockCount(), equalTo(2));
                assertThat(result.getPositionCount(), equalTo(3));
                IntBlock yearBlock = result.getBlock(1);
                assertEquals(2024, yearBlock.getInt(0));
            } finally {
                result.releaseBlocks();
            }
        } finally {
            assertTrue(driverLocalBreaker.assertEndRunLoop());
            driverLocalBreaker.close();
            assertThat("root breaker must reset to zero after release", rootBreaker.getUsed(), equalTo(0L));
        }
    }

    /**
     * Builds a one-block input page with {@code readerFactory} and exercises
     * {@link VirtualColumnIterator#inject(Page)} with {@code adapterFactory}. The iterator's
     * inject() releases the input page on any {@link Throwable} (including
     * {@link AssertionError}) before propagating, so this helper does not leak ref counts.
     */
    private static Page runInjectOnCurrentThread(
        BlockFactory readerFactory,
        BlockFactory adapterFactory,
        List<Attribute> fullOutput,
        Set<String> partitionCols,
        Map<String, Object> partitionValues
    ) {
        IntBlock dataBlock = readerFactory.newConstantIntBlockWith(7, 3);
        Page dataPage = new Page(3, new Block[] { dataBlock });

        VirtualColumnIterator it = new VirtualColumnIterator(emptyDelegate(), fullOutput, partitionCols, partitionValues, adapterFactory);
        return it.inject(dataPage);
    }

    @SuppressWarnings("AssertWithSideEffects")
    private static boolean assertionsEnabled() {
        boolean enabled = false;
        assert enabled = true;
        return enabled;
    }

    private VirtualColumnIterator newIterator(List<Attribute> fullOutput, Set<String> partitionCols, Map<String, Object> partitionValues) {
        return new VirtualColumnIterator(emptyDelegate(), fullOutput, partitionCols, partitionValues, blockFactory);
    }

    private static CloseableIterator<Page> emptyDelegate() {
        return new CloseableIterator<>() {
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
    }

    private static final class SinglePageIterator implements CloseableIterator<Page> {
        private Page page;

        SinglePageIterator(Page page) {
            this.page = page;
        }

        @Override
        public boolean hasNext() {
            return page != null;
        }

        @Override
        public Page next() {
            if (page == null) {
                throw new NoSuchElementException();
            }
            Page p = page;
            page = null;
            return p;
        }

        @Override
        public void close() {}
    }

    private static Attribute attr(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Attribute partAttr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    private static String asString(BytesRefBlock block, int position) {
        BytesRef ref = new BytesRef();
        block.getBytesRef(block.getFirstValueIndex(position), ref);
        return ref.utf8ToString();
    }
}
