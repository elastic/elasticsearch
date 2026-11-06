/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorAware;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * End-to-end tests for the deferred-extraction path through
 * {@link AsyncExternalSourceOperatorFactory}: opening N files registers N
 * {@link ColumnExtractor}s on the per-driver {@link SourceExtractors} registry, and the encoded
 * {@code _rowPosition} values emitted to downstream operators decode to the right
 * {@code (extractorId, file-local position)} pair.
 */
public class AsyncExternalSourceOperatorFactoryDeferredExtractionTests extends ESTestCase {

    private static final BlockFactory BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(NoopCircuitBreaker.INSTANCE)
        .build();

    public void testDeferredExtractionRegistersExtractorPerFileAndEncodesRowPosition() throws Exception {
        // Three files, each emitting one page with three rows. The reader emits raw file-local
        // row positions (0, 1, 2) on the _rowPosition channel; the factory's encoder must rewrite
        // them to (extractor_id, local_pos) packed longs that decode to the correct id/pos.
        AtomicInteger readCount = new AtomicInteger();
        AtomicInteger extractorsCreated = new AtomicInteger();

        FormatReader_RowPositionEmitting reader = new FormatReader_RowPositionEmitting(readCount, extractorsCreated, /* rowsPerFile = */ 3);

        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 200, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/f3.parquet"), 300, Instant.EPOCH)
        );
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

        StubStorageProvider storageProvider = new StubStorageProvider();
        StoragePath path = StoragePath.of("s3://bucket/data/f1.parquet");

        // Two attributes: a primary "value" column and the synthetic _rowPosition.
        List<Attribute> attributes = List.of(field("value", DataType.INTEGER), field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).deferredExtraction(true).build();

        // Sanity check on capability surface: factory must declare itself as deferred-extraction
        // capable (so LocalExecutionPlanner.planExternalFieldExtract can wire to it).
        assertTrue(factory instanceof DeferredExtractionCapable);

        SourceOperator operator = factory.get(driverContext);
        assertNotNull(operator);

        List<Page> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page != null) {
                pages.add(page);
            }
        }

        try {
            // One read per file = one extractor per file.
            assertEquals("one read per file", 3, readCount.get());
            assertEquals("one extractor created per file", 3, extractorsCreated.get());
            // SourceExtractors registry must mirror the file count.
            SourceExtractors registry = factory.sourceExtractorsFor(driverContext);
            assertEquals(3, registry.size());

            // Three pages, three rows each: every encoded value must decode to a known
            // (id, pos) pair. The factory opens files in fileList order, so file index ==
            // extractor id (== page order).
            assertEquals(3, pages.size());
            for (int fileIdx = 0; fileIdx < pages.size(); fileIdx++) {
                Page page = pages.get(fileIdx);
                assertEquals("two columns: value + _rowPosition", 2, page.getBlockCount());
                assertEquals(3, page.getPositionCount());

                // value column is unchanged.
                IntBlock values = (IntBlock) page.getBlock(0);
                for (int row = 0; row < 3; row++) {
                    assertEquals("payload value preserved", 100 * fileIdx + row, values.getInt(row));
                }

                // _rowPosition column is encoded.
                LongBlock encoded = (LongBlock) page.getBlock(1);
                for (int row = 0; row < 3; row++) {
                    long encodedValue = encoded.getLong(row);
                    assertEquals("extractor id matches file index", fileIdx, SourceExtractors.decodeExtractorId(encodedValue));
                    assertEquals("local position is the file-local row index", row, SourceExtractors.decodeLocalPosition(encodedValue));
                }
            }
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

    /** The ways a scan can open its units; each one registers the extractor on a different code path. */
    private enum OpenMode {
        /** Slice-queue leaves. */
        SLICE_QUEUE,
        MULTI_FILE,
        /** Single file through {@code startSyncWrapperRead}. */
        SYNC_SINGLE_FILE,
        /** Single file through {@code startNativeAsyncRead} / {@code consumePagesInBackground}. */
        NATIVE_ASYNC_SINGLE_FILE;

        boolean singleFile() {
            return this == SYNC_SINGLE_FILE || this == NATIVE_ASYNC_SINGLE_FILE;
        }
    }

    /**
     * A driver that finishes (e.g. a {@code LIMIT} reached) while the producer is still opening the next unit closes the
     * source and then the registry, so registering the newly opened unit's extractor hits a closed registry. That race is
     * expected: the producer must end quietly, close the extractor it just created and not open any further unit.
     */
    public void testProducerEndsQuietlyWhenDriverFinishesWhileOpeningNextUnit() {
        for (OpenMode mode : OpenMode.values()) {
            int triggerUnit = mode.singleFile() ? 0 : randomIntBetween(0, UNITS - 1);
            runWithDriverFinishingWhileOpening(mode, triggerUnit, true).assertEndedQuietly(triggerUnit + 1);
        }
    }

    /**
     * A closed registry while the source buffer is still live is not the shutdown race: it must still fail the query
     * instead of silently truncating results. The unregistered extractor is closed in every case.
     */
    public void testClosedRegistryWithLiveBufferStillFails() {
        for (OpenMode mode : OpenMode.values()) {
            int triggerUnit = mode.singleFile() ? 0 : randomIntBetween(0, UNITS - 1);
            runWithDriverFinishingWhileOpening(mode, triggerUnit, false).assertFailedClosedRegistry(triggerUnit + 1);
        }
    }

    /** Units (files or splits) a multi-unit scan has; a single-file scan has one. */
    private static final int UNITS = 3;

    private record FinishDuringOpen(
        OpenMode mode,
        FormatReader_RowPositionEmitting reader,
        AsyncExternalSourceOperator operator,
        AtomicInteger readCount,
        DriverContext driverContext
    ) {

        void assertEndedQuietly(int expectedReads) {
            assertNull(mode + ": a driver that finished first must not leave a failure", operator.status().failure());
            verify(driverContext, times(1)).removeAsyncAction();
            assertClosedUnregisteredExtractor(expectedReads);
        }

        void assertFailedClosedRegistry(int expectedReads) {
            Throwable failure = operator.status().failure();
            assertNotNull(mode + ": a closed registry under a live buffer must fail the query", failure);
            // Pins that the failure is the closed registry and not an unrelated error.
            assertThat(mode.toString(), failure.getMessage(), containsString("SourceExtractors is closed"));
            verify(driverContext, times(1)).removeAsyncAction();
            assertClosedUnregisteredExtractor(expectedReads);
        }

        private void assertClosedUnregisteredExtractor(int expectedReads) {
            assertEquals(mode + ": no unit is opened after the registry closed", expectedReads, readCount.get());
            assertEquals(mode.toString(), expectedReads, reader.createdExtractors.size());
            for (InMemoryColumnExtractor extractor : reader.createdExtractors) {
                assertEquals(mode + ": every extractor is closed exactly once", 1, extractor.closeCount.get());
            }
            InMemoryColumnExtractor unregistered = reader.createdExtractors.getLast();
            assertFalse(
                mode + ": the unregistered extractor must not be closed under the registry monitor",
                unregistered.closedWhileHoldingMonitor.get()
            );
        }
    }

    /**
     * Runs a deferred-extraction scan over {@link #UNITS} units (one for the single-file modes) on a manually driven executor.
     * While the producer opens unit {@code triggerUnit}, after its reader returned and before its extractor is
     * registered, the "driver" finishes: it optionally closes the operator (finishing the buffer, as {@code Driver} does
     * first) and then closes the registry (what {@code ExternalFieldExtractOperator} does).
     */
    private FinishDuringOpen runWithDriverFinishingWhileOpening(OpenMode mode, int triggerUnit, boolean finishBuffer) {
        AtomicInteger readCount = new AtomicInteger();
        FormatReader_RowPositionEmitting reader = new FormatReader_RowPositionEmitting(readCount, new AtomicInteger(), 3);
        reader.nativeAsync(mode == OpenMode.NATIVE_ASYNC_SINGLE_FILE);

        List<Attribute> attributes = List.of(field("value", DataType.INTEGER), field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));
        // DriverContext is mocked, as in the sibling tests, because a real one needs a full driver; the mock also lets
        // the test count removeAsyncAction() calls. Stop hooks and waitForAsyncActions callbacks are irrelevant here.
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        // Tasks only run when the test drains them, so the hook below can use the operator returned by get().
        Deque<Runnable> tasks = new ArrayDeque<>();
        StoragePath path = StoragePath.of("s3://bucket/data/f1.parquet");
        var builder = AsyncExternalSourceOperatorFactory.builder(
            new StubStorageProvider(),
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> tasks.add(r)
        ).deferredExtraction(true);
        switch (mode) {
            case MULTI_FILE -> {
                List<StorageEntry> entries = new ArrayList<>();
                for (int i = 1; i <= UNITS; i++) {
                    entries.add(new StorageEntry(StoragePath.of("s3://bucket/data/f" + i + ".parquet"), 100L * i, Instant.EPOCH));
                }
                builder.fileList(GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet"));
            }
            case SLICE_QUEUE -> {
                List<ExternalSplit> splits = new ArrayList<>();
                for (int i = 1; i <= UNITS; i++) {
                    Map<String, Object> config = i == UNITS ? Map.of(FileSplitProvider.LAST_SPLIT_KEY, "true") : Map.of();
                    splits.add(
                        new FileSplit("test", StoragePath.of("s3://bucket/data/f" + i + ".parquet"), 0, 100, "parquet", config, Map.of())
                    );
                }
                builder.sliceQueue(new ExternalSliceQueue(splits));
            }
            case SYNC_SINGLE_FILE, NATIVE_ASYNC_SINGLE_FILE -> {
            }
        }
        AsyncExternalSourceOperatorFactory factory = builder.build();

        SourceExtractors registry = factory.sourceExtractorsFor(driverContext);
        reader.registryMonitor = registry;
        AtomicReference<SourceOperator> operatorRef = new AtomicReference<>();
        reader.onRead(idx -> {
            if (idx == triggerUnit) {
                if (finishBuffer) {
                    operatorRef.get().close();
                }
                registry.close();
            }
        });

        AsyncExternalSourceOperator operator = (AsyncExternalSourceOperator) factory.get(driverContext);
        operatorRef.set(operator);
        Runnable task;
        while ((task = tasks.poll()) != null) {
            task.run();
        }
        if (finishBuffer == false) {
            // Releases the pages the live buffer still holds.
            operator.close();
        }
        return new FinishDuringOpen(mode, reader, operator, readCount, driverContext);
    }

    /**
     * The deferred extractor's warning sink must deposit into the driver's {@link DriverContext}, which
     * {@code DriverCompletionInfo} ships back from whatever node ran the scan, rather than into the emitting
     * thread's {@code ThreadContext} — the latter only reaches the client when the scan runs on the coordinator.
     * The sink stays budget-gated, so a flood still collapses to the cap plus one
     * overflow marker.
     */
    public void testDeferredExtractorWarningSinkFeedsTheDriverSinkUnderTheBudget() throws Exception {
        AtomicInteger readCount = new AtomicInteger();
        AtomicInteger extractorsCreated = new AtomicInteger();
        List<Consumer<String>> capturedSinks = Collections.synchronizedList(new ArrayList<>());
        FormatReader_RowPositionEmitting reader = new FormatReader_RowPositionEmitting(
            readCount,
            extractorsCreated,
            /* rowsPerFile = */ 1,
            capturedSinks
        );

        StoragePath path = StoragePath.of("s3://bucket/data/f1.parquet");
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(path, 100, Instant.EPOCH)), "s3://bucket/data/*.parquet");
        List<Attribute> attributes = List.of(field("value", DataType.INTEGER), field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));

        // A real context, since the sink under test writes into it; a mock would swallow every message.
        DriverContext driverContext = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, BLOCK_FACTORY, null);

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubStorageProvider(),
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).deferredExtraction(true).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page page = operator.getOutput();
                if (page != null) {
                    pages.add(page);
                }
            }
            assertThat("the extractor handshake must hand out a sink", capturedSinks, hasSize(1));
            Consumer<String> sink = capturedSinks.get(0);

            // One distinct message per call, so the budget counts each rather than deduplicating them, and the
            // count deliberately overshoots the cap.
            int offered = SkipWarnings.MAX_ADDED_WARNINGS + 5;
            for (int i = 0; i < offered; i++) {
                sink.accept("extractor notice " + i);
            }
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }

        driverContext.finish();
        List<String> warnings = driverContext.warnings();
        assertThat(
            "the cap admits MAX_ADDED_WARNINGS payloads plus one overflow marker",
            warnings,
            hasSize(SkipWarnings.MAX_ADDED_WARNINGS + 1)
        );
        assertThat(warnings.get(0), equalTo("extractor notice 0"));
        assertThat(warnings, hasItem(SkipWarnings.overflowMessage()));
        Releasables.close(driverContext.getSnapshot());
    }

    public void testEmptyDataProjectionWithDeferredExtractionAndNonIdentityMapping() throws Exception {
        // All data columns deferred (e.g. SORT _file.size | LIMIT n over a wide file): the source
        // projects only _rowPosition (empty queryDataSchema) while the per-file mapping stays at full,
        // non-identity width. Pre-fix this tripped the width guard; the factory must pass the
        // _rowPosition page through and still run the deferred-extraction encoding handshake.
        AtomicInteger extractorsCreated = new AtomicInteger();
        RowPositionOnlyReader reader = new RowPositionOnlyReader(extractorsCreated, /* rows = */ 3);

        StoragePath path = StoragePath.of("s3://bucket/data/f1.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(path, 100, Instant.EPOCH));
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

        // Non-identity, full-width (3) per-file mapping, as carried for a multi-file reconciliation.
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = Map.of(
            path,
            new SchemaReconciliation.FileSchemaInfo(
                new ExternalSchema(List.of(field("a", DataType.INTEGER), field("b", DataType.INTEGER), field("c", DataType.INTEGER))),
                new ColumnMapping(new int[] { 2, 1, 0 }, null),
                null
            )
        );

        // Only _rowPosition is projected — a MetadataAttribute, so queryDataSchema is empty.
        List<Attribute> attributes = List.of(rowPositionAttribute());

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubStorageProvider(),
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).schemaMap(schemaMap).deferredExtraction(true).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page p = operator.getOutput();
                if (p != null) {
                    pages.add(p);
                }
            }
            assertEquals("one page", 1, pages.size());
            Page page = pages.get(0);
            // Passed through untouched: the reader's single _rowPosition block, NOT reshaped to the
            // mapping's width 3.
            assertEquals("only the _rowPosition block survives", 1, page.getBlockCount());
            assertEquals(3, page.getPositionCount());
            // Deferred-extraction handshake still ran: one extractor registered, _rowPosition encoded.
            assertEquals("one extractor registered", 1, extractorsCreated.get());
            LongBlock encoded = (LongBlock) page.getBlock(0);
            for (int row = 0; row < 3; row++) {
                assertEquals(0, SourceExtractors.decodeExtractorId(encoded.getLong(row)));
                assertEquals(row, SourceExtractors.decodeLocalPosition(encoded.getLong(row)));
            }
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

    public void testDeferredExtractionDisabledLeavesPagesUntouched() throws Exception {
        // With deferredExtraction(false), the factory must not register extractors and must not
        // touch the _rowPosition channel. The reader's raw file-local positions should pass
        // through unchanged.
        AtomicInteger extractorsCreated = new AtomicInteger();
        FormatReader_RowPositionEmitting reader = new FormatReader_RowPositionEmitting(
            new AtomicInteger(),
            extractorsCreated,
            /* rowsPerFile = */ 2
        );

        StorageObject storageObject = mock(StorageObject.class);
        StorageProvider storageProvider = mock(StorageProvider.class);
        when(storageProvider.newObject(any())).thenReturn(storageObject);

        StoragePath path = StoragePath.of("s3://bucket/data/f.parquet");
        List<Attribute> attributes = List.of(field("value", DataType.INTEGER), field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).build();

        assertFalse(factory.deferredExtractionEnabled());

        SourceOperator operator = factory.get(driverContext);
        Page page = null;
        try {
            while (operator.isFinished() == false) {
                Page p = operator.getOutput();
                if (p != null) {
                    page = p;
                    break;
                }
            }
            assertNotNull(page);
            // Encoder was not wired: no extractors registered.
            assertEquals(0, extractorsCreated.get());
            // _rowPosition is whatever the reader emitted (raw 0, 1) — confirms the encoder
            // path is fully bypassed when the flag is off.
            LongBlock rp = (LongBlock) page.getBlock(1);
            assertEquals(0L, rp.getLong(0));
            assertEquals(1L, rp.getLong(1));
        } finally {
            if (page != null) {
                page.releaseBlocks();
            }
            operator.close();
        }
    }

    /**
     * Defensive: a {@link ColumnExtractorAware} reader satisfies the planner-time capability check,
     * but it must also return iterators that implement {@link ColumnExtractorProducer} when the
     * projection asks for {@link ColumnExtractor#ROW_POSITION_COLUMN}. If the iterator does not,
     * the factory must surface a clear {@link IllegalStateException} — it cannot silently encode
     * ids that nothing will register against, because the extract operator above TopN would then
     * fail to look up its source. Catches regressions where a future format reader forgets to
     * thread the producer interface through its iterator wrapping.
     */
    public void testDeferredExtractionFailsLoudlyWhenIteratorIsNotProducer() throws Exception {
        // Reader is ColumnExtractorAware but its iterator does NOT implement ColumnExtractorProducer.
        FormatReader reader = new NonProducerAwareReader();

        List<StorageEntry> entries = List.of(new StorageEntry(StoragePath.of("s3://bucket/data/f.parquet"), 100, Instant.EPOCH));
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

        StubStorageProvider storageProvider = new StubStorageProvider();
        StoragePath path = StoragePath.of("s3://bucket/data/f.parquet");
        List<Attribute> attributes = List.of(field("value", DataType.INTEGER), field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).deferredExtraction(true).build();

        SourceOperator operator = factory.get(driverContext);
        try {
            // The operator's slice setup is what triggers wrapWithEncoderIfNeeded; pulling output
            // until finished surfaces the failure. Either getOutput or close() will throw.
            Exception caught = expectThrows(Exception.class, () -> {
                while (operator.isFinished() == false) {
                    operator.getOutput();
                }
            });
            assertTrue(
                "expected ColumnExtractorProducer error, got: " + caught,
                rootCauseMessage(caught).contains("ColumnExtractorProducer")
            );
        } finally {
            try {
                operator.close();
            } catch (Exception ignored) {
                // close() may also surface the same failure once the operator is in a bad state;
                // we already asserted above.
            }
        }
    }

    private static String rootCauseMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage() != null ? root.getMessage() : "";
    }

    /**
     * Reader stub that <em>declares</em> {@link ColumnExtractorAware} so the planner-time
     * capability check passes, but returns plain iterators that do <em>not</em> implement
     * {@link ColumnExtractorProducer}. Used to verify the factory's runtime guard.
     */
    private static final class NonProducerAwareReader implements NoConfigFormatReader, ColumnExtractorAware {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            // One page with two columns, _rowPosition at index 1. Critically: the iterator does
            // not implement ColumnExtractorProducer.
            int n = 2;
            int[] values = new int[] { 0, 1 };
            long[] rowPositions = new long[] { 0L, 1L };
            Block valueBlock = BLOCK_FACTORY.newIntArrayVector(values, n).asBlock();
            Block rpBlock = BLOCK_FACTORY.newLongArrayVector(rowPositions, n).asBlock();
            Page page = new Page(n, valueBlock, rpBlock);
            return singletonIterator(page);
        }

        @Override
        public String formatName() {
            return "non-producer-aware";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * The factory keeps two views of its planner-resolved attributes: the user-visible {@code
     * attributes} (which include the optimizer-injected {@link ColumnExtractor#ROW_POSITION_COLUMN}
     * when deferred extraction is on) and {@code readerResolvedAttributes} (the file-schema-shaped
     * view passed to readers). The split here is what keeps a reader from misinterpreting the
     * synthetic as a real-column collision — which is exactly how the production
     * {@code OptimizedParquetColumnIterator} regression slipped in.
     */
    public void testStripRowPositionRemovesSyntheticAttribute() {
        Attribute value = field("value", DataType.INTEGER);
        Attribute rowPos = field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG);

        List<Attribute> stripped = AsyncExternalSourceOperatorFactory.stripRowPosition(List.of(value, rowPos));
        assertEquals(1, stripped.size());
        assertEquals("value", stripped.get(0).name());

        // Marker absent: identity-return so non-deferred-extraction callers pay nothing.
        List<Attribute> noMarker = List.of(value);
        assertSame(noMarker, AsyncExternalSourceOperatorFactory.stripRowPosition(noMarker));

        // Empty / null inputs are passed through unchanged.
        assertSame(List.<Attribute>of(), AsyncExternalSourceOperatorFactory.stripRowPosition(List.of()));
        assertNull(AsyncExternalSourceOperatorFactory.stripRowPosition(null));
    }

    public void testDeferredExtractionDefersOnCloseUntilRegistryCloses() throws Exception {
        // The bug this regression-tests: with deferredExtraction enabled, the factory used to
        // close its onClose (the per-query concurrency budget) the moment the last source operator
        // released. But the ExternalFieldExtractOperator runs AFTER TopN drains, so closing the
        // budget at source-release time tore down storage permits while extract() was still trying
        // to acquire them — surfacing as
        // "Failed to acquire query concurrency budget permit: Budget is closed".
        // Now onClose must be deferred until the SourceExtractors registry (driver-scoped, owned
        // by the extract operator) is closed.
        AtomicInteger readCount = new AtomicInteger();
        AtomicInteger extractorsCreated = new AtomicInteger();
        FormatReader_RowPositionEmitting reader = new FormatReader_RowPositionEmitting(readCount, extractorsCreated, 2);

        List<StorageEntry> entries = List.of(new StorageEntry(StoragePath.of("s3://bucket/data/f.parquet"), 100, Instant.EPOCH));
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");
        StubStorageProvider storageProvider = new StubStorageProvider();
        StoragePath path = StoragePath.of("s3://bucket/data/f.parquet");
        List<Attribute> attributes = List.of(field("value", DataType.INTEGER), field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AtomicInteger onCloseCalls = new AtomicInteger();
        java.io.Closeable onCloseProbe = () -> onCloseCalls.incrementAndGet();
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).deferredExtraction(true).onClose(onCloseProbe).build();

        // Resolve the registry early — typically done by ExternalFieldExtractOperator.Factory.get()
        // before the source actually finishes, mirroring the real wiring.
        SourceExtractors registry = factory.sourceExtractorsFor(driverContext);

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page p = operator.getOutput();
                if (p != null) {
                    pages.add(p);
                }
            }
        } finally {
            operator.close();
        }

        // Source done, but the registry is still open -> onClose must NOT have fired.
        assertEquals("onClose must wait for the registry while deferred extraction is on", 0, onCloseCalls.get());

        for (Page p : pages) {
            p.releaseBlocks();
        }

        // Closing the registry releases the last ref — onClose finally runs.
        registry.close();
        assertEquals("onClose runs exactly once after registry teardown", 1, onCloseCalls.get());
    }

    /**
     * {@code createDrivers} builds the source operator before the extract operator, so
     * {@link AsyncExternalSourceOperatorFactory#sourceExtractorsFor} can run after {@code get()}.
     * A producer that dies before registering an extractor must not drop the factory's deferred
     * ref — otherwise the later registry attaches to an already-returned lease.
     */
    public void testDeferredExtractionKeepsFactoryRefUntilOperatorCloseWhenGetRunsFirst() throws Exception {
        FormatReader failOnRead = new FailOnReadExtractorAwareReader();
        StorageObject storageObject = mock(StorageObject.class);
        StorageProvider storageProvider = mock(StorageProvider.class);
        when(storageProvider.newObject(any())).thenReturn(storageObject);

        StoragePath path = StoragePath.of("s3://bucket/data/f.parquet");
        List<Attribute> attributes = List.of(field("value", DataType.INTEGER), field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AtomicInteger onCloseCalls = new AtomicInteger();
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            failOnRead,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).deferredExtraction(true).onClose(() -> onCloseCalls.incrementAndGet()).build();

        SourceOperator operator = factory.get(driverContext);
        try {
            expectThrows(Exception.class, operator::getOutput);
            assertEquals("operator hold must keep the factory deferred ref after a failed get()", 0, onCloseCalls.get());

            SourceExtractors registry = factory.sourceExtractorsFor(driverContext);
            assertEquals("late sourceExtractorsFor must attach to a still-open lease", 0, onCloseCalls.get());

            operator.close();
            assertEquals("onClose waits for the registry after operator.close()", 0, onCloseCalls.get());
            registry.close();
            assertEquals("onClose runs once after operator and registry both close", 1, onCloseCalls.get());
        } finally {
            operator.close();
        }
    }

    public void testNonDeferredExtractionStillClosesOnSourceRelease() throws Exception {
        // Symmetric guard: when deferred extraction is disabled, onClose runs after the last
        // operator.close() (the producer has also finished by then) — there is no late
        // materialization reading from the budget, so keeping it alive would just delay GC.
        FormatReader_RowPositionEmitting reader = new FormatReader_RowPositionEmitting(new AtomicInteger(), new AtomicInteger(), 2);

        StorageObject storageObject = mock(StorageObject.class);
        StorageProvider storageProvider = mock(StorageProvider.class);
        when(storageProvider.newObject(any())).thenReturn(storageObject);

        StoragePath path = StoragePath.of("s3://bucket/data/f.parquet");
        List<Attribute> attributes = List.of(field("value", DataType.INTEGER), field(ColumnExtractor.ROW_POSITION_COLUMN, DataType.LONG));

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AtomicInteger onCloseCalls = new AtomicInteger();
        java.io.Closeable onCloseProbe = () -> onCloseCalls.incrementAndGet();
        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).onClose(onCloseProbe).build();

        SourceOperator operator = factory.get(driverContext);
        try {
            while (operator.isFinished() == false) {
                Page p = operator.getOutput();
                if (p != null) {
                    p.releaseBlocks();
                }
            }
        } finally {
            operator.close();
        }
        assertEquals("non-deferred path closes onClose after last operator.close()", 1, onCloseCalls.get());
    }

    public void testDeferredExtractionRequiresColumnExtractorAware() {
        // Builder must reject deferredExtraction(true) when the reader doesn't implement
        // ColumnExtractorAware — we won't be able to materialise anything later, so failing
        // fast at construction beats silently emitting refs no one can resolve.
        FormatReader plain = new PlainFormatReader();
        StoragePath path = StoragePath.of("s3://bucket/data/f.parquet");
        List<Attribute> attributes = List.of(field("value", DataType.INTEGER));
        StorageProvider storageProvider = mock(StorageProvider.class);

        expectThrows(
            IllegalArgumentException.class,
            () -> AsyncExternalSourceOperatorFactory.builder(storageProvider, plain, path, attributes, 100, 10, (Runnable r) -> r.run())
                .deferredExtraction(true)
                .build()
        );
    }

    // ---------------------------------------------------------------------------------------------
    // Stubs
    // ---------------------------------------------------------------------------------------------

    public void testNonIdentityMappingPreservesRowPositionWithoutDeferredExtraction() throws Exception {
        // Regression for the adaptSchema row-position slot derivation: the reader appends
        // _rowPosition to its projection for plain _file.record_ref composition too (no deferred extraction,
        // no paired extract exec), and the input slot is its position in the per-file projection.
        // Deriving it from the deferred flag dropped the channel on every schema-drifted file
        // (the adapter released the tail block; the downstream block-count check then failed for
        // any heterogeneous glob + METADATA _file.record_ref). The drift here: the file stores [b, a], the
        // query wants [a, b] — a non-identity mapping with _rowPosition riding at the tail.
        ProjectionEchoReader reader = new ProjectionEchoReader(/* rows = */ 3);

        StoragePath path = StoragePath.of("s3://bucket/data/f1.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(path, 100, Instant.EPOCH));
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/*.parquet");

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = Map.of(
            path,
            new SchemaReconciliation.FileSchemaInfo(
                new ExternalSchema(List.of(field("b", DataType.INTEGER), field("a", DataType.INTEGER))),
                new ColumnMapping(new int[] { 1, 0 }, null),
                null
            )
        );

        List<Attribute> attributes = List.of(field("a", DataType.INTEGER), field("b", DataType.INTEGER), rowPositionAttribute());

        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            new StubStorageProvider(),
            reader,
            path,
            attributes,
            100,
            10,
            (Runnable r) -> r.run()
        ).fileList(fileList).schemaMap(schemaMap).deferredExtraction(false).build();

        SourceOperator operator = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        try {
            while (operator.isFinished() == false) {
                Page p = operator.getOutput();
                if (p != null) {
                    pages.add(p);
                }
            }
            assertEquals("one page", 1, pages.size());
            Page page = pages.get(0);
            assertEquals("mapped data columns AND the _rowPosition channel must survive adaptation", 3, page.getBlockCount());
            IntBlock a = page.getBlock(0);
            IntBlock b = page.getBlock(1);
            LongBlock rowPos = page.getBlock(2);
            for (int i = 0; i < 3; i++) {
                assertEquals("query column a maps from the file's second slot", 100 + i, a.getInt(i));
                assertEquals("query column b maps from the file's first slot", 200 + i, b.getInt(i));
                assertEquals("row positions flow through untouched", i, rowPos.getLong(i));
            }
        } finally {
            for (Page p : pages) {
                p.releaseBlocks();
            }
            operator.close();
        }
    }

    /**
     * Emits one page per read: for each projected column, an IntBlock (values {@code 100+i} for
     * column {@code a}, {@code 200+i} for {@code b}) or the {@code _rowPosition} LongBlock
     * ({@code 0..rows-1}), in projection order — mirroring a real reader's emission contract.
     */
    private static final class ProjectionEchoReader implements NoConfigFormatReader {
        private final int rows;

        ProjectionEchoReader(int rows) {
            this.rows = rows;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            List<String> projected = context.projectedColumns();
            Block[] blocks = new Block[projected.size()];
            for (int c = 0; c < projected.size(); c++) {
                String name = projected.get(c);
                if (ColumnExtractor.ROW_POSITION_COLUMN.equals(name)) {
                    try (var builder = BLOCK_FACTORY.newLongBlockBuilder(rows)) {
                        for (int i = 0; i < rows; i++) {
                            builder.appendLong(i);
                        }
                        blocks[c] = builder.build();
                    }
                } else {
                    int base = "a".equals(name) ? 100 : 200;
                    try (var builder = BLOCK_FACTORY.newIntBlockBuilder(rows)) {
                        for (int i = 0; i < rows; i++) {
                            builder.appendInt(base + i);
                        }
                        blocks[c] = builder.build();
                    }
                }
            }
            Page page = new Page(rows, blocks);
            return new CloseableIterator<>() {
                private boolean emitted = false;

                @Override
                public boolean hasNext() {
                    return emitted == false;
                }

                @Override
                public Page next() {
                    emitted = true;
                    return page;
                }

                @Override
                public void close() {
                    if (emitted == false) {
                        page.releaseBlocks();
                    }
                }
            };
        }

        @Override
        public String formatName() {
            return "projection-echo-stub";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    /** The synthetic deferred-extraction column as the optimizer injects it: a MetadataAttribute. */
    private static MetadataAttribute rowPositionAttribute() {
        return new MetadataAttribute(
            Source.EMPTY,
            ColumnExtractor.ROW_POSITION_COLUMN,
            DataType.LONG,
            Nullability.FALSE,
            null,
            true,
            false
        );
    }

    /**
     * {@link ColumnExtractorAware} reader that emits a single _rowPosition block and no data columns
     * — the shape a reader produces when every data column has been moved to deferred extraction.
     */
    private static final class RowPositionOnlyReader implements NoConfigFormatReader, ColumnExtractorAware {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final AtomicInteger extractorsCreated;
        private final int rows;

        RowPositionOnlyReader(AtomicInteger extractorsCreated, int rows) {
            this.extractorsCreated = extractorsCreated;
            this.rows = rows;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return new RowPositionOnlyProducerIterator(extractorsCreated, rows);
        }

        @Override
        public String formatName() {
            return "rp-only-stub";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /** Single-block (_rowPosition only) producer iterator; OR-s in the assigned extractor id. */
    private static final class RowPositionOnlyProducerIterator implements CloseableIterator<Page>, ColumnExtractorProducer {
        private final AtomicInteger extractorsCreated;
        private final int rows;
        private boolean emitted = false;
        private long highBits = -1L;

        RowPositionOnlyProducerIterator(AtomicInteger extractorsCreated, int rows) {
            this.extractorsCreated = extractorsCreated;
            this.rows = rows;
        }

        @Override
        public boolean hasNext() {
            return emitted == false;
        }

        @Override
        public Page next() {
            if (emitted) {
                throw new java.util.NoSuchElementException();
            }
            emitted = true;
            long[] rowPositions = new long[rows];
            for (int i = 0; i < rows; i++) {
                rowPositions[i] = highBits == -1L ? i : (highBits | i);
            }
            return new Page(rows, BLOCK_FACTORY.newLongArrayVector(rowPositions, rows).asBlock());
        }

        @Override
        public ColumnExtractor createColumnExtractor(@Nullable Consumer<String> driverThreadWarningSink) {
            extractorsCreated.incrementAndGet();
            return new InMemoryColumnExtractor(rows);
        }

        @Override
        public void setExtractorId(int id) {
            highBits = ((long) id) << ColumnExtractor.LOCAL_POSITION_BITS;
        }

        @Override
        public void close() {}
    }

    /**
     * {@link ColumnExtractorAware} reader whose {@code read} throws before a producer iterator
     * exists, so deferred extraction never reaches {@code sourceExtractorsFor} during {@code get()}.
     */
    private static final class FailOnReadExtractorAwareReader implements NoConfigFormatReader, ColumnExtractorAware {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) throws IOException {
            throw new IOException("injected first-read failure");
        }

        @Override
        public String formatName() {
            return "fail-first-aware";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Format reader that implements {@link ColumnExtractorAware}. Each {@code read} returns one
     * page with two columns (value, _rowPosition). The {@code _rowPosition} column carries raw
     * file-local positions ({@code 0..rowsPerFile-1}) — exactly what a real
     * {@link ColumnExtractorAware} reader emits. The factory's encoder is responsible for packing
     * the assigned extractor id into the high bits before downstream operators see it.
     */
    private static final class FormatReader_RowPositionEmitting implements NoConfigFormatReader, ColumnExtractorAware {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final AtomicInteger readCount;
        private final AtomicInteger extractorsCreated;
        private final int rowsPerFile;
        /** Sinks handed to {@link ColumnExtractorProducer#createColumnExtractor}; empty unless a test wants them. */
        private final List<Consumer<String>> capturedSinks;
        /** Every extractor handed out by {@link ColumnExtractorProducer#createColumnExtractor}, in creation order. */
        final List<InMemoryColumnExtractor> createdExtractors = Collections.synchronizedList(new ArrayList<>());
        /** When set, extractors created afterwards record whether {@code close()} ran while holding this monitor. */
        volatile Object registryMonitor;
        /** Invoked with the file index at the end of {@link #read}, i.e. while the producer is still opening that unit. */
        private volatile IntConsumer onRead = idx -> {};
        private volatile boolean nativeAsync = false;

        void onRead(IntConsumer onRead) {
            this.onRead = onRead;
        }

        void nativeAsync(boolean nativeAsync) {
            this.nativeAsync = nativeAsync;
        }

        @Override
        public boolean supportsNativeAsync() {
            return nativeAsync;
        }

        FormatReader_RowPositionEmitting(AtomicInteger readCount, AtomicInteger extractorsCreated, int rowsPerFile) {
            this(readCount, extractorsCreated, rowsPerFile, Collections.synchronizedList(new ArrayList<>()));
        }

        FormatReader_RowPositionEmitting(
            AtomicInteger readCount,
            AtomicInteger extractorsCreated,
            int rowsPerFile,
            List<Consumer<String>> capturedSinks
        ) {
            this.readCount = readCount;
            this.extractorsCreated = extractorsCreated;
            this.rowsPerFile = rowsPerFile;
            this.capturedSinks = capturedSinks;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            int idx = readCount.getAndIncrement();
            CloseableIterator<Page> pages = new ProducerIterator(idx, extractorsCreated, rowsPerFile, capturedSinks, this);
            onRead.accept(idx);
            return pages;
        }

        @Override
        public String formatName() {
            return "rp-stub";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /**
     * Wraps a page iterator and exposes the {@link ColumnExtractorProducer} handshake that
     * {@link AsyncExternalSourceOperatorFactory#wrapWithEncoderIfNeeded} requires when deferred
     * extraction is enabled. Returns a fresh in-memory extractor on each call.
     */
    private static final class ProducerIterator implements CloseableIterator<Page>, ColumnExtractorProducer {
        private final int fileIndex;
        private final AtomicInteger extractorsCreated;
        private final int rowsPerFile;
        private final List<Consumer<String>> capturedSinks;
        private final FormatReader_RowPositionEmitting owner;
        private boolean emitted = false;
        private long rowPositionEncodingHighBits = -1L;

        ProducerIterator(
            int fileIndex,
            AtomicInteger extractorsCreated,
            int rowsPerFile,
            List<Consumer<String>> capturedSinks,
            FormatReader_RowPositionEmitting owner
        ) {
            this.fileIndex = fileIndex;
            this.extractorsCreated = extractorsCreated;
            this.rowsPerFile = rowsPerFile;
            this.capturedSinks = capturedSinks;
            this.owner = owner;
        }

        @Override
        public boolean hasNext() {
            return emitted == false;
        }

        @Override
        public Page next() {
            if (emitted) {
                throw new java.util.NoSuchElementException();
            }
            emitted = true;
            // Mirrors the production path: at materialise-time the iterator OR-s the
            // installed high bits into every emitted _rowPosition value, just like
            // OptimizedParquetColumnIterator does. setExtractorId must have been called by now.
            int n = rowsPerFile;
            int[] values = new int[n];
            long[] rowPositions = new long[n];
            long encodingHighBits = rowPositionEncodingHighBits;
            for (int i = 0; i < n; i++) {
                values[i] = 100 * fileIndex + i;
                rowPositions[i] = encodingHighBits == -1L ? i : (encodingHighBits | i);
            }
            Block valueBlock = BLOCK_FACTORY.newIntArrayVector(values, n).asBlock();
            Block rpBlock = BLOCK_FACTORY.newLongArrayVector(rowPositions, n).asBlock();
            return new Page(n, valueBlock, rpBlock);
        }

        @Override
        public ColumnExtractor createColumnExtractor(@Nullable Consumer<String> driverThreadWarningSink) {
            extractorsCreated.incrementAndGet();
            if (driverThreadWarningSink != null) {
                capturedSinks.add(driverThreadWarningSink);
            }
            InMemoryColumnExtractor extractor = new InMemoryColumnExtractor(rowsPerFile, owner.registryMonitor);
            owner.createdExtractors.add(extractor);
            return extractor;
        }

        @Override
        public void setExtractorId(int id) {
            rowPositionEncodingHighBits = ((long) id) << ColumnExtractor.LOCAL_POSITION_BITS;
        }

        @Override
        public void close() {}
    }

    /** Same shape as the row-position emitting reader minus the {@link ColumnExtractorAware} mixin. */
    private static final class PlainFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return singletonIterator(new Page(BLOCK_FACTORY.newIntArrayVector(new int[] { 0 }, 1).asBlock()));
        }

        @Override
        public String formatName() {
            return "plain";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /** Trivial {@link ColumnExtractor} stub — never invoked by these tests, just registered. */
    private static final class InMemoryColumnExtractor implements ColumnExtractor {
        private final int totalRows;
        private final Object monitorToCheck;
        final AtomicInteger closeCount = new AtomicInteger();
        final AtomicBoolean closedWhileHoldingMonitor = new AtomicBoolean();

        InMemoryColumnExtractor(int totalRows) {
            this(totalRows, null);
        }

        InMemoryColumnExtractor(int totalRows, @Nullable Object monitorToCheck) {
            this.totalRows = totalRows;
            this.monitorToCheck = monitorToCheck;
        }

        @Override
        public long rowCount() {
            return totalRows;
        }

        @Override
        public Block[] extract(String[] columnNames, DataType[] targetTypes, long[] localPositions, BlockFactory blockFactory) {
            throw new AssertionError("extract() must not be called from these tests");
        }

        @Override
        public void close() {
            if (monitorToCheck != null && Thread.holdsLock(monitorToCheck)) {
                closedWhileHoldingMonitor.set(true);
            }
            closeCount.incrementAndGet();
        }
    }

    /** Storage provider that fabricates {@link StorageObject}s for arbitrary paths. */
    private static final class StubStorageProvider implements StorageProvider {
        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null; // directory-aware listing is irrelevant to this test double
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3", "file");
        }

        @Override
        public void close() {}
    }

    private static final class StubStorageObject extends AbstractTestStorageObject {
        private final StoragePath path;

        StubStorageObject(StoragePath path) {
            this.path = path;
        }

        @Override
        public InputStream newStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public InputStream newStream(long position, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long length() {
            return 1024;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }

    private static CloseableIterator<Page> singletonIterator(Page page) {
        Iterator<Page> it = List.of(page).iterator();
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Page next() {
                return it.next();
            }

            @Override
            public void close() throws IOException {}
        };
    }
}
