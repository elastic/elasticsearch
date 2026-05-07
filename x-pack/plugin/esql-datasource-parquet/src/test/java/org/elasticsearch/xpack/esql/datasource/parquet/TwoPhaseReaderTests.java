/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end tests for the two-phase I/O decode flow added to
 * {@link OptimizedParquetColumnIterator}. The main correctness invariants exercised here are:
 * <ul>
 *   <li>The output is identical to the single-phase late-materialization path on the same
 *       file and filter.</li>
 *   <li>Two-phase fetches strictly fewer projection-column bytes when the filter is selective,
 *       proving that page skipping is engaged.</li>
 *   <li>Fallbacks (local storage, no projection-only columns, dense survivors, all rows
 *       filtered out) all produce correct rows and don't leak the breaker reservation.</li>
 * </ul>
 */
public class TwoPhaseReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testTwoPhaseProducesSameRowsAsSinglePhase() throws Exception {
        // Build a file that is large enough relative to predicate column bytes to clear the 0.4
        // ratio gate: an INT64 id (predicate, ~8 bytes/row) and a sizeable BINARY label
        // (projection-only, large bytes/row). The filter keeps about 1% of rows.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("test_schema");

        byte[] parquetData = buildParquet(schema, 5_000, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            g.add("label", repeat('x', 256) + "_" + i);
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 50L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        CountingStorageObject syncObj = new CountingStorageObject(parquetData, false);
        ParquetFormatReader syncReader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        List<Page> singlePhasePages = readAllPages(syncReader, syncObj);

        CountingStorageObject asyncObj = new CountingStorageObject(parquetData, true);
        ParquetFormatReader asyncReader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        List<Page> twoPhasePages = readAllPages(asyncReader, asyncObj);

        // Both paths should produce the same number of surviving rows and the same id values.
        int singleRows = singlePhasePages.stream().mapToInt(Page::getPositionCount).sum();
        int twoPhaseRows = twoPhasePages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("two-phase row count", twoPhaseRows, equalTo(singleRows));
        assertThat("expected survivor count", twoPhaseRows, equalTo(50));

        Set<Long> singleIds = collectIds(singlePhasePages);
        Set<Long> twoIds = collectIds(twoPhasePages);
        assertEquals("id sets differ", singleIds, twoIds);
    }

    public void testTwoPhaseFetchesFewerProjectionBytesThanSinglePhase() throws Exception {
        // Selective filter on a small predicate column with a much larger projection column.
        // We expect two-phase to skip pages of the label column for filtered-out rows.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("test_schema");

        byte[] parquetData = buildParquet(schema, 10_000, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            g.add("label", repeat('z', 512) + "_" + i);
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 100L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        // Single-phase: pretend storage is local (no native async) so two-phase is bypassed.
        CountingStorageObject singlePhaseObj = new CountingStorageObject(parquetData, false);
        readAllPages(new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed), singlePhaseObj).forEach(Page::releaseBlocks);

        // Two-phase: storage advertises native async, enabling the two-phase path.
        CountingStorageObject twoPhaseObj = new CountingStorageObject(parquetData, true);
        readAllPages(new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed), twoPhaseObj).forEach(Page::releaseBlocks);

        // Two-phase should fetch strictly fewer bytes (Phase 1 saves predicate-only data, Phase 2
        // saves only surviving label pages). The exact ratio depends on dictionary + page layout
        // but selective filters should reliably trim well over 50%.
        long single = singlePhaseObj.totalBytesRead.get();
        long two = twoPhaseObj.totalBytesRead.get();
        assertThat(
            "two-phase should read fewer bytes than single-phase: " + two + " vs " + single,
            two,
            org.hamcrest.Matchers.lessThan(single)
        );
    }

    public void testTwoPhaseFallsBackToSinglePhaseWhenNoProjectionOnlyColumn() throws Exception {
        // When the only projected column is also a predicate column, two-phase has nothing to
        // save in Phase 2 — the gate rejects and the iterator falls back to the standard read
        // path. The reader is still constructed with the late-materialization *flag* enabled
        // ({@code lateMaterializationEnabled=true} via the default ctor), but the runtime
        // {@code lateMaterialization} decision inside {@code OptimizedParquetColumnIterator}
        // turns off because there are no projection-only columns. With late-mat off, the
        // parquet-mr filter only drives row-group / page-index pruning, not row-level filtering;
        // a single small row group survives entirely. The iterator must still read the file
        // end-to-end without throwing — that's the contract this test pins.
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = buildParquet(schema, 100, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 30L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        StorageObject obj = new CountingStorageObject(parquetData, true);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        List<Page> pages = readAllPages(reader, obj);

        // Reader returns all rows; the filter would have been applied by an upstream operator.
        int total = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("expected all rows when there's no late-mat opportunity", total, equalTo(100));
    }

    /**
     * High predicate-byte-ratio shape on native-async storage: predicate column dominates projected
     * bytes (the q22-on-ClickBench shape). After removing the file-level byte-ratio gate, late
     * materialization must still filter rows; the iterator's own 0.4 two-phase gate correctly keeps
     * the more expensive two-phase prefetch off, but the cheap late-mat decode-skip remains in
     * effect. The byte-traffic cross-check against a non-async {@link CountingStorageObject} pins
     * "two-phase did not engage" on the async run — the ratio between the two reads stays close to
     * 1, instead of the order-of-magnitude saving two-phase would produce on this 2.5%-selective
     * filter.
     */
    public void testLateMatFiresButTwoPhaseStaysOffWhenPredicateColumnDominates() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("wide_pred")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("narrow_proj")
            .named("test_schema");

        // 40-char padding + 200 rows lands inside parquet-mr's default row-group/page sizing in a
        // shape that emits a single row group, so the late-mat filter sees the whole batch. Larger
        // padding or row counts can shift the page layout enough to hide the regression.
        int rowCount = 200;
        String padding = "x".repeat(40);
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Group g = factory.newGroup();
            g.add("wide_pred", padding + "_pred_" + String.format(java.util.Locale.ROOT, "%03d", i));
            g.add("narrow_proj", (long) i);
            return g;
        });

        ReferenceAttribute predAttr = new ReferenceAttribute(Source.EMPTY, "wide_pred", DataType.KEYWORD);
        // Lex > "padding_pred_194": survivors are i in {195..199}, 5 of 200 rows (~2.5% selective).
        org.apache.lucene.util.BytesRef threshold = new org.apache.lucene.util.BytesRef(padding + "_pred_194");
        Expression filter = new GreaterThan(Source.EMPTY, predAttr, new Literal(Source.EMPTY, threshold, DataType.KEYWORD), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        CountingStorageObject asyncObj = new CountingStorageObject(parquetData, true);
        List<Page> asyncPages = readAllPages(new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed), asyncObj);

        int asyncRows = asyncPages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("late-mat must fire and retain only matching rows", asyncRows, equalTo(5));

        // Sanity-check the surviving row IDs (195..199) — guards against a wrong-column or
        // wrong-comparator regression slipping through under the 5-row count assertion alone.
        Set<Long> survivors = new HashSet<>();
        for (Page p : asyncPages) {
            LongBlock proj = p.getBlock(1);
            for (int i = 0; i < proj.getPositionCount(); i++) {
                if (proj.isNull(i) == false) {
                    survivors.add(proj.getLong(i));
                }
            }
        }
        assertEquals("expected tail rows 195..199 to survive", Set.of(195L, 196L, 197L, 198L, 199L), survivors);

        // Cross-check on byte traffic: a non-async storage object cannot use two-phase, so its byte
        // count is the late-mat-without-two-phase baseline. If two-phase had engaged on the async
        // path, async bytes would be a small fraction of sync bytes (the projection column would be
        // skipped for ~97.5% of rows). We assert async is at least 75% of sync — generous bound that
        // fails clearly if two-phase ever engages here, while tolerating small async/sync wrapper
        // overhead asymmetries.
        CountingStorageObject syncObj = new CountingStorageObject(parquetData, false);
        readAllPages(new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed), syncObj).forEach(Page::releaseBlocks);

        long async = asyncObj.totalBytesRead.get();
        long sync = syncObj.totalBytesRead.get();
        assertTrue(
            "two-phase must stay off at high predicate-byte ratio (async=" + async + ", sync=" + sync + ")",
            async >= (sync * 3) / 4
        );
    }

    public void testTwoPhaseHandlesAllFilteredOutRowGroup() throws Exception {
        // A small file where a selective filter removes every row; verifies the all-filtered
        // path returns no rows and does not leave the iterator hung on a stale state.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("test_schema");

        byte[] parquetData = buildParquet(schema, 200, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            g.add("label", repeat('a', 128));
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        // No row has id > 10000; every row is filtered.
        Expression filter = new GreaterThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 10_000L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        StorageObject obj = new CountingStorageObject(parquetData, true);
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        List<Page> pages = readAllPages(reader, obj);
        int total = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("expect zero rows after impossible filter", total, equalTo(0));
    }

    /**
     * Regression test for the NPE in {@code nextTwoPhaseBatch} when projection {@link PageColumnReader}s
     * with {@link RowRanges} skip entire pages, producing fewer rows than {@code readBatchFiltered}
     * expects. This requires: small page size (many pages per row group), a sparse filter that
     * eliminates whole pages but not entire row groups, and {@code nativeAsync=true} to activate
     * two-phase I/O.
     *
     * Before the fix (readBatchSparse), this test crashes with:
     * {@code NullPointerException: Cannot invoke "...BytesRefArray$LongOffsets.get(long)" because "this.longOffsets" is null}
     */
    public void testTwoPhaseSparseFilterWithPageSkipping() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("test_schema");

        // 10_000 rows with a small page size (64 bytes) creates many pages per row group.
        // id values 0..9999; the filter keeps only id < 20 (~0.2% selectivity), which
        // leaves entire pages with no survivors, triggering page skipping in loadNextPage().
        int rowCount = 10_000;
        byte[] parquetData = buildParquetWithPageSize(schema, rowCount, 64, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Group g = factory.newGroup();
            g.add("id", (long) i);
            g.add("label", repeat('z', 128) + "_" + i);
            return g;
        });

        int expectedSurvivors = 20;
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, (long) expectedSurvivors, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        // nativeAsync=true triggers the two-phase path
        CountingStorageObject asyncObj = new CountingStorageObject(parquetData, true);
        ParquetFormatReader asyncReader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        List<Page> twoPhasePages = readAllPages(asyncReader, asyncObj);

        int twoPhaseRows = twoPhasePages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("two-phase should produce exactly the survivors", twoPhaseRows, equalTo(expectedSurvivors));

        Set<Long> ids = collectIds(twoPhasePages);
        for (int i = 0; i < expectedSurvivors; i++) {
            assertTrue("expected id " + i + " in result set", ids.contains((long) i));
        }
        assertThat("no extra ids", ids.size(), equalTo(expectedSurvivors));

        // Cross-check against single-phase
        CountingStorageObject syncObj = new CountingStorageObject(parquetData, false);
        ParquetFormatReader syncReader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        List<Page> singlePhasePages = readAllPages(syncReader, syncObj);
        int singlePhaseRows = singlePhasePages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("single-phase and two-phase row counts must match", twoPhaseRows, equalTo(singlePhaseRows));
    }

    private byte[] buildParquetWithPageSize(
        MessageType schema,
        int rowCount,
        int pageSize,
        java.util.function.IntFunction<Group> rowFactory
    ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile out = buildOutputFile(baos);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(out)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withPageSize(pageSize)
                .withConf(new PlainParquetConfiguration())
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                writer.write(rowFactory.apply(i));
            }
        }
        return baos.toByteArray();
    }

    private byte[] buildParquet(MessageType schema, int rowCount, java.util.function.IntFunction<Group> rowFactory) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile out = buildOutputFile(baos);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(out)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withConf(new PlainParquetConfiguration())
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                writer.write(rowFactory.apply(i));
            }
        }
        return baos.toByteArray();
    }

    private static OutputFile buildOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long pos = 0;

                    @Override
                    public long getPos() {
                        return pos;
                    }

                    @Override
                    public void write(int b) {
                        baos.write(b);
                        pos++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        baos.write(b, off, len);
                        pos += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    private List<Page> readAllPages(ParquetFormatReader reader, StorageObject obj) throws IOException {
        try (CloseableIterator<Page> it = reader.read(obj, FormatReadContext.builder().batchSize(1024).build())) {
            List<Page> pages = new ArrayList<>();
            while (it.hasNext()) {
                pages.add(it.next());
            }
            return pages;
        }
    }

    private static String repeat(char c, int n) {
        char[] arr = new char[n];
        java.util.Arrays.fill(arr, c);
        return new String(arr);
    }

    private static Set<Long> collectIds(List<Page> pages) {
        Set<Long> out = new HashSet<>();
        for (Page p : pages) {
            // Schema layout: id is the first long column we projected; with the default no-projection
            // path that means block 0.
            LongBlock block = p.getBlock(0);
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i) == false) {
                    out.add(block.getLong(i));
                }
            }
        }
        return out;
    }

    /**
     * In-memory {@link StorageObject} that counts the bytes read across all stream and async
     * read calls. Used to assert that two-phase fetches strictly fewer projection bytes than
     * single-phase. When {@code nativeAsync} is true the object reports
     * {@link StorageObject#supportsNativeAsync()} as true so two-phase activates; otherwise it
     * stays on the single-phase path.
     */
    private static final class CountingStorageObject implements StorageObject {
        private final byte[] data;
        private final boolean nativeAsync;
        final AtomicLong totalBytesRead = new AtomicLong();

        CountingStorageObject(byte[] data, boolean nativeAsync) {
            this.data = data;
            this.nativeAsync = nativeAsync;
        }

        @Override
        public InputStream newStream() {
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream newStream(long position, long length) {
            int pos = (int) position;
            int len = (int) Math.min(length, data.length - position);
            totalBytesRead.addAndGet(len);
            return new ByteArrayInputStream(data, pos, len);
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.now();
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return StoragePath.of("memory://test.parquet");
        }

        @Override
        public boolean supportsNativeAsync() {
            return nativeAsync;
        }

        @Override
        public void readBytesAsync(long position, long length, Executor executor, ActionListener<ByteBuffer> listener) {
            // Run inline like the default sync wrapper but go through our stream-based newStream
            // path so the byte counter reflects the read. We mimic StorageObject's default async
            // implementation: count once, copy once.
            try (InputStream stream = newStream(position, length)) {
                byte[] bytes = stream.readAllBytes();
                listener.onResponse(ByteBuffer.wrap(bytes));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }
}
