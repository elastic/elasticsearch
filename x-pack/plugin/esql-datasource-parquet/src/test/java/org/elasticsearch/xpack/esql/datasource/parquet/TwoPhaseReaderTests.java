/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
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
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

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

    public void testFilterEvaluatedWhenNoProjectionOnlyColumn() throws Exception {
        // When every projected column is also a predicate column, two-phase I/O does not activate
        // (nothing to defer), but late-materialization filter evaluation MUST still run: pushed
        // YES expressions depend on the reader evaluating the filter since FilterExec is removed.
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

        int total = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("filter must be evaluated even without projection-only columns", total, equalTo(30));
    }

    /**
     * High predicate-byte-ratio shape on native-async storage: predicate column dominates projected
     * bytes. After removing the file-level byte-ratio gate, late
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

    /**
     * Regression test: when every projected column is also a predicate column (no projection-only
     * columns), the late-materialization gate ({@code hasProjectionOnlyColumns}) was false, causing
     * pushed YES expressions (WildcardLike) to never be evaluated. FilterExec was already removed
     * from the plan, so all rows leaked through unfiltered.
     */
    public void testLikeFilterEvaluatedWhenAllColumnsArePredicate() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("test_schema");

        int rowCount = 100;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Group g = factory.newGroup();
            if (i % 10 == 0) {
                g.add("url", "http://www.google.com/page" + i);
            } else {
                g.add("url", "http://www.example.com/page" + i);
            }
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression filter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        // Verify that LIKE-only produces a null FilterPredicate (no native Parquet translation)
        assertNull("LIKE must not translate to a Parquet FilterPredicate", pushed.toFilterPredicate(schema));

        // Test both sync and async paths — the trivially-passes shortcut must not leak rows
        // when filterPredicate is null regardless of storage type
        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            List<Page> pages = readAllPages(reader, obj);

            int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
            assertThat("LIKE filter must be evaluated (nativeAsync=" + nativeAsync + ")", totalRows, equalTo(10));
        }
    }

    /**
     * Oracle-validated: LIKE filter column NOT in the output projection (simulates
     * {@code WHERE url LIKE "*google*" | STATS COUNT(*)} where the planner projects only
     * a non-predicate column). The reader must augment the projection to include the
     * predicate column so the filter can be evaluated.
     */
    public void testLikeFilterColumnNotInProjectionVsOracle() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("test_schema");

        int rowCount = 200;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            g.add("url", (i % 10 == 0) ? "http://google.com/" + i : "http://example.com/" + i);
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression filter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        // Oracle
        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> g.getString("url", 0).contains("google"));
        assertThat("oracle count", oracleCount, equalTo(20));

        // Project only 'id' — the predicate column 'url' is NOT in the projection.
        // The reader must augment the projection to include 'url' for filter evaluation.
        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("id")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "filter column not in projection (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Oracle-validated: LIKE + comparison with NO projection-only columns (every projected
     * column is also a predicate column). Both {@code url} and {@code searchphrase} are
     * predicate AND projected — no column is projection-only.
     * The late-mat gate must still evaluate both predicates.
     * Validated against apache-mr GroupReader + Java evaluator (independent of our engine).
     */
    public void testLikeAndComparisonNoProjectionOnlyColumnsVsOracle() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("searchphrase")
            .named("test_schema");

        int rowCount = 1000;
        byte[] parquetData = buildParquetWithPageSize(schema, rowCount, 128, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("url", (i % 20 == 0) ? "http://www.google.com/" + i : "http://example.com/" + i);
            g.add("searchphrase", (i % 3 == 0) ? "" : "query_" + i);
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        ReferenceAttribute spAttr = new ReferenceAttribute(Source.EMPTY, "searchphrase", DataType.KEYWORD);
        Expression likeFilter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        Expression neFilter = new NotEquals(Source.EMPTY, spAttr, new Literal(Source.EMPTY, new BytesRef(""), DataType.KEYWORD), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(likeFilter, neFilter));

        // Oracle: count matching rows via apache-mr (independent stack)
        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> {
            String url = g.getString("url", 0);
            String sp = g.getString("searchphrase", 0);
            return url.contains("google") && sp.isEmpty() == false;
        });
        assertTrue("oracle must find some survivors", oracleCount > 0);

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            List<Page> pages = readAllPages(reader, obj);
            int engineCount = pages.stream().mapToInt(Page::getPositionCount).sum();

            assertThat("engine (nativeAsync=" + nativeAsync + ") must match oracle count", engineCount, equalTo(oracleCount));
        }
    }

    /**
     * Oracle-validated: multi-row-group file with LIKE as the sole predicate on a SINGLE column
     * (no projection-only columns — url is both the predicate and the only projected column).
     * Uses a small row group size to force multiple row groups. Validated against apache-mr.
     */
    public void testMultiRowGroupLikeOnlyVsOracle() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("test_schema");

        int rowCount = 2000;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile out = buildOutputFile(baos);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(out)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withPageSize(64)
                .withRowGroupSize(4096L)
                .withConf(new PlainParquetConfiguration())
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                SimpleGroupFactory f = new SimpleGroupFactory(schema);
                Group g = f.newGroup();
                g.add("url", (i % 50 == 0) ? "http://google.com/search?q=" + i : "http://example.org/" + i);
                writer.write(g);
            }
        }
        byte[] parquetData = baos.toByteArray();

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression filter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        // Oracle: count via apache-mr (independent stack)
        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> g.getString("url", 0).contains("google"));
        assertThat("oracle must find 40 survivors (every 50th of 2000)", oracleCount, equalTo(40));

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            List<Page> pages = readAllPages(reader, obj);
            int engineCount = pages.stream().mapToInt(Page::getPositionCount).sum();

            assertThat("multi-RG LIKE-only (nativeAsync=" + nativeAsync + ") must match oracle", engineCount, equalTo(oracleCount));
        }
    }

    /**
     * Oracle-validated: trivially-passes shortcut should correctly fire when stats prove all
     * rows match AND the filter is fully translatable (no LIKE). This verifies the optimization
     * still works after the guards were tightened. A GreaterThanOrEqual(id, 0) filter trivially
     * passes for all row groups (all values are 0-999). Validated against apache-mr.
     */
    public void testTriviallyPassesCorrectlyFiresVsOracle() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("payload")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquetWithPageSize(schema, rowCount, 128, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            g.add("payload", "data_" + i);
            return g;
        });

        // id >= 0 trivially passes for all rows (values are 0-499)
        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new GreaterThanOrEqual(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 0L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        // Oracle: all 500 rows should survive
        Set<Long> oracleIds = readIdsWithApacheMrOracle(parquetData, schema, g -> g.getLong("id", 0) >= 0);

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            List<Page> pages = readAllPages(reader, obj);
            Set<Long> engineIds = collectIds(pages);

            assertEquals("trivially-passing filter (nativeAsync=" + nativeAsync + ") must return all rows", oracleIds, engineIds);
            assertThat("all 500 rows must survive", engineIds.size(), equalTo(500));
        }
    }

    /**
     * No overlap between filter and projection columns: filter on {@code url} (LIKE "*google*"),
     * projection on {@code [id, title]} only. The filter column is NOT in the projection.
     * The reader must augment the projection to include {@code url} so the filter can be evaluated.
     * Oracle-validated against apache-mr.
     */
    public void testFilterAndProjectionDisjoint() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("title")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("status")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            g.add("url", (i % 10 == 0) ? "http://google.com/" + i : "http://example.com/" + i);
            g.add("title", "title_" + (i % 50));
            g.add("status", (long) (i % 5));
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression filter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> g.getString("url", 0).contains("google"));
        assertThat("oracle count", oracleCount, equalTo(50));

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("id", "title")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "disjoint filter/projection (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Partial overlap between filter and projection columns: filter on {@code url}
     * (LIKE "*google*") AND {@code status < 2}, projection on {@code [id, status]}.
     * {@code status} is in both filter and projection; {@code url} is only in filter.
     * The reader must augment the projection with {@code url}. Oracle-validated.
     */
    public void testFilterAndProjectionPartialOverlap() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("title")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("status")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            g.add("url", (i % 10 == 0) ? "http://google.com/" + i : "http://example.com/" + i);
            g.add("title", "title_" + (i % 50));
            g.add("status", (long) (i % 5));
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.LONG);
        Expression likeFilter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        Expression ltFilter = new LessThan(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, 2L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(likeFilter, ltFilter));

        int oracleCount = countWithApacheMrOracle(
            parquetData,
            schema,
            g -> g.getString("url", 0).contains("google") && g.getLong("status", 0) < 2
        );
        assertTrue("oracle must find some survivors", oracleCount > 0);

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("id", "status")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "partial overlap filter/projection (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Full overlap between filter and projection columns: filter on {@code url}
     * (LIKE "*google*") AND {@code status < 2}, projection on {@code [url, status]}.
     * All filter columns are already projected. No augmentation needed. Oracle-validated.
     */
    public void testFilterAndProjectionFullOverlap() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("title")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("status")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            g.add("url", (i % 10 == 0) ? "http://google.com/" + i : "http://example.com/" + i);
            g.add("title", "title_" + (i % 50));
            g.add("status", (long) (i % 5));
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.LONG);
        Expression likeFilter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        Expression ltFilter = new LessThan(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, 2L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(likeFilter, ltFilter));

        int oracleCount = countWithApacheMrOracle(
            parquetData,
            schema,
            g -> g.getString("url", 0).contains("google") && g.getLong("status", 0) < 2
        );
        assertTrue("oracle must find some survivors", oracleCount > 0);

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("url", "status")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "full overlap filter/projection (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Filter on projected column, aggregate on non-projected: filter on {@code id < 100},
     * projection on {@code [id]}. All filter columns are already projected. Oracle: 100
     * survivors. Simulates {@code WHERE id < 100 | STATS COUNT(*)}.
     */
    public void testFilterOnProjectedColumnQueryOnNonProjected() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("title")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("status")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            g.add("url", (i % 10 == 0) ? "http://google.com/" + i : "http://example.com/" + i);
            g.add("title", "title_" + (i % 50));
            g.add("status", (long) (i % 5));
            return g;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 100L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> g.getLong("id", 0) < 100);
        assertThat("oracle count", oracleCount, equalTo(100));

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("id")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "filter on projected column (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Multiple filters, none projected: filter on {@code url LIKE "*google*"} AND
     * {@code status < 3}, projection on {@code [id, title]}. Neither filter column is in the
     * projection. The reader must augment with both {@code url} and {@code status}.
     * Oracle-validated.
     */
    public void testMultipleFiltersOnNonProjectedColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("title")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("status")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            g.add("url", (i % 10 == 0) ? "http://google.com/" + i : "http://example.com/" + i);
            g.add("title", "title_" + (i % 50));
            g.add("status", (long) (i % 5));
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.LONG);
        Expression likeFilter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        Expression ltFilter = new LessThan(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, 3L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(likeFilter, ltFilter));

        int oracleCount = countWithApacheMrOracle(
            parquetData,
            schema,
            g -> g.getString("url", 0).contains("google") && g.getLong("status", 0) < 3
        );
        assertTrue("oracle must find some survivors", oracleCount > 0);

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("id", "title")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "multiple filters on non-projected columns (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Dictionary short-circuit for high-cardinality LIKE: 500 rows with unique URLs (density
     * ratio ~1.0, well below the old isDense() threshold of 2.0). Every 25th row contains
     * "google". The relaxed threshold (positionCount >= 10) enables dictionary evaluation
     * even when dictSize ~= rowCount. Oracle-validated.
     */
    public void testDictionaryShortCircuitForHighCardinalityLike() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            if (i % 25 == 0) {
                g.add("url", "http://google.com/search?q=" + i);
            } else {
                g.add("url", "http://site" + i + ".com/page");
            }
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression filter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> g.getString("url", 0).contains("google"));
        assertThat("oracle count", oracleCount, equalTo(20));

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("id", "url")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "high-cardinality LIKE dict short-circuit (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Dictionary short-circuit for high-cardinality Equals: same high-cardinality URL data
     * as the LIKE test. Equals matches exactly one row. Verifies Equals still works correctly
     * with the relaxed dictionary threshold. Oracle-validated.
     */
    public void testDictionaryShortCircuitForHighCardinalityEquals() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            if (i % 25 == 0) {
                g.add("url", "http://google.com/search?q=" + i);
            } else {
                g.add("url", "http://site" + i + ".com/page");
            }
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression filter = new Equals(
            Source.EMPTY,
            urlAttr,
            new Literal(Source.EMPTY, new BytesRef("http://google.com/search?q=100"), DataType.KEYWORD),
            null
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> g.getString("url", 0).equals("http://google.com/search?q=100"));
        assertThat("oracle count", oracleCount, equalTo(1));

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("id", "url")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "high-cardinality Equals dict short-circuit (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Dictionary short-circuit for high-cardinality StartsWith: same high-cardinality URL data.
     * StartsWith matches the 20 google URLs. Oracle-validated.
     */
    public void testDictionaryShortCircuitForHighCardinalityStartsWith() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("test_schema");

        int rowCount = 500;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            if (i % 25 == 0) {
                g.add("url", "http://google.com/search?q=" + i);
            } else {
                g.add("url", "http://site" + i + ".com/page");
            }
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression filter = new StartsWith(
            Source.EMPTY,
            urlAttr,
            new Literal(Source.EMPTY, new BytesRef("http://google"), DataType.KEYWORD)
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> g.getString("url", 0).startsWith("http://google"));
        assertThat("oracle count", oracleCount, equalTo(20));

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            FormatReadContext ctx = FormatReadContext.builder().batchSize(1024).projectedColumns(List.of("id", "url")).build();
            try (CloseableIterator<Page> it = reader.read(obj, ctx)) {
                int engineCount = 0;
                while (it.hasNext()) {
                    Page page = it.next();
                    engineCount += page.getPositionCount();
                    page.releaseBlocks();
                }
                assertThat(
                    "high-cardinality StartsWith dict short-circuit (nativeAsync=" + nativeAsync + ") must match oracle",
                    engineCount,
                    equalTo(oracleCount)
                );
            }
        }
    }

    /**
     * Tiny block (5 rows, below the >= 10 threshold): dictionary short-circuit is skipped
     * but the per-row evaluation path must still produce correct results. Oracle-validated.
     */
    public void testDictionaryShortCircuitTinyBlockSkipped() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .named("test_schema");

        int rowCount = 5;
        byte[] parquetData = buildParquet(schema, rowCount, i -> {
            SimpleGroupFactory f = new SimpleGroupFactory(schema);
            Group g = f.newGroup();
            g.add("id", (long) i);
            g.add("url", (i == 2) ? "http://google.com/page" : "http://example.com/" + i);
            return g;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        Expression filter = new WildcardLike(Source.EMPTY, urlAttr, new WildcardPattern("*google*"));
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        int oracleCount = countWithApacheMrOracle(parquetData, schema, g -> g.getString("url", 0).contains("google"));
        assertThat("oracle count", oracleCount, equalTo(1));

        for (boolean nativeAsync : new boolean[] { false, true }) {
            CountingStorageObject obj = new CountingStorageObject(parquetData, nativeAsync);
            ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
            List<Page> pages = readAllPages(reader, obj);
            int engineCount = pages.stream().mapToInt(Page::getPositionCount).sum();

            assertThat("tiny block LIKE (nativeAsync=" + nativeAsync + ") must match oracle", engineCount, equalTo(oracleCount));
        }
    }

    /**
     * Oracle: reads ALL rows from a Parquet file via apache-mr's {@link ParquetReader} (independent
     * of our engine), applies a Java predicate, and returns the count of matching rows. This is
     * the authoritative answer — any disagreement means our engine has a bug.
     */
    private int countWithApacheMrOracle(byte[] parquetData, MessageType schema, java.util.function.Predicate<Group> filter)
        throws IOException {
        StorageObject storage = new CountingStorageObject(parquetData, false);
        ParquetReader.Builder<Group> builder = new ParquetReader.Builder<Group>(
            new ParquetStorageObjectAdapter(storage),
            new PlainParquetConfiguration()
        ) {
            @Override
            protected org.apache.parquet.hadoop.api.ReadSupport<Group> getReadSupport() {
                return new GroupReadSupport();
            }
        };
        int count = 0;
        try (ParquetReader<Group> mrReader = builder.build()) {
            Group g;
            while ((g = mrReader.read()) != null) {
                if (filter.test(g)) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Oracle variant that returns id sets (requires an {@code id} column of type INT64).
     */
    private Set<Long> readIdsWithApacheMrOracle(byte[] parquetData, MessageType schema, java.util.function.Predicate<Group> filter)
        throws IOException {
        StorageObject storage = new CountingStorageObject(parquetData, false);
        ParquetReader.Builder<Group> builder = new ParquetReader.Builder<Group>(
            new ParquetStorageObjectAdapter(storage),
            new PlainParquetConfiguration()
        ) {
            @Override
            protected org.apache.parquet.hadoop.api.ReadSupport<Group> getReadSupport() {
                return new GroupReadSupport();
            }
        };
        Set<Long> ids = new HashSet<>();
        try (ParquetReader<Group> mrReader = builder.build()) {
            Group g;
            while ((g = mrReader.read()) != null) {
                if (filter.test(g)) {
                    ids.add(g.getLong("id", 0));
                }
            }
        }
        return ids;
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
