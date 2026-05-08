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
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
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
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests verifying that the optimized reader produces correct results when
 * filters are active and that page-level RowRanges filtering actually reduces I/O.
 * Sorted data with small page sizes ensures ColumnIndex is useful for page skipping.
 */
public class OptimizedFilteredReaderTests extends ESTestCase {

    private static final int TOTAL_ROWS = 1000;

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    /**
     * Filtered optimized reads produce the same rows as filtered baseline reads.
     */
    public void testFilteredOptimizedMatchesBaseline() throws IOException {
        byte[] parquetData = createSortedIntFile();
        FilterPredicate filter = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), 200),
            FilterApi.lt(FilterApi.intColumn("id"), 400)
        );

        List<Page> baselinePages = readWithFilter(parquetData, filter, false);
        List<Page> optimizedPages = readWithFilter(parquetData, filter, true);
        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Equality filter on sorted data: optimized path should produce same results as baseline.
     */
    public void testEqualityFilterParity() throws IOException {
        byte[] parquetData = createSortedIntFile();
        FilterPredicate filter = FilterApi.eq(FilterApi.intColumn("id"), 500);

        List<Page> baselinePages = readWithFilter(parquetData, filter, false);
        List<Page> optimizedPages = readWithFilter(parquetData, filter, true);
        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * OR of equality filters: optimized path should produce same results as baseline.
     */
    public void testOrEqualityFilterParity() throws IOException {
        byte[] parquetData = createSortedIntFile();
        FilterPredicate filter = FilterApi.or(FilterApi.eq(FilterApi.intColumn("id"), 100), FilterApi.eq(FilterApi.intColumn("id"), 900));

        List<Page> baselinePages = readWithFilter(parquetData, filter, false);
        List<Page> optimizedPages = readWithFilter(parquetData, filter, true);
        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Optimized path with no filter still works (regression check).
     */
    public void testOptimizedWithoutFilter() throws IOException {
        byte[] parquetData = createSortedIntFile();

        List<Page> baselinePages = readWithFilter(parquetData, null, false);
        List<Page> optimizedPages = readWithFilter(parquetData, null, true);
        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Multi-column file with filter: optimized path produces correct results
     * for projected columns alongside the filtered column.
     */
    public void testMultiColumnFilteredParity() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(INT32)
            .named("id")
            .required(INT64)
            .named("big_id")
            .required(DOUBLE)
            .named("score")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("multi_col_test");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < TOTAL_ROWS; i++) {
                groups.add(
                    factory.newGroup().append("id", i).append("big_id", (long) i * 1000).append("score", i * 1.5).append("name", "row_" + i)
                );
            }
            return groups;
        });

        FilterPredicate filter = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), 100),
            FilterApi.lt(FilterApi.intColumn("id"), 300)
        );

        List<Page> baselinePages = readWithFilter(parquetData, filter, false);
        List<Page> optimizedPages = readWithFilter(parquetData, filter, true);
        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Filter that eliminates all rows: both paths should produce no data.
     */
    public void testFilterEliminatesAllRows() throws IOException {
        byte[] parquetData = createSortedIntFile();
        FilterPredicate filter = FilterApi.eq(FilterApi.intColumn("id"), -1);

        List<Page> baselinePages = readWithFilter(parquetData, filter, false);
        List<Page> optimizedPages = readWithFilter(parquetData, filter, true);

        int baselineRows = baselinePages.stream().mapToInt(Page::getPositionCount).sum();
        int optimizedRows = optimizedPages.stream().mapToInt(Page::getPositionCount).sum();
        assertEquals(baselineRows, optimizedRows);
        assertEquals(0, optimizedRows);
        assertEquals("No pages should be emitted when all rows eliminated", 0, optimizedPages.size());
    }

    /**
     * Filter that matches all rows: both paths should produce same data.
     */
    public void testFilterMatchesAllRows() throws IOException {
        byte[] parquetData = createSortedIntFile();
        FilterPredicate filter = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), 0),
            FilterApi.lt(FilterApi.intColumn("id"), TOTAL_ROWS)
        );

        List<Page> baselinePages = readWithFilter(parquetData, filter, false);
        List<Page> optimizedPages = readWithFilter(parquetData, filter, true);
        assertPagesEqual(baselinePages, optimizedPages);
    }

    // --- PushedExpressions path tests (exercises ColumnIndexRowRangesComputer → RowRanges) ---

    /**
     * Range filter via PushedExpressions: exercises the RowRanges code path and
     * verifies parity against both the baseline reader and the FilterCompat optimized reader.
     */
    public void testPushedExpressionsRangeFilterParity() throws IOException {
        byte[] parquetData = createSortedIntFile();

        FilterPredicate filterCompat = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), 200),
            FilterApi.lt(FilterApi.intColumn("id"), 400)
        );
        Expression esqlFilter = new org.elasticsearch.xpack.esql.expression.predicate.logical.And(
            Source.EMPTY,
            new GreaterThanOrEqual(Source.EMPTY, intAttr("id"), intLit(200), null),
            new LessThan(Source.EMPTY, intAttr("id"), intLit(400), null)
        );

        List<Page> baselinePages = readWithFilter(parquetData, filterCompat, false);
        List<Page> optimizedCompatPages = readWithFilter(parquetData, filterCompat, true);
        List<Page> optimizedPushedPages = readWithPushedExpressions(parquetData, esqlFilter);

        assertPagesEqual(baselinePages, optimizedCompatPages);
        assertPagesEqual(baselinePages, optimizedPushedPages);
    }

    /**
     * Equality filter via PushedExpressions: verifies the RowRanges path produces
     * identical results to both baseline and FilterCompat optimized paths.
     */
    public void testPushedExpressionsEqualityFilterParity() throws IOException {
        byte[] parquetData = createSortedIntFile();

        FilterPredicate filterCompat = FilterApi.eq(FilterApi.intColumn("id"), 500);
        Expression esqlFilter = new Equals(Source.EMPTY, intAttr("id"), intLit(500), null);

        List<Page> baselinePages = readWithFilter(parquetData, filterCompat, false);
        List<Page> optimizedCompatPages = readWithFilter(parquetData, filterCompat, true);
        List<Page> optimizedPushedPages = readWithPushedExpressions(parquetData, esqlFilter);

        assertPagesEqual(baselinePages, optimizedCompatPages);
        assertPagesEqual(baselinePages, optimizedPushedPages);
    }

    /**
     * Filter eliminating all rows via PushedExpressions: RowRanges path should also produce zero rows.
     */
    public void testPushedExpressionsEliminatesAllRows() throws IOException {
        byte[] parquetData = createSortedIntFile();

        Expression esqlFilter = new Equals(Source.EMPTY, intAttr("id"), intLit(-1), null);

        List<Page> pushedPages = readWithPushedExpressions(parquetData, esqlFilter);
        int totalRows = pushedPages.stream().mapToInt(Page::getPositionCount).sum();
        assertEquals(0, totalRows);
    }

    /**
     * Filter matching all rows via PushedExpressions: RowRanges path should return every row.
     */
    public void testPushedExpressionsMatchesAllRows() throws IOException {
        byte[] parquetData = createSortedIntFile();

        Expression esqlFilter = new org.elasticsearch.xpack.esql.expression.predicate.logical.And(
            Source.EMPTY,
            new GreaterThanOrEqual(Source.EMPTY, intAttr("id"), intLit(0), null),
            new LessThan(Source.EMPTY, intAttr("id"), intLit(TOTAL_ROWS), null)
        );

        List<Page> baselinePages = readWithFilter(parquetData, null, false);
        List<Page> pushedPages = readWithPushedExpressions(parquetData, esqlFilter);
        assertPagesEqual(baselinePages, pushedPages);
    }

    /**
     * Multi-column file with PushedExpressions filter: verifies that all projected columns
     * produce correct values when the RowRanges path is active.
     */
    public void testPushedExpressionsMultiColumnParity() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(INT32)
            .named("id")
            .required(INT64)
            .named("big_id")
            .required(DOUBLE)
            .named("score")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("multi_col_test");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < TOTAL_ROWS; i++) {
                groups.add(
                    factory.newGroup().append("id", i).append("big_id", (long) i * 1000).append("score", i * 1.5).append("name", "row_" + i)
                );
            }
            return groups;
        });

        FilterPredicate filterCompat = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), 100),
            FilterApi.lt(FilterApi.intColumn("id"), 300)
        );
        Expression esqlFilter = new org.elasticsearch.xpack.esql.expression.predicate.logical.And(
            Source.EMPTY,
            new GreaterThanOrEqual(Source.EMPTY, intAttr("id"), intLit(100), null),
            new LessThan(Source.EMPTY, intAttr("id"), intLit(300), null)
        );

        List<Page> baselinePages = readWithFilter(parquetData, filterCompat, false);
        List<Page> pushedPages = readWithPushedExpressions(parquetData, esqlFilter);
        assertPagesEqual(baselinePages, pushedPages);
    }

    /**
     * Trivially-passes guard: a multi-column projection with a filter that the row-group stats
     * prove every row satisfies. The optimized reader should bypass late-materialization filter
     * evaluation entirely for every row group and still produce results identical to the
     * baseline. Exercises the {@link TriviallyPassesChecker} integration.
     */
    public void testPushedExpressionsTriviallyPassingFilterParity() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(INT32)
            .named("id")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("trivial_pass_test");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < TOTAL_ROWS; i++) {
                groups.add(factory.newGroup().append("id", i).append("name", "row_" + i));
            }
            return groups;
        });

        // id ranges [0, TOTAL_ROWS): the filter `id >= 0 AND id < TOTAL_ROWS` is trivially
        // satisfied by every row group's stats. With `name` also in the projection (a non-predicate
        // column), late materialization is enabled; the trivially-passes guard should kick in and
        // bypass per-row filter evaluation.
        FilterPredicate filterCompat = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), 0),
            FilterApi.lt(FilterApi.intColumn("id"), TOTAL_ROWS)
        );
        Expression esqlFilter = new org.elasticsearch.xpack.esql.expression.predicate.logical.And(
            Source.EMPTY,
            new GreaterThanOrEqual(Source.EMPTY, intAttr("id"), intLit(0), null),
            new LessThan(Source.EMPTY, intAttr("id"), intLit(TOTAL_ROWS), null)
        );

        List<Page> baselinePages = readWithFilter(parquetData, filterCompat, false);
        List<Page> pushedPages = readWithPushedExpressions(parquetData, esqlFilter);
        assertPagesEqual(baselinePages, pushedPages);
    }

    /**
     * Regression test for the trivially-passes shortcut leak: a query that mixes a
     * Pushability.YES conjunct that does not translate to a Parquet FilterPredicate
     * ({@code WildcardLike}) with a Pushability.RECHECK conjunct that does translate AND
     * is satisfied by row-group statistics for every row ({@code status = 200}).
     *
     * <p>Without the {@code hasYesConjunctOutsideFilterPredicate} guard in
     * {@link ParquetFormatReader}, the trivially-passes shortcut would skip late-mat for
     * every row group (because the FilterPredicate translation only contains {@code status = 200}
     * and stats prove it). With LIKE pushed as YES, {@code FilterExec} no longer re-applies
     * the predicate downstream, so the unfiltered rows would leak to the user. The guard
     * suppresses the shortcut whenever a YES conjunct is absent from the FilterPredicate,
     * forcing late-mat to evaluate the LIKE itself.
     *
     * <p><b>DO NOT WEAKEN OR REMOVE THIS TEST.</b> The combination of (LIKE as YES, status as
     * RECHECK, status stats-trivial across every row group) is the exact shape that produced
     * silent wrong-results when the YES promotion of {@code WildcardLike} landed without an
     * audit of the trivially-passes shortcut. Changes that "simplify" the shortcut, the
     * helper {@link ParquetPushedExpressions#hasYesConjunctOutsideFilterPredicate}, or the
     * guard call site in {@link ParquetFormatReader} MUST keep this test passing — the
     * symptom is silent over-inclusion (returns {@code rows} instead of {@code rows / 2}).
     * Companion planner-layer tests in {@code ParquetFilterPushdownSupportTests} and unit
     * tests in {@code ParquetPushedExpressionsTests} must also stay green.
     */
    public void testPushedExpressionsLikeWithStatsTrivialEqDoesNotLeak() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("url")
            .required(INT64)
            .named("status")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("like_with_eq_test");

        int rows = 200;
        // Every row has status = 200 (so stats prove status == 200 for the whole file).
        // Half the rows match LIKE "*google*"; the other half don't. Without the guard the
        // optimized reader would return all rows; with the guard it returns only the matches.
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                String url = (i % 2 == 0) ? "https://www.google.com/search?q=" + i : "https://example.org/page?id=" + i;
                groups.add(factory.newGroup().append("url", url).append("status", 200L).append("label", "label_padding_chunk_" + i));
            }
            return groups;
        });

        ReferenceAttribute urlAttr = new ReferenceAttribute(Source.EMPTY, "url", DataType.KEYWORD);
        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.LONG);
        Expression like = new org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike(
            Source.EMPTY,
            urlAttr,
            new org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern("*google*")
        );
        Expression statusEq = new Equals(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, 200L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(like, statusEq));

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        StorageObject storageObject = createStorageObject(parquetData);
        int total;
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            int count = 0;
            while (iter.hasNext()) {
                Page page = iter.next();
                count += page.getPositionCount();
                page.releaseBlocks();
            }
            total = count;
        }
        assertThat("LIKE conjunct must be applied even when sibling conjunct is stats-trivial", total, equalTo(rows / 2));
    }

    /**
     * Companion to {@link #testPushedExpressionsLikeWithStatsTrivialEqDoesNotLeak}: when every
     * pushed conjunct does translate to a FilterPredicate, the trivially-passes shortcut should
     * still fire (i.e. the guard must not be over-conservative). We can't observe shortcut firing
     * directly here, but we can verify it produces the correct rows; combined with the existing
     * {@code testPushedExpressionsTriviallyPassingFilterParity} this confirms the shortcut path
     * stays exercised for all-translatable filters.
     */
    public void testPushedExpressionsAllTranslatableTriviallyPassesStillFires() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(INT64)
            .named("status")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("all_translatable_test");

        int rows = 200;
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                groups.add(factory.newGroup().append("status", 200L).append("label", "row_" + i));
            }
            return groups;
        });

        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.LONG);
        Expression statusEq = new Equals(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, 200L, DataType.LONG), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(statusEq));

        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        StorageObject storageObject = createStorageObject(parquetData);
        int total;
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            int count = 0;
            while (iter.hasNext()) {
                Page page = iter.next();
                count += page.getPositionCount();
                page.releaseBlocks();
            }
            total = count;
        }
        assertThat("trivially-passing all-translatable filter must return every row", total, equalTo(rows));
    }

    /**
     * NOT(Eq) via PushedExpressions: verifies the RowRanges path returns all rows that
     * the baseline returns — i.e. NOT does not drop rows from mixed pages.
     */
    public void testPushedExpressionsNotEqualityParity() throws IOException {
        byte[] parquetData = createSortedIntFile();

        FilterPredicate filterCompat = FilterApi.not(FilterApi.eq(FilterApi.intColumn("id"), 500));
        Expression esqlFilter = new Not(Source.EMPTY, new Equals(Source.EMPTY, intAttr("id"), intLit(500), null));

        List<Page> baselinePages = readWithFilter(parquetData, filterCompat, false);
        List<Page> pushedPages = readWithPushedExpressions(parquetData, esqlFilter);
        assertPagesEqual(baselinePages, pushedPages);
    }

    /**
     * NOT(OR(Eq, Eq)) via PushedExpressions: compound NOT that cannot be simplified to
     * NotEq by De Morgan. Verifies conservative RowRanges for compound negations.
     */
    public void testPushedExpressionsNotCompoundParity() throws IOException {
        byte[] parquetData = createSortedIntFile();

        FilterPredicate innerCompat = FilterApi.or(
            FilterApi.eq(FilterApi.intColumn("id"), 100),
            FilterApi.eq(FilterApi.intColumn("id"), 900)
        );
        FilterPredicate filterCompat = FilterApi.not(innerCompat);
        Expression innerEsql = new org.elasticsearch.xpack.esql.expression.predicate.logical.Or(
            Source.EMPTY,
            new Equals(Source.EMPTY, intAttr("id"), intLit(100), null),
            new Equals(Source.EMPTY, intAttr("id"), intLit(900), null)
        );
        Expression esqlFilter = new Not(Source.EMPTY, innerEsql);

        List<Page> baselinePages = readWithFilter(parquetData, filterCompat, false);
        List<Page> pushedPages = readWithPushedExpressions(parquetData, esqlFilter);
        assertPagesEqual(baselinePages, pushedPages);
    }

    /**
     * Multi-row-group file where all row groups have the same row count, exercising
     * the simplified row-group alignment logic (rowGroupOrdinal direct indexing).
     */
    public void testSameRowCountMultiRowGroupParity() throws IOException {
        MessageType schema = Types.buildMessage().required(INT32).named("id").named("same_rg_test");
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 2000; i++) {
                groups.add(factory.newGroup().append("id", i));
            }
            return groups;
        });

        FilterPredicate filterCompat = FilterApi.and(
            FilterApi.gtEq(FilterApi.intColumn("id"), 500),
            FilterApi.lt(FilterApi.intColumn("id"), 1500)
        );
        Expression esqlFilter = new org.elasticsearch.xpack.esql.expression.predicate.logical.And(
            Source.EMPTY,
            new GreaterThanOrEqual(Source.EMPTY, intAttr("id"), intLit(500), null),
            new LessThan(Source.EMPTY, intAttr("id"), intLit(1500), null)
        );

        List<Page> baselinePages = readWithFilter(parquetData, filterCompat, false);
        List<Page> pushedPages = readWithPushedExpressions(parquetData, esqlFilter);
        assertPagesEqual(baselinePages, pushedPages);
    }

    // --- Pre-warm dictionary-pages parity tests ---

    /**
     * End-to-end correctness test for the dictionary-page pre-warm optimization. Writes a
     * multi-row-group file with a dictionary-encoded BINARY column and applies an equality
     * filter on a value present only in some row groups. The optimized path pre-fetches
     * dictionary pages in a coalesced batch and feeds them to {@code RowGroupFilter} via the
     * pre-warmed cache; the baseline path reads dictionary pages on demand via parquet-mr's
     * own code. Both must produce bit-identical row contents.
     */
    public void testDictionaryFilterPreWarmParity() throws IOException {
        byte[] parquetData = createDictionaryEncodedStringFile();

        FilterPredicate filter = FilterApi.eq(FilterApi.binaryColumn("category"), org.apache.parquet.io.api.Binary.fromString("cat_3"));

        List<Page> baselinePages = readWithFilter(parquetData, filter, false);
        List<Page> optimizedPages = readWithFilter(parquetData, filter, true);
        assertPagesEqual(baselinePages, optimizedPages);

        int totalRows = optimizedPages.stream().mapToInt(Page::getPositionCount).sum();
        assertTrue("Equality filter on a present dictionary value must return some rows", totalRows > 0);
    }

    /**
     * Same setup as {@link #testDictionaryFilterPreWarmParity} but for an equality filter on a
     * value not in the dictionary. The dictionary filter should drop every row group on both
     * paths; both must return zero rows.
     */
    public void testDictionaryFilterPreWarmDropsRowGroupsParity() throws IOException {
        byte[] parquetData = createDictionaryEncodedStringFile();

        FilterPredicate filter = FilterApi.eq(
            FilterApi.binaryColumn("category"),
            org.apache.parquet.io.api.Binary.fromString("not_in_dictionary")
        );

        List<Page> baselinePages = readWithFilter(parquetData, filter, false);
        List<Page> optimizedPages = readWithFilter(parquetData, filter, true);
        assertPagesEqual(baselinePages, optimizedPages);

        int baselineRows = baselinePages.stream().mapToInt(Page::getPositionCount).sum();
        int optimizedRows = optimizedPages.stream().mapToInt(Page::getPositionCount).sum();
        assertEquals(0, baselineRows);
        assertEquals(0, optimizedRows);
    }

    /**
     * Builds a multi-row-group parquet file with a dictionary-encoded BINARY column. The
     * dictionary alphabet is small (16 values) so every row group keeps dictionary encoding,
     * which exercises {@code RowGroupFilter}'s DICTIONARY-level pruning code path that the
     * pre-warm optimization targets.
     */
    private byte[] createDictionaryEncodedStringFile() throws IOException {
        MessageType schema = Types.buildMessage()
            .required(INT32)
            .named("id")
            .required(BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("category")
            .named("dict_test");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        // 16-value dictionary: small enough that the writer keeps dictionary encoding for every
        // row group; row count is sized to comfortably exceed the writer's small row group budget
        // so we get multiple dictionary pages — the multi-row-group case is where pre-warm pays.
        int rows = 65_536;
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withRowGroupSize(64 * 1024L)
                .withPageSize(4 * 1024)
                .withDictionaryPageSize(64 * 1024)
                .withDictionaryEncoding(true)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (int i = 0; i < rows; i++) {
                Group g = groupFactory.newGroup().append("id", i).append("category", "cat_" + (i % 16));
                writer.write(g);
            }
        }
        return outputStream.toByteArray();
    }

    // --- Helpers ---

    private static ReferenceAttribute intAttr(String name) {
        return new ReferenceAttribute(Source.EMPTY, name, DataType.INTEGER);
    }

    private static Literal intLit(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private byte[] createSortedIntFile() throws IOException {
        MessageType schema = Types.buildMessage().required(INT32).named("id").named("sorted_test");
        return createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < TOTAL_ROWS; i++) {
                groups.add(factory.newGroup().append("id", i));
            }
            return groups;
        });
    }

    @FunctionalInterface
    interface GroupCreator {
        List<Group> create(SimpleGroupFactory factory);
    }

    private byte[] createParquetFile(MessageType schema, GroupCreator groupCreator) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        List<Group> groups = groupCreator.create(groupFactory);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withRowGroupSize(10 * 1024 * 1024)
                .withPageSize(64)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (Group group : groups) {
                writer.write(group);
            }
        }
        return outputStream.toByteArray();
    }

    private List<Page> readWithFilter(byte[] parquetData, FilterPredicate filter, boolean optimized) throws IOException {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, optimized);
        if (filter != null) {
            reader = reader.withPushedFilter(FilterCompat.get(filter));
        }
        StorageObject storageObject = createStorageObject(parquetData);
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            List<Page> pages = new ArrayList<>();
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
            return pages;
        }
    }

    /**
     * Reads using the optimized path with a {@link ParquetPushedExpressions} filter.
     * This exercises the full RowRanges code path: resolveFilterPredicate → ColumnIndexRowRangesComputer
     * → RowRanges[] → PageColumnReader page skipping + ColumnChunkPrefetcher filtered prefetch.
     */
    private List<Page> readWithPushedExpressions(byte[] parquetData, Expression esqlFilter) throws IOException {
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(esqlFilter));
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        StorageObject storageObject = createStorageObject(parquetData);
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(null, 1024))) {
            List<Page> pages = new ArrayList<>();
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
            return pages;
        }
    }

    private void assertPagesEqual(List<Page> expected, List<Page> actual) {
        int expectedRows = expected.stream().mapToInt(Page::getPositionCount).sum();
        int actualRows = actual.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("total row count mismatch", actualRows, equalTo(expectedRows));

        int ep = 0, ePos = 0;
        int ap = 0, aPos = 0;
        for (int row = 0; row < expectedRows; row++) {
            Page ePage = expected.get(ep);
            Page aPage = actual.get(ap);
            for (int b = 0; b < ePage.getBlockCount(); b++) {
                assertBlockValueEqual(ePage.getBlock(b), ePos, aPage.getBlock(b), aPos, row, b);
            }
            ePos++;
            if (ePos >= ePage.getPositionCount()) {
                ep++;
                ePos = 0;
            }
            aPos++;
            if (aPos >= aPage.getPositionCount()) {
                ap++;
                aPos = 0;
            }
        }
    }

    private void assertBlockValueEqual(Block expected, int ePos, Block actual, int aPos, int row, int block) {
        String ctx = "row " + row + " block " + block;
        assertThat(ctx + " null", actual.isNull(aPos), equalTo(expected.isNull(ePos)));
        if (expected.isNull(ePos)) {
            return;
        }
        if (expected instanceof IntBlock eb && actual instanceof IntBlock ab) {
            assertThat(ctx, ab.getInt(ab.getFirstValueIndex(aPos)), equalTo(eb.getInt(eb.getFirstValueIndex(ePos))));
        } else if (expected instanceof LongBlock eb && actual instanceof LongBlock ab) {
            assertThat(ctx, ab.getLong(ab.getFirstValueIndex(aPos)), equalTo(eb.getLong(eb.getFirstValueIndex(ePos))));
        } else if (expected instanceof DoubleBlock eb && actual instanceof DoubleBlock ab) {
            assertThat(ctx, ab.getDouble(ab.getFirstValueIndex(aPos)), equalTo(eb.getDouble(eb.getFirstValueIndex(ePos))));
        } else if (expected instanceof BooleanBlock eb && actual instanceof BooleanBlock ab) {
            assertThat(ctx, ab.getBoolean(ab.getFirstValueIndex(aPos)), equalTo(eb.getBoolean(eb.getFirstValueIndex(ePos))));
        } else if (expected instanceof BytesRefBlock eb && actual instanceof BytesRefBlock ab) {
            assertThat(
                ctx,
                ab.getBytesRef(ab.getFirstValueIndex(aPos), new BytesRef()),
                equalTo(eb.getBytesRef(eb.getFirstValueIndex(ePos), new BytesRef()))
            );
        }
    }

    private StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int pos = (int) position;
                int len = (int) Math.min(length, data.length - position);
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
                return StoragePath.of("memory://filtered_test.parquet");
            }
        };
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return createPositionOutputStream(outputStream);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return createPositionOutputStream(outputStream);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }

            @Override
            public String getPath() {
                return "memory://filtered_test.parquet";
            }
        };
    }

    private static PositionOutputStream createPositionOutputStream(ByteArrayOutputStream outputStream) {
        return new PositionOutputStream() {
            @Override
            public long getPos() {
                return outputStream.size();
            }

            @Override
            public void write(int b) {
                outputStream.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                outputStream.write(b, off, len);
            }
        };
    }
}
