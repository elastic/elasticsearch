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
