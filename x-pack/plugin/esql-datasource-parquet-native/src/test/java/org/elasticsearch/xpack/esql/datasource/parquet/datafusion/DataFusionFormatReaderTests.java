/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.datafusion;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Tests for {@link DataFusionFormatReader} that verify end-to-end reading of Parquet files
 * via the native DataFusion library.
 * <p>
 * Uses a pre-generated Parquet test file created by {@link #createTestParquetFile(Path)}.
 * The native library must be built before running these tests.
 */
public class DataFusionFormatReaderTests extends ESTestCase {

    private BlockFactory blockFactory;
    private Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        tempDir = createTempDir();

        // Load native library directly from build output
        String nativeLibPath = System.getProperty("tests.native.lib.path");
        if (nativeLibPath != null) {
            System.load(nativeLibPath);
        }
    }

    public void testFormatName() {
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);
        assertEquals("parquet", reader.formatName());
    }

    public void testFileExtensions() {
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);
        List<String> extensions = reader.fileExtensions();
        assertTrue(extensions.contains(".parquet"));
    }

    public void testReadSchema() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        List<Attribute> attributes = metadata.schema();

        assertEquals(3, attributes.size());
        assertEquals("id", attributes.get(0).name());
        assertEquals("name", attributes.get(1).name());
        assertEquals("age", attributes.get(2).name());
    }

    public void testReadData() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(null, 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            int totalRows = 0;
            for (Page page : pages) {
                totalRows += page.getPositionCount();
            }
            assertEquals(3, totalRows);

            Page firstPage = pages.get(0);
            assertEquals(3, firstPage.getBlockCount());

            LongBlock idBlock = (LongBlock) firstPage.getBlock(0);
            BytesRefBlock nameBlock = (BytesRefBlock) firstPage.getBlock(1);
            IntBlock ageBlock = (IntBlock) firstPage.getBlock(2);

            assertEquals(1L, idBlock.getLong(0));
            assertEquals(2L, idBlock.getLong(1));
            assertEquals(3L, idBlock.getLong(2));

            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("Bob"), nameBlock.getBytesRef(1, new BytesRef()));
            assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(2, new BytesRef()));

            assertEquals(30, ageBlock.getInt(0));
            assertEquals(25, ageBlock.getInt(1));
            assertEquals(35, ageBlock.getInt(2));
        } finally {
            releasePages(pages);
        }
    }

    public void testReadWithProjection() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(List.of("name", "age"), 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            Page firstPage = pages.get(0);
            assertEquals(2, firstPage.getBlockCount());

            BytesRefBlock nameBlock = (BytesRefBlock) firstPage.getBlock(0);
            IntBlock ageBlock = (IntBlock) firstPage.getBlock(1);

            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("Bob"), nameBlock.getBytesRef(1, new BytesRef()));
            assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(2, new BytesRef()));

            assertEquals(30, ageBlock.getInt(0));
            assertEquals(25, ageBlock.getInt(1));
            assertEquals(35, ageBlock.getInt(2));
        } finally {
            releasePages(pages);
        }
    }

    public void testProjectionSingleColumn() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(List.of("age"), 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            Page firstPage = pages.get(0);
            assertEquals(1, firstPage.getBlockCount());

            IntBlock ageBlock = (IntBlock) firstPage.getBlock(0);
            assertEquals(3, firstPage.getPositionCount());
            assertEquals(30, ageBlock.getInt(0));
            assertEquals(25, ageBlock.getInt(1));
            assertEquals(35, ageBlock.getInt(2));
        } finally {
            releasePages(pages);
        }
    }

    public void testProjectionReversedColumnOrder() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(List.of("age", "id"), 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            Page firstPage = pages.get(0);
            assertEquals(2, firstPage.getBlockCount());

            IntBlock ageBlock = (IntBlock) firstPage.getBlock(0);
            LongBlock idBlock = (LongBlock) firstPage.getBlock(1);

            assertEquals(30, ageBlock.getInt(0));
            assertEquals(1L, idBlock.getLong(0));
            assertEquals(25, ageBlock.getInt(1));
            assertEquals(2L, idBlock.getLong(1));
        } finally {
            releasePages(pages);
        }
    }

    public void testProjectionWithFilter() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);

        Expression filter = new GreaterThan(Source.EMPTY, attr("age", DataType.INTEGER), intLit(28), null);
        DataFusionFilterPushdownSupport support = new DataFusionFilterPushdownSupport();
        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        FormatReader reader = new DataFusionFormatReader(blockFactory).withPushedFilter(result.pushedFilter());
        FormatReadContext context = FormatReadContext.of(List.of("name"), 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            assertEquals(2, totalRows(pages));
            Page firstPage = pages.get(0);
            assertEquals(1, firstPage.getBlockCount());

            BytesRefBlock nameBlock = (BytesRefBlock) firstPage.getBlock(0);
            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
            assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(1, new BytesRef()));
        } finally {
            releasePages(pages);
        }
    }

    public void testProjectionNullReadsAllColumns() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(null, 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            Page firstPage = pages.get(0);
            assertEquals(3, firstPage.getBlockCount());
        } finally {
            releasePages(pages);
        }
    }

    public void testExecutionPlanShowsProjectedColumns() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(List.of("name"), 1024);
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            assertThat(iter, instanceOf(DataFusionFormatReader.DataFusionBatchIterator.class));
            String plan = ((DataFusionFormatReader.DataFusionBatchIterator) iter).executionPlan();
            assertNotNull(plan);
            assertThat(plan, containsString("name"));
            assertThat(plan, not(containsString("projection=[id, name, age]")));
        }
    }

    public void testExecutionPlanShowsAllColumnsWhenNoProjection() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(null, 1024);
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            assertThat(iter, instanceOf(DataFusionFormatReader.DataFusionBatchIterator.class));
            String plan = ((DataFusionFormatReader.DataFusionBatchIterator) iter).executionPlan();
            assertNotNull(plan);
            assertThat(plan, containsString("id"));
            assertThat(plan, containsString("name"));
            assertThat(plan, containsString("age"));
        }
    }

    public void testExecutionPlanShowsFilter() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);

        Expression filter = new GreaterThan(Source.EMPTY, attr("age", DataType.INTEGER), intLit(28), null);
        DataFusionFilterPushdownSupport support = new DataFusionFilterPushdownSupport();
        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));

        FormatReader reader = new DataFusionFormatReader(blockFactory).withPushedFilter(result.pushedFilter());
        FormatReadContext context = FormatReadContext.of(List.of("name", "age"), 1024);
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            assertThat(iter, instanceOf(DataFusionFormatReader.DataFusionBatchIterator.class));
            String plan = ((DataFusionFormatReader.DataFusionBatchIterator) iter).executionPlan();
            assertNotNull(plan);
            assertThat(plan, containsString("Filter"));
        }
    }

    public void testReadWithLimit() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        FormatReadContext context = FormatReadContext.of(null, 1024).withRowLimit(2);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }

        try {
            int totalRows = 0;
            for (Page page : pages) {
                totalRows += page.getPositionCount();
            }
            assertEquals(2, totalRows);
        } finally {
            releasePages(pages);
        }
    }

    // ---- Filter pushdown tests ----

    public void testFilterEqualsInt() throws Exception {
        // age == 30 => only Alice (id=1)
        List<Page> pages = readWithFilter(new Equals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(30), null));
        try {
            assertEquals(1, totalRows(pages));
            LongBlock idBlock = (LongBlock) pages.get(0).getBlock(0);
            assertEquals(1L, idBlock.getLong(0));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterEqualsString() throws Exception {
        // name == "Bob" => only Bob (id=2)
        List<Page> pages = readWithFilter(new Equals(Source.EMPTY, attr("name", DataType.KEYWORD), keywordLit("Bob"), null));
        try {
            assertEquals(1, totalRows(pages));
            LongBlock idBlock = (LongBlock) pages.get(0).getBlock(0);
            assertEquals(2L, idBlock.getLong(0));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterNotEquals() throws Exception {
        // age != 30 => Bob (25) and Charlie (35)
        List<Page> pages = readWithFilter(new NotEquals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(30), null));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterGreaterThan() throws Exception {
        // age > 28 => Alice (30) and Charlie (35)
        List<Page> pages = readWithFilter(new GreaterThan(Source.EMPTY, attr("age", DataType.INTEGER), intLit(28), null));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterLessThan() throws Exception {
        // age < 30 => only Bob (25)
        List<Page> pages = readWithFilter(new LessThan(Source.EMPTY, attr("age", DataType.INTEGER), intLit(30), null));
        try {
            assertEquals(1, totalRows(pages));
            IntBlock ageBlock = (IntBlock) pages.get(0).getBlock(2);
            assertEquals(25, ageBlock.getInt(0));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterGreaterThanOrEqual() throws Exception {
        // age >= 30 => Alice (30) and Charlie (35)
        List<Page> pages = readWithFilter(new GreaterThanOrEqual(Source.EMPTY, attr("age", DataType.INTEGER), intLit(30), null));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterAnd() throws Exception {
        // age > 20 AND age < 31 => Alice (30) and Bob (25)
        Expression left = new GreaterThan(Source.EMPTY, attr("age", DataType.INTEGER), intLit(20), null);
        Expression right = new LessThan(Source.EMPTY, attr("age", DataType.INTEGER), intLit(31), null);
        List<Page> pages = readWithFilter(new And(Source.EMPTY, left, right));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterOr() throws Exception {
        // age == 25 OR age == 35 => Bob and Charlie
        Expression left = new Equals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(25), null);
        Expression right = new Equals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(35), null);
        List<Page> pages = readWithFilter(new Or(Source.EMPTY, left, right));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterNot() throws Exception {
        // NOT (age == 30) => Bob (25) and Charlie (35)
        Expression inner = new Equals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(30), null);
        List<Page> pages = readWithFilter(new Not(Source.EMPTY, inner));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterIn() throws Exception {
        // age IN (25, 35) => Bob and Charlie
        Expression value = attr("age", DataType.INTEGER);
        List<Expression> list = List.of(intLit(25), intLit(35));
        List<Page> pages = readWithFilter(new In(Source.EMPTY, value, list));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterIsNotNull() throws Exception {
        // name IS NOT NULL => all 3 rows (no nulls in test data)
        List<Page> pages = readWithFilter(new IsNotNull(Source.EMPTY, attr("name", DataType.KEYWORD)));
        try {
            assertEquals(3, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterStartsWith() throws Exception {
        // name STARTS WITH "Al" => only Alice
        List<Page> pages = readWithFilter(new StartsWith(Source.EMPTY, attr("name", DataType.KEYWORD), keywordLit("Al")));
        try {
            assertEquals(1, totalRows(pages));
            BytesRefBlock nameBlock = (BytesRefBlock) pages.get(0).getBlock(1);
            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterNoMatchReturnsEmpty() throws Exception {
        // age == 999 => no rows
        List<Page> pages = readWithFilter(new Equals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(999), null));
        try {
            assertEquals(0, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterPushdownSupportCanPush() {
        DataFusionFilterPushdownSupport support = new DataFusionFilterPushdownSupport();
        Expression filter = new Equals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(30), null);
        assertEquals(FilterPushdownSupport.Pushability.YES, support.canPush(filter));
    }

    public void testFilterPushdownSupportPushFilters() {
        DataFusionFilterPushdownSupport support = new DataFusionFilterPushdownSupport();
        Expression pushable = new Equals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(30), null);
        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(pushable));

        assertTrue(result.hasPushedFilter());
        assertEquals(1, result.pushedExpressions().size());
        assertEquals(0, result.remainder().size());

        // Clean up the native handle
        DataFusionPushedFilter pushedFilter = (DataFusionPushedFilter) result.pushedFilter();
        DataFusionBridge.freeExpr(pushedFilter.exprHandle());
    }

    // ---- LIKE filter pushdown tests ----

    public void testFilterLikeWildcardSuffix() throws Exception {
        // name LIKE "Al*" => Alice
        List<Page> pages = readWithFilter(new WildcardLike(Source.EMPTY, attr("name", DataType.KEYWORD), new WildcardPattern("Al*")));
        try {
            assertEquals(1, totalRows(pages));
            BytesRefBlock nameBlock = (BytesRefBlock) pages.get(0).getBlock(1);
            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterLikeWildcardPrefix() throws Exception {
        // name LIKE "*lie" => Charlie
        List<Page> pages = readWithFilter(new WildcardLike(Source.EMPTY, attr("name", DataType.KEYWORD), new WildcardPattern("*lie")));
        try {
            assertEquals(1, totalRows(pages));
            BytesRefBlock nameBlock = (BytesRefBlock) pages.get(0).getBlock(1);
            assertEquals(new BytesRef("Charlie"), nameBlock.getBytesRef(0, new BytesRef()));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterLikeContains() throws Exception {
        // name LIKE "*li*" => Alice and Charlie
        List<Page> pages = readWithFilter(new WildcardLike(Source.EMPTY, attr("name", DataType.KEYWORD), new WildcardPattern("*li*")));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterLikeSingleCharWildcard() throws Exception {
        // name LIKE "Bo?" => Bob
        List<Page> pages = readWithFilter(new WildcardLike(Source.EMPTY, attr("name", DataType.KEYWORD), new WildcardPattern("Bo?")));
        try {
            assertEquals(1, totalRows(pages));
            BytesRefBlock nameBlock = (BytesRefBlock) pages.get(0).getBlock(1);
            assertEquals(new BytesRef("Bob"), nameBlock.getBytesRef(0, new BytesRef()));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterLikeNoMatch() throws Exception {
        // name LIKE "*xyz*" => no match
        List<Page> pages = readWithFilter(new WildcardLike(Source.EMPTY, attr("name", DataType.KEYWORD), new WildcardPattern("*xyz*")));
        try {
            assertEquals(0, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterLikeAndEquals() throws Exception {
        // name LIKE "*li*" AND age == 30 => Alice only
        Expression like = new WildcardLike(Source.EMPTY, attr("name", DataType.KEYWORD), new WildcardPattern("*li*"));
        Expression eq = new Equals(Source.EMPTY, attr("age", DataType.INTEGER), intLit(30), null);
        List<Page> pages = readWithFilter(new And(Source.EMPTY, like, eq));
        try {
            assertEquals(1, totalRows(pages));
            BytesRefBlock nameBlock = (BytesRefBlock) pages.get(0).getBlock(1);
            assertEquals(new BytesRef("Alice"), nameBlock.getBytesRef(0, new BytesRef()));
        } finally {
            releasePages(pages);
        }
    }

    public void testFilterNotLike() throws Exception {
        // NOT (name LIKE "Al*") => Bob and Charlie
        Expression like = new WildcardLike(Source.EMPTY, attr("name", DataType.KEYWORD), new WildcardPattern("Al*"));
        List<Page> pages = readWithFilter(new Not(Source.EMPTY, like));
        try {
            assertEquals(2, totalRows(pages));
        } finally {
            releasePages(pages);
        }
    }

    public void testCanConvertWildcardLike() {
        DataFusionFilterPushdownSupport support = new DataFusionFilterPushdownSupport();
        Expression like = new WildcardLike(Source.EMPTY, attr("name", DataType.KEYWORD), new WildcardPattern("*test*"));
        assertEquals(FilterPushdownSupport.Pushability.YES, support.canPush(like));
    }

    public void testEsqlWildcardToSqlLikeConversion() {
        assertEquals("%", DataFusionFilterPushdownSupport.esqlWildcardToSqlLike("*"));
        assertEquals("_", DataFusionFilterPushdownSupport.esqlWildcardToSqlLike("?"));
        assertEquals("%google%", DataFusionFilterPushdownSupport.esqlWildcardToSqlLike("*google*"));
        assertEquals("%.google.%", DataFusionFilterPushdownSupport.esqlWildcardToSqlLike("*.google.*"));
        assertEquals("Al%", DataFusionFilterPushdownSupport.esqlWildcardToSqlLike("Al*"));
        assertEquals("Bo_", DataFusionFilterPushdownSupport.esqlWildcardToSqlLike("Bo?"));
        assertEquals("test\\%value", DataFusionFilterPushdownSupport.esqlWildcardToSqlLike("test%value"));
        assertEquals("test\\_value", DataFusionFilterPushdownSupport.esqlWildcardToSqlLike("test_value"));
    }

    // ---- Aggregate pushdown tests ----

    public void testMetadataIncludesColumnStatistics() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        assertTrue(metadata.statistics().isPresent());
        SourceStatistics statistics = metadata.statistics().get();

        assertTrue(statistics.rowCount().isPresent());
        assertEquals(3L, statistics.rowCount().getAsLong());

        assertTrue(statistics.columnStatistics().isPresent());
        var colStats = statistics.columnStatistics().get();
        assertTrue(colStats.containsKey("age"));

        SourceStatistics.ColumnStatistics ageStats = colStats.get("age");
        assertTrue(ageStats.nullCount().isPresent());
        assertEquals(0L, ageStats.nullCount().getAsLong());
        assertTrue(ageStats.minValue().isPresent());
        assertEquals(25, ageStats.minValue().get());
        assertTrue(ageStats.maxValue().isPresent());
        assertEquals(35, ageStats.maxValue().get());
    }

    public void testMetadataStringColumnStats() throws Exception {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);

        SourceMetadata metadata = reader.metadata(storageObject);
        var colStats = metadata.statistics().get().columnStatistics().get();
        assertTrue(colStats.containsKey("name"));

        SourceStatistics.ColumnStatistics nameStats = colStats.get("name");
        assertTrue(nameStats.minValue().isPresent());
        assertEquals("Alice", nameStats.minValue().get());
        assertTrue(nameStats.maxValue().isPresent());
        assertEquals("Charlie", nameStats.maxValue().get());
    }

    public void testAggregatePushdownSupportCountMinMax() {
        DataFusionAggregatePushdownSupport support = new DataFusionAggregatePushdownSupport();

        Attribute ageAttr = attr("age", DataType.INTEGER);
        List<Expression> aggregates = List.of(
            new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")),
            new Min(Source.EMPTY, ageAttr),
            new Max(Source.EMPTY, ageAttr)
        );

        assertEquals(AggregatePushdownSupport.Pushability.YES, support.canPushAggregates(aggregates, List.of()));
    }

    public void testAggregatePushdownRejectsGroupBy() {
        DataFusionAggregatePushdownSupport support = new DataFusionAggregatePushdownSupport();

        Attribute ageAttr = attr("age", DataType.INTEGER);
        List<Expression> aggregates = List.of(new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*")));
        List<Expression> groupings = List.of(ageAttr);

        assertEquals(AggregatePushdownSupport.Pushability.NO, support.canPushAggregates(aggregates, groupings));
    }

    public void testAggregatePushdownSupportIsWired() {
        DataFusionFormatReader reader = new DataFusionFormatReader(blockFactory);
        assertNotNull(reader.aggregatePushdownSupport());
        assertNotSame(AggregatePushdownSupport.UNSUPPORTED, reader.aggregatePushdownSupport());
    }

    // ---- Filter test helpers ----

    private List<Page> readWithFilter(Expression filter) throws IOException {
        Path parquetFile = createTestParquetFile(tempDir);
        StorageObject storageObject = createFileStorageObject(parquetFile);

        DataFusionFilterPushdownSupport support = new DataFusionFilterPushdownSupport();
        FilterPushdownSupport.PushdownResult result = support.pushFilters(List.of(filter));
        assertTrue("Filter should be pushable", result.hasPushedFilter());

        FormatReader reader = new DataFusionFormatReader(blockFactory).withPushedFilter(result.pushedFilter());
        FormatReadContext context = FormatReadContext.of(null, 1024);
        List<Page> pages = new ArrayList<>();
        try (CloseableIterator<Page> iter = reader.read(storageObject, context)) {
            while (iter.hasNext()) {
                pages.add(iter.next());
            }
        }
        return pages;
    }

    private static int totalRows(List<Page> pages) {
        int total = 0;
        for (Page page : pages) {
            total += page.getPositionCount();
        }
        return total;
    }

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static Literal intLit(int value) {
        return new Literal(Source.EMPTY, value, DataType.INTEGER);
    }

    private static Literal keywordLit(String value) {
        return new Literal(Source.EMPTY, new BytesRef(value), DataType.KEYWORD);
    }

    /**
     * Creates a simple 3-row Parquet file with columns: id (INT64), name (UTF8), age (INT32).
     * Uses parquet-mr (test dependency only) to write the file.
     */
    private static Path createTestParquetFile(Path dir) throws IOException {
        Path file = dir.resolve("test.parquet");
        // Write a minimal Parquet file using the Apache Parquet Java writer.
        // This dependency is only used in tests; the runtime reader is purely native.
        org.apache.parquet.schema.MessageType schema = org.apache.parquet.schema.Types.buildMessage()
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY)
            .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
            .named("name")
            .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .named("test_schema");

        org.apache.parquet.io.OutputFile outputFile = new org.apache.parquet.io.OutputFile() {
            @Override
            public org.apache.parquet.io.PositionOutputStream create(long blockSizeHint) throws IOException {
                java.io.OutputStream os = Files.newOutputStream(file);
                return new org.apache.parquet.io.PositionOutputStream() {
                    long pos = 0;

                    @Override
                    public long getPos() {
                        return pos;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        os.write(b);
                        pos++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        os.write(b, off, len);
                        pos += len;
                    }

                    @Override
                    public void flush() throws IOException {
                        os.flush();
                    }

                    @Override
                    public void close() throws IOException {
                        os.close();
                    }
                };
            }

            @Override
            public org.apache.parquet.io.PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
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

        org.apache.parquet.example.data.simple.SimpleGroupFactory factory = new org.apache.parquet.example.data.simple.SimpleGroupFactory(
            schema
        );

        try (
            org.apache.parquet.hadoop.ParquetWriter<org.apache.parquet.example.data.Group> writer =
                org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(outputFile)
                    .withType(schema)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                    .build()
        ) {
            org.apache.parquet.example.data.Group g1 = factory.newGroup();
            g1.add("id", 1L);
            g1.add("name", "Alice");
            g1.add("age", 30);
            writer.write(g1);

            org.apache.parquet.example.data.Group g2 = factory.newGroup();
            g2.add("id", 2L);
            g2.add("name", "Bob");
            g2.add("age", 25);
            writer.write(g2);

            org.apache.parquet.example.data.Group g3 = factory.newGroup();
            g3.add("id", 3L);
            g3.add("name", "Charlie");
            g3.add("age", 35);
            writer.write(g3);
        }

        return file;
    }

    private static void releasePages(List<Page> pages) {
        for (Page page : pages) {
            page.releaseBlocks();
        }
    }

    private static StorageObject createFileStorageObject(Path path) {
        return new StorageObject() {
            @Override
            public InputStream newStream() throws IOException {
                return Files.newInputStream(path);
            }

            @Override
            public InputStream newStream(long position, long length) throws IOException {
                InputStream is = Files.newInputStream(path);
                is.skipNBytes(position);
                return is;
            }

            @Override
            public long length() throws IOException {
                return Files.size(path);
            }

            @Override
            public Instant lastModified() {
                return null;
            }

            @Override
            public boolean exists() throws IOException {
                return Files.exists(path);
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("file://" + path.toAbsolutePath());
            }
        };
    }
}
