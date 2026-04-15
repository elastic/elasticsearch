/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests for the optimized Parquet reader feature flag and correctness parity with the baseline.
 */
public class OptimizedParquetReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testDefaultReaderIsBaseline() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of());
        assertSame(reader, configured);
    }

    public void testWithConfigOptimizedReaderTrue() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", true));
        assertNotSame(reader, configured);
    }

    public void testWithConfigOptimizedReaderFalse() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", false));
        assertSame(reader, configured);
    }

    public void testWithConfigOptimizedReaderStringTrue() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", "true"));
        assertNotSame(reader, configured);
    }

    public void testWithConfigNullConfig() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(null);
        assertSame(reader, configured);
    }

    public void testWithConfigPreservesPushedFilter() throws Exception {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader withFilter = (ParquetFormatReader) reader.withPushedFilter(null);
        ParquetFormatReader withConfig = (ParquetFormatReader) withFilter.withConfig(Map.of("optimized_reader", true));
        assertNotSame(withFilter, withConfig);
    }

    public void testNodeLevelSettingDefault() {
        ParquetDataSourcePlugin plugin = new ParquetDataSourcePlugin();
        Map<String, FormatReaderFactory> factories = plugin.formatReaders(Settings.EMPTY);
        assertNotNull(factories.get("parquet"));
    }

    public void testNodeLevelSettingEnabled() {
        ParquetDataSourcePlugin plugin = new ParquetDataSourcePlugin();
        Settings settings = Settings.builder().put("esql.parquet.optimized_reader", true).build();
        Map<String, FormatReaderFactory> factories = plugin.formatReaders(settings);
        assertNotNull(factories.get("parquet"));
    }

    public void testWithConfigOverridesNodeDefault() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", false));
        assertNotSame(reader, configured);
    }

    /**
     * Correctness parity: both baseline and optimized paths produce identical output for the same file.
     */
    public void testCorrectnessParitySimpleTypes() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "user_" + i);
                g.add("age", 20 + (i % 50));
                g.add("active", i % 2 == 0);
                g.add("score", i * 1.5);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Correctness parity with nullable columns.
     */
    public void testCorrectnessParityNullableColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                if (i % 3 != 0) {
                    g.add("name", "user_" + i);
                }
                if (i % 5 != 0) {
                    g.add("age", 20 + i);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Correctness parity with column projection.
     */
    public void testCorrectnessParityWithProjection() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "user_" + i);
                g.add("age", 20 + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        List<String> projection = List.of("id", "name");

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject, projection);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject, projection);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Correctness parity with row limit.
     */
    public void testCorrectnessParityWithRowLimit() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "user_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        int rowLimit = 10;

        List<Page> baselinePages;
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, false).read(
                storageObject,
                FormatReadContext.builder().batchSize(1024).rowLimit(rowLimit).build()
            )
        ) {
            baselinePages = collectPages(iter);
        }

        List<Page> optimizedPages;
        try (
            CloseableIterator<Page> iter = new ParquetFormatReader(blockFactory, true).read(
                storageObject,
                FormatReadContext.builder().batchSize(1024).rowLimit(rowLimit).build()
            )
        ) {
            optimizedPages = collectPages(iter);
        }

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Metadata preloading: optimized reader preloads column/offset indexes without breaking correctness.
     */
    public void testMetadataPreloadingDoesNotBreakCorrectness() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 3 == 0 ? "active" : "inactive");
                g.add("value", i * 0.5);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Dictionary pruning: when a pushed filter matches no dictionary values, the row group is skipped.
     * Verifies that a filter for a non-existent value returns zero rows.
     */
    public void testDictionaryPruningSkipsRowGroupNoMatch() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 2 == 0 ? "active" : "inactive");
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.KEYWORD);
        Equals equalsExpr = new Equals(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, new BytesRef("nonexistent"), DataType.KEYWORD));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(optimizedReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("dictionary pruning should skip all row groups for non-existent value", totalRows, equalTo(0));
    }

    /**
     * Dictionary pruning: when a pushed filter matches some dictionary values, rows are returned.
     */
    public void testDictionaryPruningKeepsRowGroupWithMatch() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 2 == 0 ? "active" : "inactive");
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.KEYWORD);
        Equals equalsExpr = new Equals(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, new BytesRef("active"), DataType.KEYWORD));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(optimizedReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("dictionary pruning should keep row groups when values match", totalRows, greaterThan(0));
    }

    /**
     * Dictionary pruning with integer column: no match means row group skipped.
     */
    public void testDictionaryPruningIntegerColumnNoMatch() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("category")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("category", i % 5);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute catAttr = new ReferenceAttribute(Source.EMPTY, "category", DataType.INTEGER);
        Equals equalsExpr = new Equals(Source.EMPTY, catAttr, new Literal(Source.EMPTY, 999, DataType.INTEGER));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(optimizedReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("dictionary pruning should skip row groups for non-existent integer value", totalRows, equalTo(0));
    }

    /**
     * No pushed filter: optimized reader still works correctly (no dictionary pruning attempted).
     */
    public void testNoPushedFilterOptimizedReaderWorks() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "user_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Non-dictionary column: predicate on a non-dictionary-encoded column should not cause issues.
     */
    public void testNonDictionaryColumnFallsThrough() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("value", i * 1.1);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute valAttr = new ReferenceAttribute(Source.EMPTY, "value", DataType.DOUBLE);
        Equals equalsExpr = new Equals(Source.EMPTY, valAttr, new Literal(Source.EMPTY, 999.0, DataType.DOUBLE));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> optimizedPages = readAllPages(optimizedReader, storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Coalesced metadata preloading: verifies that the coalesced path (via StorageObject + CoalescedRangeReader)
     * produces identical results to the sequential path. The in-memory StorageObject supports readBytesAsync
     * via the default sync wrapper, exercising the full coalesced pipeline.
     */
    public void testCoalescedMetadataPreloadingCorrectness() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("category")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 3 == 0 ? "active" : (i % 3 == 1 ? "inactive" : "pending"));
                g.add("category", i % 10);
                g.add("value", i * 0.1);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Coalesced preloading with dictionary pruning: end-to-end test that the coalesced metadata
     * fetch + dictionary evaluation work together correctly.
     */
    public void testCoalescedPreloadWithDictionaryPruning() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 4 == 0 ? "alpha" : (i % 4 == 1 ? "beta" : (i % 4 == 2 ? "gamma" : "delta")));
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.KEYWORD);
        Equals equalsExpr = new Equals(
            Source.EMPTY,
            statusAttr,
            new Literal(Source.EMPTY, new BytesRef("nonexistent_value"), DataType.KEYWORD)
        );
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(optimizedReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("coalesced preload + dictionary pruning should skip all row groups", totalRows, equalTo(0));
    }

    // --- Helpers ---

    private List<Page> readAllPages(ParquetFormatReader reader, StorageObject storageObject) throws IOException {
        return readAllPages(reader, storageObject, null);
    }

    private List<Page> readAllPages(ParquetFormatReader reader, StorageObject storageObject, List<String> projection) throws IOException {
        try (CloseableIterator<Page> iter = reader.read(storageObject, FormatReadContext.of(projection, 1024))) {
            return collectPages(iter);
        }
    }

    private List<Page> collectPages(CloseableIterator<Page> iter) {
        List<Page> pages = new ArrayList<>();
        while (iter.hasNext()) {
            pages.add(iter.next());
        }
        return pages;
    }

    private void assertPagesEqual(List<Page> expected, List<Page> actual) {
        assertThat("page count mismatch", actual.size(), equalTo(expected.size()));
        for (int p = 0; p < expected.size(); p++) {
            Page ep = expected.get(p);
            Page ap = actual.get(p);
            assertThat("block count mismatch in page " + p, ap.getBlockCount(), equalTo(ep.getBlockCount()));
            assertThat("position count mismatch in page " + p, ap.getPositionCount(), equalTo(ep.getPositionCount()));
            for (int b = 0; b < ep.getBlockCount(); b++) {
                assertBlocksEqual(ep.getBlock(b), ap.getBlock(b), p, b);
            }
        }
    }

    private void assertBlocksEqual(
        org.elasticsearch.compute.data.Block expected,
        org.elasticsearch.compute.data.Block actual,
        int page,
        int block
    ) {
        String ctx = "page " + page + " block " + block;
        assertThat(ctx + " position count", actual.getPositionCount(), equalTo(expected.getPositionCount()));
        for (int pos = 0; pos < expected.getPositionCount(); pos++) {
            boolean expNull = expected.isNull(pos);
            boolean actNull = actual.isNull(pos);
            assertThat(ctx + " null at " + pos, actNull, equalTo(expNull));
            if (expNull == false) {
                if (expected instanceof LongBlock lb && actual instanceof LongBlock lab) {
                    assertThat(
                        ctx + " long at " + pos,
                        lab.getLong(lab.getFirstValueIndex(pos)),
                        equalTo(lb.getLong(lb.getFirstValueIndex(pos)))
                    );
                } else if (expected instanceof IntBlock ib && actual instanceof IntBlock iab) {
                    assertThat(
                        ctx + " int at " + pos,
                        iab.getInt(iab.getFirstValueIndex(pos)),
                        equalTo(ib.getInt(ib.getFirstValueIndex(pos)))
                    );
                } else if (expected instanceof DoubleBlock db && actual instanceof DoubleBlock dab) {
                    assertThat(
                        ctx + " double at " + pos,
                        dab.getDouble(dab.getFirstValueIndex(pos)),
                        equalTo(db.getDouble(db.getFirstValueIndex(pos)))
                    );
                } else if (expected instanceof BooleanBlock bb && actual instanceof BooleanBlock bab) {
                    assertThat(
                        ctx + " boolean at " + pos,
                        bab.getBoolean(bab.getFirstValueIndex(pos)),
                        equalTo(bb.getBoolean(bb.getFirstValueIndex(pos)))
                    );
                } else if (expected instanceof BytesRefBlock brb && actual instanceof BytesRefBlock brab) {
                    assertThat(
                        ctx + " bytesref at " + pos,
                        brab.getBytesRef(brab.getFirstValueIndex(pos), new org.apache.lucene.util.BytesRef()),
                        equalTo(brb.getBytesRef(brb.getFirstValueIndex(pos), new org.apache.lucene.util.BytesRef()))
                    );
                }
            }
        }
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
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (Group group : groups) {
                writer.write(group);
            }
        }
        return outputStream.toByteArray();
    }

    /**
     * Creates a Parquet file with dictionary encoding enabled and column indexes written.
     * Uses a small row group size to ensure dictionary pages are present.
     */
    private byte[] createParquetFileWithDictionary(MessageType schema, GroupCreator groupCreator) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        List<Group> groups = groupCreator.create(groupFactory);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(true)
                .withPageWriteChecksumEnabled(false)
                .build()
        ) {
            for (Group group : groups) {
                writer.write(group);
            }
        }
        return outputStream.toByteArray();
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        outputStream.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        outputStream.write(b, off, len);
                        position += len;
                    }

                    @Override
                    public void close() throws IOException {
                        outputStream.close();
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

            @Override
            public String getPath() {
                return "memory://test.parquet";
            }
        };
    }

    // --- Page-level skipping tests (Stage 2) ---

    /**
     * Page-level skipping with sorted data: a range predicate on a sorted column should skip
     * pages whose min/max don't overlap with the predicate range. Correctness parity with baseline.
     */
    public void testPageSkippingSortedColumnCorrectnessParity() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "user_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        GreaterThan gtExpr = new GreaterThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 900L, DataType.LONG));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> optimizedPages = readAllPages(optimizedReader, storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Page-level skipping: predicate that matches no pages should return same results as baseline
     * (which uses parquet-java's built-in statistics filtering).
     */
    public void testPageSkippingNoMatchingPages() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        GreaterThan gtExpr = new GreaterThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 99999L, DataType.LONG));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(optimizedReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("page-level skipping should return 0 rows for out-of-range predicate", totalRows, equalTo(0));
    }

    /**
     * Page-level skipping with LessThan on sorted column: correctness parity.
     */
    public void testPageSkippingLessThanSortedColumn() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("value", i * 10);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        LessThan ltExpr = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 50L, DataType.LONG));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(ltExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> optimizedPages = readAllPages(optimizedReader, storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Page-level skipping with Equals on string column: correctness parity.
     */
    public void testPageSkippingEqualsStringColumn() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("category")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            String[] categories = { "alpha", "beta", "gamma", "delta" };
            for (int i = 0; i < 400; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("category", categories[i / 100]);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute catAttr = new ReferenceAttribute(Source.EMPTY, "category", DataType.KEYWORD);
        Equals equalsExpr = new Equals(Source.EMPTY, catAttr, new Literal(Source.EMPTY, new BytesRef("beta"), DataType.KEYWORD));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> optimizedPages = readAllPages(optimizedReader, storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Page-level skipping: no pushed filter means no page skipping, correctness parity maintained.
     */
    public void testPageSkippingNoPushedFilter() throws Exception {
        MessageType schema = Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.INT64).named("id").named("test_schema");

        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Page-level skipping with DATETIME (TIMESTAMP_MILLIS) column: the optimized reader should
     * convert ESQL epoch millis to the physical unit before comparing against ColumnIndex min/max.
     * Correctness parity with baseline.
     */
    public void testPageSkippingDatetimeTimestampMillis() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .named("test_schema");

        long baseMillis = 1_700_000_000_000L; // ~2023-11-14
        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                Group g = factory.newGroup();
                g.add("ts", baseMillis + i * 86_400_000L); // one day apart
                g.add("id", (long) i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        long filterMillis = baseMillis + 450 * 86_400_000L;
        ReferenceAttribute tsAttr = new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME);
        GreaterThan gtExpr = new GreaterThan(Source.EMPTY, tsAttr, new Literal(Source.EMPTY, filterMillis, DataType.DATETIME));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> optimizedPages = readAllPages(optimizedReader, storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Page-level skipping with DATE (INT32 days) column: ESQL epoch millis should be
     * converted to days before comparing against ColumnIndex min/max.
     */
    public void testPageSkippingDateColumn() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.dateType())
            .named("dt")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                Group g = factory.newGroup();
                g.add("dt", i); // days since epoch
                g.add("id", (long) i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        long filterMillis = 450L * 86_400_000L; // day 450 in millis
        ReferenceAttribute dtAttr = new ReferenceAttribute(Source.EMPTY, "dt", DataType.DATETIME);
        GreaterThan gtExpr = new GreaterThan(Source.EMPTY, dtAttr, new Literal(Source.EMPTY, filterMillis, DataType.DATETIME));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> optimizedPages = readAllPages(optimizedReader, storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Page-level skipping with TIMESTAMP_MICROS column: ESQL epoch millis should be
     * converted to microseconds before comparing against ColumnIndex min/max.
     */
    public void testPageSkippingTimestampMicros() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .named("test_schema");

        long baseMillis = 1_700_000_000_000L;
        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                Group g = factory.newGroup();
                g.add("ts", (baseMillis + i * 86_400_000L) * 1000L); // micros
                g.add("id", (long) i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        long filterMillis = baseMillis + 450 * 86_400_000L;
        ReferenceAttribute tsAttr = new ReferenceAttribute(Source.EMPTY, "ts", DataType.DATETIME);
        GreaterThan gtExpr = new GreaterThan(Source.EMPTY, tsAttr, new Literal(Source.EMPTY, filterMillis, DataType.DATETIME));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> optimizedPages = readAllPages(optimizedReader, storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Creates a Parquet file with column indexes and small page size to ensure multiple pages
     * per row group, enabling page-level skipping tests.
     */
    private byte[] createParquetFileWithColumnIndexes(MessageType schema, GroupCreator groupCreator) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(outputStream);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        List<Group> groups = groupCreator.create(groupFactory);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withDictionaryEncoding(false)
                .withPageSize(256)
                .withRowGroupSize(64L * 1024)
                .withPageWriteChecksumEnabled(false)
                .build()
        ) {
            for (Group group : groups) {
                writer.write(group);
            }
        }
        return outputStream.toByteArray();
    }

    // --- Stage 3: Batch column reader tests ---

    /**
     * Batch reader: non-nullable columns use the fast path (no def-level check).
     * Correctness parity with baseline.
     */
    public void testBatchReaderNonNullableColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("count")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("flag")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("count", i * 10);
                g.add("score", i * 0.1);
                g.add("flag", i % 2 == 0);
                g.add("label", "item_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader: nullable columns with mixed null/non-null values.
     * Tests direct BitSet construction in BatchColumnReader.
     */
    public void testBatchReaderNullableColumnsWithNulls() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("nullable_long")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("nullable_int")
            .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("nullable_double")
            .optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("nullable_bool")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("nullable_str")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 150; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                if (i % 3 != 0) {
                    g.add("nullable_long", (long) (i * 100));
                }
                if (i % 4 != 0) {
                    g.add("nullable_int", i * 10);
                }
                if (i % 5 != 0) {
                    g.add("nullable_double", i * 0.5);
                }
                if (i % 7 != 0) {
                    g.add("nullable_bool", i % 2 == 0);
                }
                if (i % 2 != 0) {
                    g.add("nullable_str", "val_" + i);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader: all-null optional columns should produce correct null blocks.
     */
    public void testBatchReaderAllNullColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("all_null_long")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("all_null_str")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader: INT32 widened to LONG via planner type. Tests the INT32-as-LONG batch path.
     */
    public void testBatchReaderInt32WidenedToLong() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("small_int")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("nullable_small_int")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("small_int", i);
                if (i % 3 != 0) {
                    g.add("nullable_small_int", i * 2);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader: FLOAT columns read as DOUBLE. Tests the float-to-double batch path.
     */
    public void testBatchReaderFloatAsDouble() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("float_val")
            .optional(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("nullable_float")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                Group g = factory.newGroup();
                g.add("float_val", (float) (i * 0.25));
                if (i % 4 != 0) {
                    g.add("nullable_float", (float) (i * 1.5));
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader: DATETIME columns (TIMESTAMP_MILLIS, TIMESTAMP_MICROS, DATE).
     * Tests timestamp conversion in the batch path.
     */
    public void testBatchReaderDatetimeColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts_millis")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts_micros")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.dateType())
            .named("dt")
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("nullable_ts")
            .named("test_schema");

        long baseMillis = 1_700_000_000_000L;
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("ts_millis", baseMillis + i * 1000L);
                g.add("ts_micros", (baseMillis + i * 1000L) * 1000L);
                g.add("dt", i);
                if (i % 3 != 0) {
                    g.add("nullable_ts", baseMillis + i * 60_000L);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader: DECIMAL columns read as DOUBLE via the batch path.
     */
    public void testBatchReaderDecimalAsDouble() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("price_int")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.decimalType(4, 18))
            .named("price_long")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("nullable_price")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                Group g = factory.newGroup();
                g.add("price_int", i * 100 + 99);
                g.add("price_long", (long) i * 10000 + 1234);
                if (i % 3 != 0) {
                    g.add("nullable_price", i * 50);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader: mixed flat and list columns in the same file. Flat columns use BatchColumnReader,
     * list columns fall back to the row-at-a-time path. Both must produce correct results.
     */
    public void testBatchReaderMixedFlatAndListColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .requiredList()
            .requiredElement(PrimitiveType.PrimitiveTypeName.INT32)
            .named("tags")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("score", i * 0.5);
                Group tagsList = g.addGroup("tags");
                for (int j = 0; j < (i % 3) + 1; j++) {
                    tagsList.addGroup("list").append("element", i * 10 + j);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader: large batch to test page boundary handling within BatchColumnReader.
     * Uses a batch size larger than a single page to ensure page transitions work correctly.
     */
    public void testBatchReaderLargeBatchAcrossPages() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("data")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 2000; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("data", "row_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    /**
     * Batch reader with pushed filter: verifies that batch decoding works correctly
     * alongside dictionary pruning and page-level skipping.
     */
    public void testBatchReaderWithPushedFilter() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 3 == 0 ? "active" : (i % 3 == 1 ? "inactive" : "pending"));
                g.add("value", i * 0.1);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.KEYWORD);
        Equals equalsExpr = new Equals(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, new BytesRef("active"), DataType.KEYWORD));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader optimizedReader = new ParquetFormatReader(blockFactory, true);
        optimizedReader = (ParquetFormatReader) optimizedReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> optimizedPages = readAllPages(optimizedReader, storageObject);

        assertPagesEqual(baselinePages, optimizedPages);
    }

    // --- Stage 3b: Page-level reader tests ---

    /**
     * Page-level reader: correctness parity with simple non-nullable types.
     */
    public void testPageLevelReaderSimpleTypes() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("active")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "user_" + i);
                g.add("age", 20 + (i % 50));
                g.add("active", i % 2 == 0);
                g.add("score", i * 1.5);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader: correctness parity with nullable columns.
     */
    public void testPageLevelReaderNullableColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("age")
            .optional(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("flag")
            .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                if (i % 3 != 0) {
                    g.add("name", "user_" + i);
                }
                if (i % 5 != 0) {
                    g.add("age", 20 + i);
                }
                if (i % 4 != 0) {
                    g.add("flag", i % 2 == 0);
                }
                if (i % 7 != 0) {
                    g.add("score", i * 0.5);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader: INT32 widened to LONG and FLOAT widened to DOUBLE.
     */
    public void testPageLevelReaderTypeWidening() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("int_as_long")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .named("nullable_int_as_long")
            .required(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("float_as_double")
            .optional(PrimitiveType.PrimitiveTypeName.FLOAT)
            .named("nullable_float")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                Group g = factory.newGroup();
                g.add("int_as_long", i * 100);
                if (i % 3 != 0) {
                    g.add("nullable_int_as_long", i * 50);
                }
                g.add("float_as_double", (float) (i * 0.25));
                if (i % 4 != 0) {
                    g.add("nullable_float", (float) (i * 1.5));
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader: DATETIME columns (TIMESTAMP_MILLIS, TIMESTAMP_MICROS, DATE).
     */
    public void testPageLevelReaderDatetimeColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts_millis")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts_micros")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.dateType())
            .named("dt")
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("nullable_ts")
            .named("test_schema");

        long baseMillis = 1_700_000_000_000L;
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("ts_millis", baseMillis + i * 1000L);
                g.add("ts_micros", (baseMillis + i * 1000L) * 1000L);
                g.add("dt", i);
                if (i % 3 != 0) {
                    g.add("nullable_ts", baseMillis + i * 60_000L);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader: DECIMAL columns read as DOUBLE.
     */
    public void testPageLevelReaderDecimalAsDouble() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("price_int")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.decimalType(4, 18))
            .named("price_long")
            .optional(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("nullable_price")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                Group g = factory.newGroup();
                g.add("price_int", i * 100 + 99);
                g.add("price_long", (long) i * 10000 + 1234);
                if (i % 3 != 0) {
                    g.add("nullable_price", i * 50);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader: mixed flat + list columns. Flat columns use PageColumnReader,
     * list columns fall back to ColumnReader row-at-a-time path.
     */
    public void testPageLevelReaderMixedFlatAndListColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .requiredList()
            .requiredElement(PrimitiveType.PrimitiveTypeName.INT32)
            .named("tags")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("score", i * 0.5);
                Group tagsList = g.addGroup("tags");
                for (int j = 0; j < (i % 3) + 1; j++) {
                    tagsList.addGroup("list").append("element", i * 10 + j);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader: dictionary-encoded columns.
     */
    public void testPageLevelReaderDictionaryEncoded() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 3 == 0 ? "active" : (i % 3 == 1 ? "inactive" : "pending"));
                g.add("value", i * 0.1);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader: large batch across multiple pages.
     */
    public void testPageLevelReaderLargeBatchAcrossPages() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("data")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithColumnIndexes(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 2000; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("data", "row_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader with pushed filter: verifies page-level decode works alongside
     * dictionary pruning and page-level skipping.
     */
    public void testPageLevelReaderWithPushedFilter() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 3 == 0 ? "active" : (i % 3 == 1 ? "inactive" : "pending"));
                g.add("value", i * 0.1);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.KEYWORD);
        Equals equalsExpr = new Equals(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, new BytesRef("active"), DataType.KEYWORD));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader baselineReader = new ParquetFormatReader(blockFactory, false);
        baselineReader = (ParquetFormatReader) baselineReader.withPushedFilter(pushedExprs);

        ParquetFormatReader pageLevelReader = new ParquetFormatReader(blockFactory, true, true);
        pageLevelReader = (ParquetFormatReader) pageLevelReader.withPushedFilter(pushedExprs);

        List<Page> baselinePages = readAllPages(baselineReader, storageObject);
        List<Page> pageLevelPages = readAllPages(pageLevelReader, storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    /**
     * Page-level reader: all-null optional column.
     */
    public void testPageLevelReaderAllNullColumn() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("nullable_val")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> pageLevelPages = readAllPages(new ParquetFormatReader(blockFactory, true, true), storageObject);

        assertPagesEqual(baselinePages, pageLevelPages);
    }

    // --- Stage 4: Late materialization tests ---

    /**
     * Late materialization: filtered read with predicate on one column, projection on others.
     * Verifies that results match the page-level reader (which decodes all columns then lets
     * parquet-java's built-in filter drive row group selection).
     */
    public void testLateMaterializationBasicFilter() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("value", i * 0.5);
                g.add("label", "item_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        GreaterThan gtExpr = new GreaterThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 200L, DataType.LONG));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));

        ParquetFormatReader pageLevelReader = new ParquetFormatReader(blockFactory, true, true);
        pageLevelReader = (ParquetFormatReader) pageLevelReader.withPushedFilter(pushedExprs);

        ParquetFormatReader lateMatReader = new ParquetFormatReader(blockFactory, true, true, true);
        lateMatReader = (ParquetFormatReader) lateMatReader.withPushedFilter(pushedExprs);

        List<Page> pageLevelPages = readAllPages(pageLevelReader, storageObject);
        List<Page> lateMatPages = readAllPages(lateMatReader, storageObject);

        int pageLevelTotalRows = pageLevelPages.stream().mapToInt(Page::getPositionCount).sum();
        int lateMatTotalRows = lateMatPages.stream().mapToInt(Page::getPositionCount).sum();

        // Late materialization should return fewer rows (only those matching id > 200)
        assertThat("late materialization should produce fewer rows than page-level reader", lateMatTotalRows, lessThan(pageLevelTotalRows));
        // Should return exactly 99 rows (id 201..299)
        assertThat("late materialization should match expected surviving row count", lateMatTotalRows, equalTo(99));
    }

    /**
     * Late materialization with no pushed filter: all columns decoded normally, no filtering.
     * Results should match page-level reader exactly.
     */
    public void testLateMaterializationNoFilter() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("count")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("count", i * 10);
                g.add("score", i * 0.1);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        // No pushed filter → lateMaterialization should effectively be disabled (no predicate columns)
        ParquetFormatReader pageLevelReader = new ParquetFormatReader(blockFactory, true, true);
        ParquetFormatReader lateMatReader = new ParquetFormatReader(blockFactory, true, true, true);

        List<Page> pageLevelPages = readAllPages(pageLevelReader, storageObject);
        List<Page> lateMatPages = readAllPages(lateMatReader, storageObject);

        assertPagesEqual(pageLevelPages, lateMatPages);
    }

    /**
     * Late materialization with Equals on keyword column: only rows matching the string value
     * should survive.
     */
    public void testLateMaterializationKeywordEquals() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("status")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFileWithDictionary(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("status", i % 3 == 0 ? "active" : (i % 3 == 1 ? "inactive" : "pending"));
                g.add("value", i * 0.1);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute statusAttr = new ReferenceAttribute(Source.EMPTY, "status", DataType.KEYWORD);
        Equals equalsExpr = new Equals(Source.EMPTY, statusAttr, new Literal(Source.EMPTY, new BytesRef("active"), DataType.KEYWORD));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(equalsExpr));

        ParquetFormatReader lateMatReader = new ParquetFormatReader(blockFactory, true, true, true);
        lateMatReader = (ParquetFormatReader) lateMatReader.withPushedFilter(pushedExprs);

        List<Page> lateMatPages = readAllPages(lateMatReader, storageObject);
        int totalRows = lateMatPages.stream().mapToInt(Page::getPositionCount).sum();

        // 100 rows where status == "active" (every 3rd row: 0, 3, 6, ..., 297)
        assertThat("late materialization should return only matching rows", totalRows, equalTo(100));
    }

    /**
     * Late materialization with nullable columns: verifies null handling in predicate evaluation
     * and block filtering.
     */
    public void testLateMaterializationNullableColumns() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveType.PrimitiveTypeName.INT64)
            .named("filter_col")
            .optional(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("projection_col")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                if (i % 5 != 0) {
                    g.add("filter_col", (long) i);
                }
                if (i % 3 != 0) {
                    g.add("projection_col", i * 0.1);
                }
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute filterAttr = new ReferenceAttribute(Source.EMPTY, "filter_col", DataType.LONG);
        GreaterThan gtExpr = new GreaterThan(Source.EMPTY, filterAttr, new Literal(Source.EMPTY, 150L, DataType.LONG));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));

        ParquetFormatReader lateMatReader = new ParquetFormatReader(blockFactory, true, true, true);
        lateMatReader = (ParquetFormatReader) lateMatReader.withPushedFilter(pushedExprs);

        List<Page> lateMatPages = readAllPages(lateMatReader, storageObject);
        int totalRows = lateMatPages.stream().mapToInt(Page::getPositionCount).sum();

        // Rows where filter_col > 150: id 151,152,...,199 excluding multiples of 5 (null filter_col)
        // Multiples of 5 from 155..195: 155,160,165,170,175,180,185,190,195 = 9 nulls
        // Total: 49 - 9 = 40 surviving rows (null filter_col rows are eliminated by GT comparison)
        int expectedNulls = 0;
        int expectedSurvivors = 0;
        for (int i = 0; i < 200; i++) {
            if (i % 5 == 0) continue;
            if (i > 150) expectedSurvivors++;
        }
        assertThat("late materialization with nullable filter column", totalRows, equalTo(expectedSurvivors));
    }

    /**
     * Late materialization: predicate column also in projection. The predicate column Block
     * should be decoded once and reused in the output Page.
     */
    public void testLateMaterializationPredicateInProjection() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("value", i * 2.0);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        LessThan ltExpr = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 10L, DataType.LONG));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(ltExpr));

        ParquetFormatReader lateMatReader = new ParquetFormatReader(blockFactory, true, true, true);
        lateMatReader = (ParquetFormatReader) lateMatReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(lateMatReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();

        assertThat("predicate column (id) in projection, LessThan filter", totalRows, equalTo(10));

        // Verify the id column values are 0..9
        for (Page page : pages) {
            LongBlock idBlock = page.getBlock(0);
            for (int i = 0; i < page.getPositionCount(); i++) {
                long idVal = idBlock.getLong(idBlock.getFirstValueIndex(i));
                assertThat("surviving id should be < 10", idVal, lessThan(10L));
            }
        }
    }

    /**
     * Late materialization: all rows eliminated by filter. Projection columns should be skipped
     * entirely (no decode).
     */
    public void testLateMaterializationAllRowsEliminated() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("value", i * 0.5);
                g.add("label", "item_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        GreaterThan gtExpr = new GreaterThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 99999L, DataType.LONG));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));

        ParquetFormatReader lateMatReader = new ParquetFormatReader(blockFactory, true, true, true);
        lateMatReader = (ParquetFormatReader) lateMatReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(lateMatReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();

        assertThat("all rows eliminated by impossible filter", totalRows, equalTo(0));
    }

    /**
     * Late materialization with boolean predicate column.
     */
    public void testLateMaterializationBooleanPredicate() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
            .named("flag")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("flag", i % 4 == 0);
                g.add("score", i * 1.5);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute flagAttr = new ReferenceAttribute(Source.EMPTY, "flag", DataType.BOOLEAN);
        Equals eqExpr = new Equals(Source.EMPTY, flagAttr, new Literal(Source.EMPTY, true, DataType.BOOLEAN));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(eqExpr));

        ParquetFormatReader lateMatReader = new ParquetFormatReader(blockFactory, true, true, true);
        lateMatReader = (ParquetFormatReader) lateMatReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(lateMatReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();

        // flag == true for every 4th row: 0, 4, 8, ..., 196 = 50 rows
        assertThat("boolean predicate filter", totalRows, equalTo(50));
    }

    /**
     * Late materialization with integer predicate column.
     */
    public void testLateMaterializationIntegerPredicate() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("category")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                Group g = factory.newGroup();
                g.add("category", i % 5);
                g.add("id", (long) i);
                g.add("score", i * 0.3);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);

        ReferenceAttribute catAttr = new ReferenceAttribute(Source.EMPTY, "category", DataType.INTEGER);
        Equals eqExpr = new Equals(Source.EMPTY, catAttr, new Literal(Source.EMPTY, 0, DataType.INTEGER));
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(eqExpr));

        ParquetFormatReader lateMatReader = new ParquetFormatReader(blockFactory, true, true, true);
        lateMatReader = (ParquetFormatReader) lateMatReader.withPushedFilter(pushedExprs);

        List<Page> pages = readAllPages(lateMatReader, storageObject);
        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();

        // category == 0 for every 5th row: 0, 5, 10, ..., 195 = 40 rows
        assertThat("integer predicate filter", totalRows, equalTo(40));
    }

    // --- Helpers ---

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
                return StoragePath.of("memory://test.parquet");
            }
        };
    }
}
