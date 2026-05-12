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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
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
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class OptimizedParquetReaderTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
    }

    public void testFormatUuidValidBytes() {
        byte[] bytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            bytes[i] = (byte) (i * 16 + i);
        }
        String uuid = ParquetColumnDecoding.formatUuid(bytes);
        assertNotNull(uuid);
        assertEquals(36, uuid.length());
        assertEquals('-', uuid.charAt(8));
        assertEquals('-', uuid.charAt(13));
        assertEquals('-', uuid.charAt(18));
        assertEquals('-', uuid.charAt(23));
    }

    public void testFormatUuidNullThrows() {
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class, () -> ParquetColumnDecoding.formatUuid(null));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("null"));
    }

    public void testFormatUuidTooShortThrows() {
        byte[] bytes = new byte[10];
        QlIllegalArgumentException e = expectThrows(QlIllegalArgumentException.class, () -> ParquetColumnDecoding.formatUuid(bytes));
        assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("10"));
    }

    public void testWithConfigOptimizedReaderTrue() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", true));
        assertSame(reader, configured);
    }

    public void testWithConfigOptimizedReaderFalse() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        ParquetFormatReader configured = (ParquetFormatReader) reader.withConfig(Map.of("optimized_reader", false));
        assertNotSame(reader, configured);
    }

    public void testWithConfigDefaults() {
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory);
        assertSame(reader, reader.withConfig(null));
        assertSame(reader, reader.withConfig(Map.of()));
    }

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

    // --- Late Materialization Tests ---

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

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression gtExpr = new GreaterThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 200L, DataType.LONG), null);
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushedExprs);

        StorageObject storageObject = createStorageObject(parquetData);
        List<Page> pages = readAllPages(reader, storageObject);

        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        // id > 200 means ids 201..299 => 99 rows
        assertThat("total rows after filter", totalRows, equalTo(99));

        // Verify id values are in range 201..299
        int rowIdx = 0;
        for (Page page : pages) {
            LongBlock idBlock = page.getBlock(0);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                long id = idBlock.getLong(pos);
                assertTrue("id " + id + " should be > 200", id > 200);
                assertTrue("id " + id + " should be < 300", id < 300);
                rowIdx++;
            }
        }
        assertThat("verified row count", rowIdx, equalTo(99));
    }

    public void testLateMaterializationNoFilter() throws Exception {
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
                g.add("value", i * 1.0);
                g.add("label", "item_" + i);
                groups.add(g);
            }
            return groups;
        });

        StorageObject storageObject = createStorageObject(parquetData);
        // No pushed filter — late materialization should not activate
        List<Page> baselinePages = readAllPages(new ParquetFormatReader(blockFactory, false), storageObject);
        List<Page> optimizedPages = readAllPages(new ParquetFormatReader(blockFactory, true), storageObject);
        assertPagesEqual(baselinePages, optimizedPages);
    }

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

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression gtExpr = new GreaterThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 99999L, DataType.LONG), null);
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushedExprs);

        StorageObject storageObject = createStorageObject(parquetData);
        List<Page> pages = readAllPages(reader, storageObject);

        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("all rows should be eliminated", totalRows, equalTo(0));
    }

    public void testLateMaterializationPredicateInProjection() throws Exception {
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("value")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("big_col1")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("big_col2")
            .required(PrimitiveType.PrimitiveTypeName.INT64)
            .named("big_col3")
            .named("test_schema");

        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Group g = factory.newGroup();
                g.add("id", i);
                g.add("value", i * 2.0);
                g.add("big_col1", (long) i * 100);
                g.add("big_col2", (long) i * 200);
                g.add("big_col3", (long) i * 300);
                groups.add(g);
            }
            return groups;
        });

        ReferenceAttribute idAttr = new ReferenceAttribute(Source.EMPTY, "id", DataType.INTEGER);
        Expression ltExpr = new LessThan(Source.EMPTY, idAttr, new Literal(Source.EMPTY, 10, DataType.INTEGER), null);
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(ltExpr));
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushedExprs);

        StorageObject storageObject = createStorageObject(parquetData);
        List<Page> pages = readAllPages(reader, storageObject);

        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("should have 10 rows (id < 10)", totalRows, equalTo(10));

        int rowIdx = 0;
        for (Page page : pages) {
            IntBlock idBlock = page.getBlock(0);
            DoubleBlock valueBlock = page.getBlock(1);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                int id = idBlock.getInt(pos);
                double value = valueBlock.getDouble(pos);
                assertThat("id at row " + rowIdx, id, equalTo(rowIdx));
                assertThat("value at row " + rowIdx, value, equalTo(rowIdx * 2.0));
                rowIdx++;
            }
        }
    }

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
                    g.add("projection_col", i * 1.5);
                }
                groups.add(g);
            }
            return groups;
        });

        ReferenceAttribute filterAttr = new ReferenceAttribute(Source.EMPTY, "filter_col", DataType.LONG);
        Expression gtExpr = new GreaterThan(Source.EMPTY, filterAttr, new Literal(Source.EMPTY, 150L, DataType.LONG), null);
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushedExprs);

        StorageObject storageObject = createStorageObject(parquetData);
        List<Page> pages = readAllPages(reader, storageObject);

        int totalRows = pages.stream().mapToInt(Page::getPositionCount).sum();

        // Compute expected: rows where filter_col > 150 AND filter_col is not null
        // filter_col is null when i % 5 == 0, otherwise filter_col == i
        // So we need i > 150 AND i % 5 != 0
        int expectedRows = 0;
        for (int i = 0; i < 200; i++) {
            if (i % 5 != 0 && i > 150) {
                expectedRows++;
            }
        }
        assertThat("survivor count for filter_col > 150 with nulls", totalRows, equalTo(expectedRows));
    }

    public void testLateMaterializationWithRowLimit() throws Exception {
        // 3 columns: predicate (int), projection-only (string), projection-only (double)
        // The int predicate column is narrow; the two projection columns are wider.
        // This guarantees the predicate byte ratio < 0.5, activating late materialization.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("filter_col")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("label")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE)
            .named("score")
            .named("test_schema");

        int totalRows = 200;
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < totalRows; i++) {
                Group g = factory.newGroup();
                g.add("filter_col", i);
                // Use a long repeating string to make the string column wide relative to the int column
                g.add("label", "description_for_item_number_" + i + "_with_extra_padding_to_increase_byte_size");
                g.add("score", i * 3.14);
                groups.add(g);
            }
            return groups;
        });

        // Highly selective filter: filter_col > 190 matches rows 191..199 = 9 rows (~4.5%)
        ReferenceAttribute filterAttr = new ReferenceAttribute(Source.EMPTY, "filter_col", DataType.INTEGER);
        Expression gtExpr = new GreaterThan(Source.EMPTY, filterAttr, new Literal(Source.EMPTY, 190, DataType.INTEGER), null);
        ParquetPushedExpressions pushedExprs = new ParquetPushedExpressions(List.of(gtExpr));
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushedExprs);

        // Set row limit smaller than the 9 matching rows
        int rowLimit = 5;
        StorageObject storageObject = createStorageObject(parquetData);
        List<Page> pages;
        try (
            CloseableIterator<Page> iter = reader.read(
                storageObject,
                FormatReadContext.builder().batchSize(1024).rowLimit(rowLimit).build()
            )
        ) {
            pages = collectPages(iter);
        }

        int outputRows = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("output row count should equal the row limit", outputRows, equalTo(rowLimit));

        // Verify the output values are correct (not corrupted by the filter/compact cycle)
        int rowIdx = 0;
        for (Page page : pages) {
            IntBlock filterBlock = page.getBlock(0);
            BytesRefBlock labelBlock = page.getBlock(1);
            DoubleBlock scoreBlock = page.getBlock(2);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                int filterVal = filterBlock.getInt(pos);
                assertTrue("filter_col value " + filterVal + " should be > 190", filterVal > 190);
                assertTrue("filter_col value " + filterVal + " should be < 200", filterVal < 200);

                // Verify the label and score correspond to the filter value
                String label = labelBlock.getBytesRef(pos, new org.apache.lucene.util.BytesRef()).utf8ToString();
                String expectedLabel = "description_for_item_number_" + filterVal + "_with_extra_padding_to_increase_byte_size";
                assertThat("label at row " + rowIdx, label, equalTo(expectedLabel));
                assertThat("score at row " + rowIdx, scoreBlock.getDouble(pos), equalTo(filterVal * 3.14));
                rowIdx++;
            }
        }
    }

    /**
     * Late materialization filters rows when the predicate column is a small fraction of the
     * projected bytes (the easy case where the file-level byte-ratio gate, when it still existed,
     * was always permissive).
     */
    public void testLateMaterializationFiltersWhenPredicateColumnIsNarrow() throws Exception {
        // Predicate column is a narrow int; projection columns are wide strings. The projection-only
        // bytes dominate, so deferring their decode for non-matching rows is clearly profitable.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("pred_col")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("wide_col1")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("wide_col2")
            .named("test_schema");

        int totalRows = 100;
        String padding = "x".repeat(200);
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < totalRows; i++) {
                Group g = factory.newGroup();
                g.add("pred_col", i);
                g.add("wide_col1", padding + "_col1_" + i);
                g.add("wide_col2", padding + "_col2_" + i);
                groups.add(g);
            }
            return groups;
        });

        ReferenceAttribute predAttr = new ReferenceAttribute(Source.EMPTY, "pred_col", DataType.INTEGER);
        Expression filter = new GreaterThan(Source.EMPTY, predAttr, new Literal(Source.EMPTY, 89, DataType.INTEGER), null);
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));
        ParquetFormatReader reader = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);

        StorageObject storageObject = createStorageObject(parquetData);
        List<Page> pages = readAllPages(reader, storageObject);

        int totalRowsOut = pages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("late-mat active should retain only matching rows", totalRowsOut, equalTo(10));

        for (Page page : pages) {
            IntBlock predBlock = page.getBlock(0);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                int val = predBlock.getInt(pos);
                assertTrue("pred_col " + val + " should be > 89", val > 89);
            }
        }
    }

    /**
     * Late materialization is no longer file-gated by predicate-byte ratio. Even when the predicate
     * column dominates the projected bytes — the regression shape that motivated removing the gate
     * — late-mat still fires and filters rows. Under the current {@code Pushability.YES} rule for
     * fully-evaluable conjuncts, the upstream {@code FilterExec} is dropped from the plan, so any
     * file-level suppression of late-mat would leak unfiltered rows past the source. The expensive
     * two-phase prefetch path stays gated by its own threshold inside the iterator, which is the
     * correct scope for that decision (verified separately by {@code TwoPhaseReaderTests}).
     */
    public void testLateMaterializationStillFiresWhenPredicateColumnDominates() throws Exception {
        // Predicate column is a wide string (~200+ bytes/row); projection-only column is a narrow
        // int. The byte ratio is well above the old 0.5 file-level threshold; the test pins that
        // late-mat is no longer gated off in this shape.
        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("wide_pred")
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("narrow_proj")
            .named("test_schema");

        int totalRows = 100;
        String padding = "x".repeat(200);
        byte[] parquetData = createParquetFile(schema, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int i = 0; i < totalRows; i++) {
                Group g = factory.newGroup();
                g.add("wide_pred", padding + "_pred_" + i);
                g.add("narrow_proj", i);
                groups.add(g);
            }
            return groups;
        });

        // Lexicographic > "padding_pred_94" matches values ending in _95, _96, _97, _98, _99 = 5 rows.
        // (e.g. "_pred_8" < "_pred_94" because '8' < '9' at the first differing position.)
        ReferenceAttribute predAttr = new ReferenceAttribute(Source.EMPTY, "wide_pred", DataType.KEYWORD);
        Expression filter = new GreaterThan(
            Source.EMPTY,
            predAttr,
            new Literal(Source.EMPTY, new org.apache.lucene.util.BytesRef(padding + "_pred_94"), DataType.KEYWORD),
            null
        );
        ParquetPushedExpressions pushed = new ParquetPushedExpressions(List.of(filter));

        // With late-mat enabled (default), the file-level byte-ratio gate has been removed, so the
        // reader filters rows even though the predicate column dominates the byte footprint.
        ParquetFormatReader readerLateMat = new ParquetFormatReader(blockFactory, true).withPushedFilter(pushed);
        StorageObject storageObject = createStorageObject(parquetData);
        List<Page> pagesLateMat = readAllPages(readerLateMat, storageObject);
        int rowsLateMat = pagesLateMat.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("late-mat must filter rows even at high predicate-byte ratio", rowsLateMat, equalTo(5));

        // Sanity-check the surviving values.
        for (Page page : pagesLateMat) {
            BytesRefBlock predBlock = page.getBlock(0);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                String val = predBlock.getBytesRef(pos, new org.apache.lucene.util.BytesRef()).utf8ToString();
                assertTrue("wide_pred [" + val + "] should be > padding_pred_94", val.compareTo(padding + "_pred_94") > 0);
            }
        }

        // Contrast: with late-mat explicitly disabled via config, the optimized path does no
        // row-level filtering (only row-group/page statistics, which cannot prune within a single
        // row group), so all 100 rows come back. This is the existing kill-switch contract; the
        // file-level byte-ratio heuristic is gone but the explicit config knob is unchanged.
        ParquetFormatReader readerNoLateMat = ((ParquetFormatReader) new ParquetFormatReader(blockFactory, true).withConfig(
            Map.of(ParquetFormatReader.CONFIG_LATE_MATERIALIZATION, false)
        )).withPushedFilter(pushed);
        List<Page> pagesNoLateMat = readAllPages(readerNoLateMat, storageObject);
        int rowsNoLateMat = pagesNoLateMat.stream().mapToInt(Page::getPositionCount).sum();
        assertThat("explicit no-late-mat returns all rows (no row-level filtering)", rowsNoLateMat, equalTo(totalRows));
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
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
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
                return "memory://test.parquet";
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
