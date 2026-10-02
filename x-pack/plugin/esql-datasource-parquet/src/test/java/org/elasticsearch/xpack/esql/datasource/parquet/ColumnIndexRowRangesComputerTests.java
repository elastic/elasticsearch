/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.BoundaryOrder;
import org.apache.parquet.format.ColumnIndex;
import org.apache.parquet.format.OffsetIndex;
import org.apache.parquet.format.PageLocation;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link ColumnIndexRowRangesComputer} verifying that page-level min/max
 * evaluation produces correct RowRanges for various predicate types.
 */
public class ColumnIndexRowRangesComputerTests extends ESTestCase {

    private static final long ROW_GROUP_ROW_COUNT = 1000;
    private static final int PAGE_SIZE = 100;
    private static final int PAGE_COUNT = 10;

    public void testEqIntIncludesMatchingPage() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.intColumn("id"), 550);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page containing 550 should be included", result.overlaps(500, 600));
        assertFalse("Page [0,100) should be excluded", result.overlaps(0, 100));
        assertFalse("Page [200,300) should be excluded", result.overlaps(200, 300));
    }

    public void testEqIntExcludesAllPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.intColumn("id"), 5000);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);
        assertEquals("No pages should match value outside range", 0, result.selectedRowCount());
    }

    public void testGtIntIncludesHighPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gt(FilterApi.intColumn("id"), 700);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [700,800) should be included (max=799 > 700)", result.overlaps(700, 800));
        assertTrue("Page [800,900) should be included", result.overlaps(800, 900));
        assertTrue("Page [900,1000) should be included", result.overlaps(900, 1000));
        assertFalse("Page [0,100) should be excluded (max=99, not > 700)", result.overlaps(0, 100));
    }

    public void testGtEqIntIncludesMatchingPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gtEq(FilterApi.intColumn("id"), 500);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [500,600) should be included", result.overlaps(500, 600));
        assertTrue("Page [900,1000) should be included", result.overlaps(900, 1000));
        assertFalse("Page [0,100) should be excluded", result.overlaps(0, 100));
    }

    public void testLtIntIncludesLowPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.intColumn("id"), 200);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [0,100) should be included", result.overlaps(0, 100));
        assertTrue("Page [100,200) should be included (min=100 < 200)", result.overlaps(100, 200));
        assertFalse("Page [300,400) should be excluded (min=300, not < 200)", result.overlaps(300, 400));
    }

    public void testLtEqIntIncludesMatchingPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.ltEq(FilterApi.intColumn("id"), 199);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [0,100) should be included", result.overlaps(0, 100));
        assertTrue("Page [100,200) should be included (min=100 <= 199)", result.overlaps(100, 200));
        assertFalse("Page [200,300) should be excluded (min=200, not <= 199)", result.overlaps(200, 300));
    }

    public void testNotEqPrunesConstantPagesMatchingValue() {
        // Half the pages are constant value 5 (the excluded value), the other half are constant 99.
        // NotEq(5) must prune the value-5 pages and keep the rest. Result density must be low
        // enough to survive the RowRanges discard heuristic, hence pruning many pages here.
        int[] pageValues = { 5, 5, 5, 5, 5, 99, 99, 99, 99, 99 };
        PreloadedRowGroupMetadata metadata = buildConstantIntMetadata("id", pageValues, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), 5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertFalse("Page [0,100) (constant value 5) should be pruned by NotEq(5)", result.overlaps(0, 100));
        assertFalse("Page [400,500) (constant value 5) should be pruned by NotEq(5)", result.overlaps(400, 500));
        assertTrue("Page [500,600) (constant value 99) should survive", result.overlaps(500, 600));
        assertTrue("Page [900,1000) (constant value 99) should survive", result.overlaps(900, 1000));
        assertEquals("Exactly the value-99 pages survive", 500L, result.selectedRowCount());
    }

    public void testNotEqKeepsConstantPagesNotMatchingValue() {
        // None of the constant pages equal the excluded value, so every page survives the predicate.
        // Because the result selects all rows, RowRanges.shouldDiscard() normalizes it to the all-rows
        // sentinel, which is what isAll() observes here. The assertion verifies semantic correctness:
        // NotEq never wrongly drops rows when no page can be safely pruned.
        int[] pageValues = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        PreloadedRowGroupMetadata metadata = buildConstantIntMetadata("id", pageValues, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), 42);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("All pages should survive when value matches no constant page", result.isAll());
    }

    public void testNotEqKeepsPagesWithMixedValues() {
        // Pages have min < max, so NotEq cannot prune any page (only min == max == value is prunable).
        // Same isAll() / shouldDiscard() caveat as testNotEqKeepsConstantPagesNotMatchingValue.
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), 500);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("NotEq should keep all pages when no page has min == max == value", result.isAll());
    }

    public void testNotEqBinaryPrunesConstantPagesMatchingValue() {
        String[] pageValues = { "active", "active", "active", "active", "active", "idle", "idle", "idle", "idle", "idle" };
        PreloadedRowGroupMetadata metadata = buildConstantBinaryMetadata("status", pageValues, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.binaryColumn("status"), Binary.fromString("active"));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertFalse("Page [0,100) (constant 'active') should be pruned", result.overlaps(0, 100));
        assertFalse("Page [400,500) (constant 'active') should be pruned", result.overlaps(400, 500));
        assertTrue("Page [500,600) (constant 'idle') should survive", result.overlaps(500, 600));
        assertEquals("Exactly the 'idle' pages survive", 500L, result.selectedRowCount());
    }

    public void testNotEqWithNullValueReturnsAll() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), null);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("NotEq with null value (IS NOT NULL) should conservatively return all rows", result.isAll());
    }

    public void testAndIntersectsRanges() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.and(FilterApi.gtEq(FilterApi.intColumn("id"), 300), FilterApi.lt(FilterApi.intColumn("id"), 600));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [300,400) should be in intersection", result.overlaps(300, 400));
        assertTrue("Page [400,500) should be in intersection", result.overlaps(400, 500));
        assertTrue("Page [500,600) should be in intersection", result.overlaps(500, 600));
        assertFalse("Page [0,100) should not be in intersection", result.overlaps(0, 100));
        assertFalse("Page [700,800) should not be in intersection", result.overlaps(700, 800));
    }

    public void testOrUnionsRanges() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.or(FilterApi.eq(FilterApi.intColumn("id"), 50), FilterApi.eq(FilterApi.intColumn("id"), 950));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [0,100) should be included (contains 50)", result.overlaps(0, 100));
        assertTrue("Page [900,1000) should be included (contains 950)", result.overlaps(900, 1000));
        assertFalse("Page [400,500) should be excluded", result.overlaps(400, 500));
    }

    public void testNotReturnsAllConservatively() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.not(FilterApi.eq(FilterApi.intColumn("id"), 550));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertEquals(
            "NOT must conservatively return all rows (complement would drop mixed pages)",
            ROW_GROUP_ROW_COUNT,
            result.selectedRowCount()
        );
    }

    public void testInIncludesMatchingPages() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.in(FilterApi.intColumn("id"), Set.of(150, 750));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [100,200) should be included (contains 150)", result.overlaps(100, 200));
        assertTrue("Page [700,800) should be included (contains 750)", result.overlaps(700, 800));
        assertFalse("Page [400,500) should be excluded", result.overlaps(400, 500));
    }

    public void testMissingColumnIndexReturnsAll() {
        PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.empty();
        FilterPredicate pred = FilterApi.eq(FilterApi.intColumn("missing_col"), 42);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Missing ColumnIndex should return all rows", result.isAll());
    }

    public void testNullPredicateReturnsAll() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);

        RowRanges result = ColumnIndexRowRangesComputer.compute(null, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Null predicate should return all rows", result.isAll());
    }

    public void testEqWithNullValueReturnsAll() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.intColumn("id"), null);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Eq with null value (IS NULL) should return all rows", result.isAll());
    }

    public void testLongColumnEq() {
        PreloadedRowGroupMetadata metadata = buildLongMetadata("ts", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.longColumn("ts"), 550L);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page containing 550 should be included", result.overlaps(500, 600));
        assertFalse("Page [0,100) should be excluded", result.overlaps(0, 100));
    }

    public void testDoubleColumnGtEq() {
        PreloadedRowGroupMetadata metadata = buildDoubleMetadata("score", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gtEq(FilterApi.doubleColumn("score"), 800.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page [800,900) should be included", result.overlaps(800, 900));
        assertTrue("Page [900,1000) should be included", result.overlaps(900, 1000));
        assertFalse("Page [0,100) should be excluded", result.overlaps(0, 100));
    }

    public void testBinaryColumnEq() {
        PreloadedRowGroupMetadata metadata = buildBinaryMetadata("name", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.binaryColumn("name"), Binary.fromString("page_5"));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("Page with matching min/max should be included", result.overlaps(500, 600));
    }

    public void testLtKeepsNaNStatsPagesConservatively() {
        double[] mins = { -10.0, 10.0, -5.0, Double.NaN };
        double[] maxes = { -1.0, 15.0, Double.NaN, 5.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testEqKeepsNaNStatsPagesConservatively() {
        double[] mins = { 40.0, 100.0, 40.0, Double.NaN };
        double[] maxes = { 60.0, 200.0, Double.NaN, 60.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.doubleColumn("score"), 50.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testGtKeepsNaNStatsPagesConservatively() {
        double[] mins = { 50.0, 10.0, -5.0, Double.NaN };
        double[] maxes = { 150.0, 15.0, Double.NaN, 200.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gt(FilterApi.doubleColumn("score"), 100.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testLtKeepsAllNaNStatsPageConservatively() {
        double[] mins = { -10.0, 10.0, Double.NaN };
        double[] maxes = { -1.0, 15.0, Double.NaN };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 3L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("All-NaN page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
    }

    public void testLtEqKeepsNaNStatsPagesConservatively() {
        double[] mins = { -10.0, 10.0, -5.0, Double.NaN };
        double[] maxes = { -1.0, 15.0, Double.NaN, 5.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.ltEq(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testGtEqKeepsNaNStatsPagesConservatively() {
        double[] mins = { 50.0, 10.0, -5.0, Double.NaN };
        double[] maxes = { 150.0, 15.0, Double.NaN, 200.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.gtEq(FilterApi.doubleColumn("score"), 100.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testInKeepsNaNStatsPagesConservatively() {
        double[] mins = { 40.0, 100.0, 40.0, Double.NaN };
        double[] maxes = { 60.0, 200.0, Double.NaN, 60.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.in(FilterApi.doubleColumn("score"), Set.of(50.0, 55.0));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 4L * PAGE_SIZE);

        assertTrue("Real-stats matching page must be kept", result.overlaps(0, PAGE_SIZE));
        assertFalse("Real-stats failing page must be excluded", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(3L * PAGE_SIZE, 4L * PAGE_SIZE));
    }

    public void testNotEqKeepsNaNStatsPagesConservatively() {
        // NaN stats can never be proven equal to the value, so NotEq cannot prune NaN-stats pages.
        // The constant value-50 page (min == max == 50) is the only one safely prunable.
        double[] mins = { 50.0, -5.0, Double.NaN };
        double[] maxes = { 50.0, Double.NaN, 60.0 };
        PreloadedRowGroupMetadata metadata = buildDoubleMetadataExplicit("score", mins, maxes, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.doubleColumn("score"), 50.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, 3L * PAGE_SIZE);

        assertFalse("Constant value-50 page must be pruned by NotEq(50)", result.overlaps(0, PAGE_SIZE));
        assertTrue("NaN-max page must be kept conservatively", result.overlaps(PAGE_SIZE, 2L * PAGE_SIZE));
        assertTrue("NaN-min page must be kept conservatively", result.overlaps(2L * PAGE_SIZE, 3L * PAGE_SIZE));
    }

    // -----------------------------------------------------------------------------------
    // Schema-driven decode: FLOAT vs DOUBLE disambiguation and conservative reject for
    // non-native DOUBLE backings (DECIMAL, Float16).
    // -----------------------------------------------------------------------------------

    public void testFloatColumnDecodedViaFloatBuffers() {
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.FLOAT).named("score");
        ByteBuffer min = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(-10.0f).flip();
        ByteBuffer max = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.LITTLE_ENDIAN).putFloat(-1.0f).flip();
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("score", ptype, min, max, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue("FLOAT-encoded page satisfying Lt(0.5) must be included", result.overlaps(0, PAGE_SIZE));
    }

    public void testDoubleColumnDecodedViaDoubleBuffers() {
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("score");
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("score", ptype, doubleToBuffer(10.0), doubleToBuffer(15.0), PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertFalse("DOUBLE-encoded page failing Lt(0.5) must be excluded", result.overlaps(0, PAGE_SIZE));
    }

    public void testDecimalEncodedDoubleColumnIsKeptConservatively() {
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(2, 9))
            .named("price");
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("price", ptype, intToBuffer(1000), intToBuffer(500000), PAGE_SIZE);
        FilterPredicate pred = FilterApi.gt(FilterApi.doubleColumn("price"), 100.0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue("DECIMAL-encoded column must be kept conservatively for doubleColumn predicates", result.overlaps(0, PAGE_SIZE));
    }

    public void testFloat16EncodedDoubleColumnIsKeptConservatively() {
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(2)
            .as(LogicalTypeAnnotation.float16Type())
            .named("ratio");
        // Float16 little-endian per parquet-format spec (LSB first).
        ByteBuffer min = ByteBuffer.wrap(new byte[] { 0x00, 0x3C }); // 1.0
        ByteBuffer max = ByteBuffer.wrap(new byte[] { 0x00, 0x40 }); // 2.0
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("ratio", ptype, min, max, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("ratio"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue("Float16-encoded column must be kept conservatively for doubleColumn predicates", result.overlaps(0, PAGE_SIZE));
    }

    /**
     * Locks down the schema-driven decode path: a FLOAT schema must dictate that the first
     * 4 bytes of the stats buffer are read as a float, even when the buffer happens to be
     * 8 bytes wide. The legacy heuristic ({@code buf.remaining() == Float.BYTES}) would
     * have misread these bytes as a double.
     *
     * <p>The buffer encodes the double {@code 1.0} in little-endian
     * ({@code [00,00,00,00, 00,00,F0,3F]}); its first 4 bytes interpret as float {@code 0.0}.
     * A FLOAT-driven decode therefore yields min=max=0.0 and the {@code Lt(0.5)} predicate
     * keeps the page; a DOUBLE-driven decode would yield 1.0 and exclude it.
     */
    public void testSchemaDrivesFloatDecodeWhenBufferIsEightBytes() {
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.FLOAT).named("score");
        ByteBuffer wideMin = ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(1.0).flip();
        ByteBuffer wideMax = ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN).putDouble(1.0).flip();
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("score", ptype, wideMin, wideMax, PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.doubleColumn("score"), 0.5);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue(
            "FLOAT schema must drive the decode regardless of buffer width; first 4 bytes read as float 0.0 satisfy Lt(0.5)",
            result.overlaps(0, PAGE_SIZE)
        );
    }

    // -----------------------------------------------------------------------------------
    // esql-planning#1030: a Parquet uint32 column (physical INT32) whose true max exceeds
    // Integer.MAX_VALUE stores a raw int32 that reads as negative under Java's signed
    // Integer ordering, even though the ColumnIndex's own min/max were computed by the
    // writer using unsigned ordering. Comparisons here must agree with the writer's
    // unsigned ordering or this (deliberately conservative, "never fewer rows") visitor
    // would itself drop the matching page.
    // -----------------------------------------------------------------------------------

    public void testUnsignedInt32GtKeepsPageWithOverflowedMax() {
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false))
            .named("u32");
        // True unsigned range [50_000, 4_000_000_000]; raw int32 bits of 4_000_000_000 are negative.
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata(
            "u32",
            ptype,
            intToBuffer(50_000),
            intToBuffer((int) 4_000_000_000L),
            PAGE_SIZE
        );
        FilterPredicate pred = FilterApi.gt(FilterApi.intColumn("u32"), 100_000);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue(
            "unsigned max (4_000_000_000) is > 100_000; a signed read of its raw negative bits must not prune this page",
            result.overlaps(0, PAGE_SIZE)
        );
    }

    public void testUnsignedInt32LtExcludesPageEntirelyAboveThreshold() {
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false))
            .named("u32");
        // True unsigned range [3_000_000_000, 4_000_000_000], both with negative raw int32 bits.
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata(
            "u32",
            ptype,
            intToBuffer((int) 3_000_000_000L),
            intToBuffer((int) 4_000_000_000L),
            PAGE_SIZE
        );
        FilterPredicate pred = FilterApi.lt(FilterApi.intColumn("u32"), 100_000);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertFalse(
            "unsigned min (3_000_000_000) is not < 100_000; a signed read (negative min) must not wrongly keep this page",
            result.overlaps(0, PAGE_SIZE)
        );
    }

    public void testUnsignedInt32InKeepsPageMatchingOverflowedValue() {
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false))
            .named("u32");
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata(
            "u32",
            ptype,
            intToBuffer(50_000),
            intToBuffer((int) 4_000_000_000L),
            PAGE_SIZE
        );
        FilterPredicate pred = FilterApi.in(FilterApi.intColumn("u32"), Set.of((int) 4_000_000_000L));

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue("IN value 4_000_000_000 (narrowed) falls within the page's unsigned range", result.overlaps(0, PAGE_SIZE));
    }

    public void testSignedInt32StillUsesSignedComparisonNoRegression() {
        // No logical type annotation (plain signed INT32): a negative min must still be
        // ordered below a small positive value, exactly as natural Integer ordering dictates.
        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("delta");
        PreloadedRowGroupMetadata metadata = buildSinglePageMetadata("delta", ptype, intToBuffer(-1000), intToBuffer(-1), PAGE_SIZE);
        FilterPredicate pred = FilterApi.lt(FilterApi.intColumn("delta"), 0);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, PAGE_SIZE);

        assertTrue("plain signed INT32 must keep the signed-negative page for Lt(0)", result.overlaps(0, PAGE_SIZE));
    }

    public void testBooleanColumnEq() {
        PreloadedRowGroupMetadata metadata = buildBooleanMetadata("flag", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.eq(FilterApi.booleanColumn("flag"), true);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("At least some pages should match true", result.selectedRowCount() > 0);
    }

    // --- Metadata builders ---

    private static PreloadedRowGroupMetadata buildIntMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            int min = p * pageSize;
            int max = (p + 1) * pageSize - 1;
            minValues.add(intToBuffer(min));
            maxValues.add(intToBuffer(max));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 4, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.ASCENDING);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    /**
     * Builds metadata where page p has constant value pageValues[p] (i.e. min == max == pageValues[p]).
     * Useful for exercising NotEq pruning on low-cardinality / sorted-clustered data. Boundary order
     * is reported as UNORDERED because the per-page values are caller-supplied and may not be sorted.
     */
    private static PreloadedRowGroupMetadata buildConstantIntMetadata(String columnPath, int[] pageValues, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageValues.length; p++) {
            minValues.add(intToBuffer(pageValues[p]));
            maxValues.add(intToBuffer(pageValues[p]));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 4, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    /**
     * Builds metadata where page p has constant binary value pageValues[p] (i.e. min == max == pageValues[p]).
     */
    private static PreloadedRowGroupMetadata buildConstantBinaryMetadata(String columnPath, String[] pageValues, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageValues.length; p++) {
            Binary value = Binary.fromString(pageValues[p]);
            minValues.add(ByteBuffer.wrap(value.getBytes()));
            maxValues.add(ByteBuffer.wrap(value.getBytes()));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 10, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildLongMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            long min = (long) p * pageSize;
            long max = (long) (p + 1) * pageSize - 1;
            minValues.add(longToBuffer(min));
            maxValues.add(longToBuffer(max));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 8, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.ASCENDING);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildDoubleMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            double min = (double) p * pageSize;
            double max = (double) (p + 1) * pageSize - 1;
            minValues.add(doubleToBuffer(min));
            maxValues.add(doubleToBuffer(max));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 8, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.ASCENDING);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    /**
     * Builds a DOUBLE column index where page {@code p} has explicit {@code mins[p]} / {@code maxes[p]}
     * stats. Used to exercise edge cases (NaN, equal min==max, etc.) that the contiguous-range
     * helper {@link #buildDoubleMetadata} cannot express.
     */
    private static PreloadedRowGroupMetadata buildDoubleMetadataExplicit(String columnPath, double[] mins, double[] maxes, int pageSize) {
        assert mins.length == maxes.length : "mins/maxes length mismatch";
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < mins.length; p++) {
            minValues.add(doubleToBuffer(mins[p]));
            maxValues.add(doubleToBuffer(maxes[p]));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 8, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(columnPath);
        var typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        var typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    /**
     * Single-page metadata helper for the schema-driven decode tests. The caller supplies both
     * the {@link PrimitiveType} (driving the file MessageType captured on
     * {@link PreloadedRowGroupMetadata}) and the raw min/max stats bytes — necessary because
     * DECIMAL and Float16 columns intentionally produce buffer lengths (4 and 2 bytes) that the
     * legacy {@code remaining() == Float.BYTES} heuristic mishandles, and we want to drive the
     * exact buffer the runtime would observe rather than re-encode via a Java primitive.
     */
    private static PreloadedRowGroupMetadata buildSinglePageMetadata(
        String columnPath,
        PrimitiveType primitiveType,
        ByteBuffer min,
        ByteBuffer max,
        int pageSize
    ) {
        List<ByteBuffer> minValues = List.of(min);
        List<ByteBuffer> maxValues = List.of(max);
        List<Boolean> nullPages = List.of(Boolean.FALSE);
        List<PageLocation> pageLocations = List.of(new PageLocation(0L, pageSize * 4, 0));

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        var typedCi = ParquetMetadataConverter.fromParquetColumnIndex(primitiveType, ci);
        var typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, primitiveType, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildBinaryMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            Binary min = Binary.fromString("page_" + p);
            Binary max = Binary.fromString("page_" + p + "_z");
            minValues.add(ByteBuffer.wrap(min.getBytes()));
            maxValues.add(ByteBuffer.wrap(max.getBytes()));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize * 10, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.ASCENDING);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildBooleanMetadata(String columnPath, int pageCount, int pageSize) {
        List<ByteBuffer> minValues = new ArrayList<>();
        List<ByteBuffer> maxValues = new ArrayList<>();
        List<Boolean> nullPages = new ArrayList<>();
        List<PageLocation> pageLocations = new ArrayList<>();

        for (int p = 0; p < pageCount; p++) {
            boolean min = p % 2 == 0;
            boolean max = true;
            minValues.add(ByteBuffer.wrap(new byte[] { (byte) (min ? 1 : 0) }));
            maxValues.add(ByteBuffer.wrap(new byte[] { (byte) (max ? 1 : 0) }));
            nullPages.add(false);
            pageLocations.add(new PageLocation(p * 1000L, pageSize, p * pageSize));
        }

        ColumnIndex ci = new ColumnIndex(nullPages, minValues, maxValues, BoundaryOrder.UNORDERED);
        OffsetIndex oi = new OffsetIndex(pageLocations);

        PrimitiveType ptype = Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(columnPath);
        org.apache.parquet.internal.column.columnindex.ColumnIndex typedCi = ParquetMetadataConverter.fromParquetColumnIndex(ptype, ci);
        org.apache.parquet.internal.column.columnindex.OffsetIndex typedOi = ParquetMetadataConverter.fromParquetOffsetIndex(oi);

        return buildMetadata(columnPath, ptype, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildMetadata(
        String columnPath,
        PrimitiveType primitiveType,
        org.apache.parquet.internal.column.columnindex.ColumnIndex ci,
        org.apache.parquet.internal.column.columnindex.OffsetIndex oi
    ) {
        Map<String, org.apache.parquet.internal.column.columnindex.ColumnIndex> columnIndexes = new HashMap<>();
        Map<String, org.apache.parquet.internal.column.columnindex.OffsetIndex> offsetIndexes = new HashMap<>();
        columnIndexes.put("0:" + columnPath, ci);
        offsetIndexes.put("0:" + columnPath, oi);
        MessageType schema = new MessageType("test", primitiveType);
        return new PreloadedRowGroupMetadata(columnIndexes, offsetIndexes, schema);
    }

    private static ByteBuffer intToBuffer(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).flip();
    }

    private static ByteBuffer longToBuffer(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).flip();
    }

    private static ByteBuffer doubleToBuffer(double value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(value).flip();
    }
}
