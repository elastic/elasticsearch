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

    public void testNotEqReturnsAll() {
        PreloadedRowGroupMetadata metadata = buildIntMetadata("id", PAGE_COUNT, PAGE_SIZE);
        FilterPredicate pred = FilterApi.notEq(FilterApi.intColumn("id"), 500);

        RowRanges result = ColumnIndexRowRangesComputer.compute(pred, metadata, 0, ROW_GROUP_ROW_COUNT);

        assertTrue("NotEq should conservatively return all rows", result.isAll());
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

        return buildMetadata(columnPath, typedCi, typedOi);
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

        return buildMetadata(columnPath, typedCi, typedOi);
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

        return buildMetadata(columnPath, typedCi, typedOi);
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

        return buildMetadata(columnPath, typedCi, typedOi);
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

        return buildMetadata(columnPath, typedCi, typedOi);
    }

    private static PreloadedRowGroupMetadata buildMetadata(
        String columnPath,
        org.apache.parquet.internal.column.columnindex.ColumnIndex ci,
        org.apache.parquet.internal.column.columnindex.OffsetIndex oi
    ) {
        Map<String, org.apache.parquet.internal.column.columnindex.ColumnIndex> columnIndexes = new HashMap<>();
        Map<String, org.apache.parquet.internal.column.columnindex.OffsetIndex> offsetIndexes = new HashMap<>();
        columnIndexes.put("0:" + columnPath, ci);
        offsetIndexes.put("0:" + columnPath, oi);
        return new PreloadedRowGroupMetadata(columnIndexes, offsetIndexes);
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
