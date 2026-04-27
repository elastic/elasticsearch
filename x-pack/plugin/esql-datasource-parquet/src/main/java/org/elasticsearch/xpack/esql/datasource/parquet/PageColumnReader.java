/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.BitSet;

/**
 * Page-level batch column reader that bypasses {@code ColumnReadStoreImpl} and works directly
 * with {@link PageReader} and {@link DataPage} objects. For each batch request, it:
 * <ol>
 *   <li>Reads data pages from the {@link PageReader}</li>
 *   <li>Splits V1/V2 pages into def-level and value streams</li>
 *   <li>Bulk-decodes def levels via {@link DefinitionLevelDecoder} → null {@link BitSet}</li>
 *   <li>Bulk-decodes values via {@link PlainValueDecoder} or {@link DictionaryValueDecoder}</li>
 *   <li>Assembles into ESQL {@link Block}s</li>
 * </ol>
 *
 * <p>Only handles flat columns (maxRepLevel == 0). List columns must use the row-at-a-time path.
 *
 * <p>When {@link RowRanges} are provided, pages whose row span does not overlap the selected
 * ranges are skipped entirely in {@link #loadNextPage()}, enabling page-level filtering for
 * the optimized reader path.
 */
final class PageColumnReader {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();
    private static final long MILLIS_PER_DAY = Duration.ofDays(1).toMillis();
    private static final long NANOS_PER_MILLI = 1_000_000L;
    private static final int JULIAN_EPOCH_OFFSET = 2_440_588;

    private final PageReader pageReader;
    private final ColumnDescriptor descriptor;
    private final ColumnInfo info;
    private final RowRanges rowRanges;
    private final int maxDefLevel;

    private Dictionary dictionary;
    private boolean dictionaryLoaded;

    private final DefinitionLevelDecoder defDecoder = new DefinitionLevelDecoder();
    private final PlainValueDecoder plainDecoder = new PlainValueDecoder();
    private final DictionaryValueDecoder dictDecoder = new DictionaryValueDecoder();
    private ValuesReader fallbackReader;
    private boolean useFallbackReader;

    private final DecodeBuffers buffers;

    private ByteBuffer currentDefLevelBytes;
    private ByteBuffer currentValueBytes;
    private Encoding currentEncoding;
    private int currentPageValueCount;
    private int physicalPageValueCount;
    private int currentPageConsumed;
    private boolean currentPageIsV1;
    private long rowPositionInRowGroup;
    private boolean columnExhausted;

    PageColumnReader(PageReader pageReader, ColumnDescriptor descriptor, ColumnInfo info, RowRanges rowRanges) {
        this.pageReader = pageReader;
        this.descriptor = descriptor;
        this.info = info;
        this.rowRanges = rowRanges;
        this.maxDefLevel = descriptor.getMaxDefinitionLevel();
        this.columnExhausted = false;
        this.rowPositionInRowGroup = 0;
        this.currentPageConsumed = 0;
        this.currentPageValueCount = 0;
        this.physicalPageValueCount = 0;
        this.buffers = new DecodeBuffers();
    }

    Block readBatch(int maxRows, BlockFactory blockFactory) {
        loadDictionaryIfNeeded();
        return switch (info.esqlType()) {
            case BOOLEAN -> readBooleanBatch(maxRows, blockFactory);
            case INTEGER -> readIntBatch(maxRows, blockFactory);
            case LONG -> {
                if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                    yield readInt32AsLongBatch(maxRows, blockFactory);
                }
                yield readLongBatch(maxRows, blockFactory);
            }
            case DOUBLE -> readDoubleBatch(maxRows, blockFactory);
            case KEYWORD, TEXT -> readBytesBatch(maxRows, blockFactory);
            case DATETIME -> readDatetimeBatch(maxRows, blockFactory);
            default -> {
                skipRows(maxRows);
                yield blockFactory.newConstantNullBlock(maxRows);
            }
        };
    }

    private void loadDictionaryIfNeeded() {
        if (dictionaryLoaded) {
            return;
        }
        dictionaryLoaded = true;
        DictionaryPage dictPage = pageReader.readDictionaryPage();
        if (dictPage != null) {
            dictionary = dictPage.decode(descriptor);
        }
    }

    private boolean ensurePage() {
        if (columnExhausted) {
            return false;
        }
        if (currentPageConsumed < currentPageValueCount) {
            return true;
        }
        return loadNextPage();
    }

    private boolean loadNextPage() {
        if (physicalPageValueCount > 0 && currentPageConsumed < physicalPageValueCount) {
            int remainder = physicalPageValueCount - currentPageConsumed;
            if (remainder > 0) {
                int nonNullSkipped = defDecoder.skip(remainder);
                skipValues(nonNullSkipped);
                rowPositionInRowGroup += remainder;
                currentPageConsumed = physicalPageValueCount;
            }
        }
        while (true) {
            DataPage page = pageReader.readPage();
            if (page == null) {
                columnExhausted = true;
                physicalPageValueCount = 0;
                return false;
            }

            page.getFirstRowIndex().ifPresent(firstRow -> {
                if (firstRow > rowPositionInRowGroup) {
                    rowPositionInRowGroup = firstRow;
                }
            });

            int pageRowCount = page.getValueCount();

            if (rowRanges != null && rowRanges.overlaps(rowPositionInRowGroup, rowPositionInRowGroup + pageRowCount) == false) {
                rowPositionInRowGroup += pageRowCount;
                continue;
            }

            currentPageValueCount = pageRowCount;
            physicalPageValueCount = pageRowCount;
            currentPageConsumed = 0;

            if (page instanceof DataPageV1 v1) {
                currentPageIsV1 = true;
                currentEncoding = v1.getValueEncoding();
                splitV1Page(v1);
            } else if (page instanceof DataPageV2 v2) {
                currentPageIsV1 = false;
                currentEncoding = v2.getDataEncoding();
                splitV2Page(v2);
            } else {
                throw new QlIllegalArgumentException("Unexpected page type: " + page.getClass().getName());
            }

            initDecoders();
            return true;
        }
    }

    private void splitV1Page(DataPageV1 v1) {
        try {
            ByteBuffer pageBytes = v1.getBytes().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);

            if (maxDefLevel > 0) {
                int dlStart = pageBytes.position();
                int dlLength = pageBytes.getInt();
                currentDefLevelBytes = pageBytes.slice(dlStart, Integer.BYTES + dlLength).order(ByteOrder.LITTLE_ENDIAN);

                int valueStart = dlStart + Integer.BYTES + dlLength;
                int valueLen = pageBytes.limit() - valueStart;
                currentValueBytes = pageBytes.slice(valueStart, valueLen).order(ByteOrder.LITTLE_ENDIAN);
            } else {
                currentDefLevelBytes = null;
                currentValueBytes = pageBytes;
            }
        } catch (IOException e) {
            throw new QlIllegalArgumentException("Failed to read V1 page bytes: " + e.getMessage(), e);
        }
    }

    private void splitV2Page(DataPageV2 v2) {
        try {
            if (maxDefLevel > 0) {
                BytesInput dlInput = v2.getDefinitionLevels();
                currentDefLevelBytes = dlInput.toByteBuffer();
            } else {
                currentDefLevelBytes = null;
            }
            currentValueBytes = v2.getData().toByteBuffer();
        } catch (IOException e) {
            throw new QlIllegalArgumentException("Failed to read V2 page bytes: " + e.getMessage(), e);
        }
    }

    private void initDecoders() {
        if (maxDefLevel > 0 && currentDefLevelBytes != null) {
            defDecoder.init(currentDefLevelBytes, currentPageValueCount, maxDefLevel, currentPageIsV1);
        } else {
            defDecoder.init(EMPTY_BYTE_BUFFER.duplicate(), currentPageValueCount, 0, false);
        }

        if (currentEncoding.usesDictionary()) {
            useFallbackReader = false;
            dictDecoder.init(currentValueBytes);
        } else if (currentEncoding == Encoding.PLAIN || currentEncoding == Encoding.BIT_PACKED) {
            useFallbackReader = false;
            plainDecoder.init(currentValueBytes);
        } else {
            useFallbackReader = true;
            try {
                fallbackReader = currentEncoding.getValuesReader(descriptor, ValuesType.VALUES);
                byte[] bytes = new byte[currentValueBytes.remaining()];
                currentValueBytes.duplicate().get(bytes);
                fallbackReader.initFromPage(currentPageValueCount, bytes, 0);
            } catch (IOException e) {
                throw new QlIllegalArgumentException(
                    "Failed to init fallback decoder for encoding " + currentEncoding + ": " + e.getMessage(),
                    e
                );
            }
        }
    }

    private int availableInPage() {
        return currentPageValueCount - currentPageConsumed;
    }

    private void advancePosition(int count) {
        currentPageConsumed += count;
        rowPositionInRowGroup += count;
    }

    void skipRows(int count) {
        int remaining = count;
        while (remaining > 0) {
            if (ensurePage() == false) {
                break;
            }
            int fromPage = Math.min(remaining, availableInPage());
            int nonNullSkipped = defDecoder.skip(fromPage);
            skipValues(nonNullSkipped);
            advancePosition(fromPage);
            remaining -= fromPage;
        }
    }

    private void skipValues(int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        if (currentEncoding.usesDictionary()) {
            dictDecoder.skip(nonNullCount);
        } else if (useFallbackReader) {
            fallbackReader.skip(nonNullCount);
        } else {
            skipPlainValues(nonNullCount);
        }
    }

    private void skipPlainValues(int count) {
        switch (info.parquetType()) {
            case INT32 -> plainDecoder.skipInts(count);
            case INT64 -> plainDecoder.skipLongs(count);
            case FLOAT -> plainDecoder.skipFloats(count);
            case DOUBLE -> plainDecoder.skipDoubles(count);
            case BOOLEAN -> plainDecoder.skipBooleans(count);
            case BINARY -> plainDecoder.skipBinaries(count);
            case FIXED_LEN_BYTE_ARRAY -> plainDecoder.skipFixedBinaries(count, descriptor.getPrimitiveType().getTypeLength());
            case INT96 -> plainDecoder.skipFixedBinaries(count, 12);
            default -> throw new QlIllegalArgumentException("Unsupported Parquet type for skip: " + info.parquetType());
        }
    }

    private void readIntsDispatch(int[] values, int offset, int count) {
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readInts(values, offset, count, dictionary);
        } else if (useFallbackReader) {
            for (int i = 0; i < count; i++) {
                values[offset + i] = fallbackReader.readInteger();
            }
        } else {
            plainDecoder.readInts(values, offset, count);
        }
    }

    private void readLongsDispatch(long[] values, int offset, int count) {
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readLongs(values, offset, count, dictionary);
        } else if (useFallbackReader) {
            for (int i = 0; i < count; i++) {
                values[offset + i] = fallbackReader.readLong();
            }
        } else {
            plainDecoder.readLongs(values, offset, count);
        }
    }

    private void readFloatsDispatch(double[] values, int offset, int count) {
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readFloats(values, offset, count, dictionary);
        } else if (useFallbackReader) {
            for (int i = 0; i < count; i++) {
                values[offset + i] = fallbackReader.readFloat();
            }
        } else {
            plainDecoder.readFloats(values, offset, count);
        }
    }

    private void readDoublesDispatch(double[] values, int offset, int count) {
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readDoubles(values, offset, count, dictionary);
        } else if (useFallbackReader) {
            for (int i = 0; i < count; i++) {
                values[offset + i] = fallbackReader.readDouble();
            }
        } else {
            plainDecoder.readDoubles(values, offset, count);
        }
    }

    private void readBooleansDispatch(boolean[] values, int offset, int count) {
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBooleans(values, offset, count, dictionary);
        } else if (useFallbackReader) {
            for (int i = 0; i < count; i++) {
                values[offset + i] = fallbackReader.readBoolean();
            }
        } else {
            plainDecoder.readBooleans(values, offset, count);
        }
    }

    private void readBinariesDispatch(BytesRef[] values, int offset, int count) {
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBinaries(values, offset, count, dictionary);
        } else if (useFallbackReader) {
            for (int i = 0; i < count; i++) {
                org.apache.parquet.io.api.Binary bin = fallbackReader.readBytes();
                byte[] bytes = bin.getBytes();
                values[offset + i] = new BytesRef(bytes, 0, bytes.length);
            }
        } else if (descriptor.getPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            plainDecoder.readFixedBinaries(values, offset, count, descriptor.getPrimitiveType().getTypeLength());
        } else {
            plainDecoder.readBinaries(values, offset, count);
        }
    }

    // --- Boolean ---

    private Block readBooleanBatch(int maxRows, BlockFactory blockFactory) {
        boolean[] values = buffers.booleans(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullBooleans(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantBoolean(values, produced, blockFactory);
            if (constant != null) return constant;
            return blockFactory.newBooleanArrayVector(values, produced).asBlock();
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            readBooleanValues(values, nulls, produced, fromPage, nonNull);
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantBoolean(values, produced, blockFactory);
            if (constant != null) return constant;
            return blockFactory.newBooleanArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return blockFactory.newBooleanArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
    }

    private int readNonNullBooleans(boolean[] values, int offset, int maxRows) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            readBooleansDispatch(values, offset + produced, fromPage);
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readBooleanValues(boolean[] values, WordMask nulls, int offset, int totalRows, int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        if (nonNullCount == totalRows) {
            readBooleansDispatch(values, offset, nonNullCount);
            return;
        }
        boolean[] packed = new boolean[nonNullCount];
        readBooleansDispatch(packed, 0, nonNullCount);
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Int ---

    private Block readIntBatch(int maxRows, BlockFactory blockFactory) {
        int[] values = buffers.ints(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullInts(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantInt(values, produced, blockFactory);
            if (constant != null) return constant;
            return blockFactory.newIntArrayVector(values, produced).asBlock();
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            readIntValues(values, nulls, produced, fromPage, nonNull);
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantInt(values, produced, blockFactory);
            if (constant != null) return constant;
            return blockFactory.newIntArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return blockFactory.newIntArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
    }

    private int readNonNullInts(int[] values, int offset, int maxRows) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            readIntsDispatch(values, offset + produced, fromPage);
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readIntValues(int[] values, WordMask nulls, int offset, int totalRows, int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        if (nonNullCount == totalRows) {
            readIntsDispatch(values, offset, nonNullCount);
            return;
        }
        int[] packed = new int[nonNullCount];
        readIntsDispatch(packed, 0, nonNullCount);
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Long ---

    private Block readLongBatch(int maxRows, BlockFactory blockFactory) {
        long[] values = buffers.longs(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullLongs(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeLongBlock(blockFactory, produced, true, null);
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            readLongValues(values, nulls, produced, fromPage, nonNull);
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeLongBlock(blockFactory, produced, true, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return takeLongBlock(blockFactory, produced, false, nulls.toBitSet());
    }

    private int readNonNullLongs(long[] values, int offset, int maxRows) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readLongs(values, offset + produced, fromPage, dictionary);
            } else if (useFallbackReader) {
                for (int i = 0; i < fromPage; i++) {
                    values[offset + produced + i] = fallbackReader.readLong();
                }
            } else {
                plainDecoder.readLongs(values, offset + produced, fromPage);
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readLongValues(long[] values, WordMask nulls, int offset, int totalRows, int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        if (nonNullCount == totalRows) {
            readLongsDispatch(values, offset, nonNullCount);
            return;
        }
        long[] packed = new long[nonNullCount];
        readLongsDispatch(packed, 0, nonNullCount);
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Int32 widened to Long ---

    private Block readInt32AsLongBatch(int maxRows, BlockFactory blockFactory) {
        long[] values = buffers.longs(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullInt32AsLong(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeLongBlock(blockFactory, produced, true, null);
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            readInt32AsLongValues(values, nulls, produced, fromPage, nonNull);
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeLongBlock(blockFactory, produced, true, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return takeLongBlock(blockFactory, produced, false, nulls.toBitSet());
    }

    private int readNonNullInt32AsLong(long[] values, int offset, int maxRows) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int[] intValues = buffers.ints(fromPage);
            readIntsDispatch(intValues, 0, fromPage);
            for (int i = 0; i < fromPage; i++) {
                values[offset + produced + i] = intValues[i];
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readInt32AsLongValues(long[] values, WordMask nulls, int offset, int totalRows, int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        int[] intPacked = buffers.ints(nonNullCount);
        readIntsDispatch(intPacked, 0, nonNullCount);
        if (nonNullCount == totalRows) {
            for (int i = 0; i < nonNullCount; i++) {
                values[offset + i] = intPacked[i];
            }
        } else {
            int pi = 0;
            for (int i = 0; i < totalRows; i++) {
                if (nulls.get(offset + i) == false) {
                    values[offset + i] = intPacked[pi++];
                }
            }
        }
    }

    // --- Double ---

    private Block readDoubleBatch(int maxRows, BlockFactory blockFactory) {
        LogicalTypeAnnotation logical = info.logicalType();
        if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimal) {
            return readDecimalAsDoubleBatch(maxRows, decimal.getScale(), blockFactory);
        }
        if (logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
            return readFloat16Batch(maxRows, blockFactory);
        }
        boolean isFloat = info.parquetType() == PrimitiveType.PrimitiveTypeName.FLOAT;
        double[] values = buffers.doubles(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullDoubles(values, 0, maxRows, isFloat);
            Block constant = ConstantBlockDetection.tryConstantDouble(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeDoubleBlock(blockFactory, produced, true, null);
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            readDoubleValues(values, nulls, produced, fromPage, nonNull, isFloat);
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantDouble(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeDoubleBlock(blockFactory, produced, true, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return takeDoubleBlock(blockFactory, produced, false, nulls.toBitSet());
    }

    private int readNonNullDoubles(double[] values, int offset, int maxRows, boolean isFloat) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            if (isFloat) {
                readFloatsDispatch(values, offset + produced, fromPage);
            } else {
                readDoublesDispatch(values, offset + produced, fromPage);
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readDoubleValues(double[] values, WordMask nulls, int offset, int totalRows, int nonNullCount, boolean isFloat) {
        if (nonNullCount == 0) {
            return;
        }
        if (nonNullCount == totalRows) {
            if (isFloat) {
                readFloatsDispatch(values, offset, nonNullCount);
            } else {
                readDoublesDispatch(values, offset, nonNullCount);
            }
            return;
        }
        double[] packed = new double[nonNullCount];
        if (isFloat) {
            readFloatsDispatch(packed, 0, nonNullCount);
        } else {
            readDoublesDispatch(packed, 0, nonNullCount);
        }
        scatter(packed, values, nulls, offset, totalRows);
    }

    private Block readDecimalAsDoubleBatch(int maxRows, int scale, BlockFactory blockFactory) {
        double[] values = buffers.doubles(maxRows);
        if (maxDefLevel == 0) {
            int produced = 0;
            int remaining = maxRows;
            while (remaining > 0 && ensurePage()) {
                int fromPage = Math.min(remaining, availableInPage());
                readDecimalValues(values, produced, fromPage, scale);
                advancePosition(fromPage);
                produced += fromPage;
                remaining -= fromPage;
            }
            Block constant = ConstantBlockDetection.tryConstantDouble(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeDoubleBlock(blockFactory, produced, true, null);
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            if (nonNull > 0) {
                if (nonNull == fromPage) {
                    readDecimalValues(values, produced, nonNull, scale);
                } else {
                    double[] packed = new double[nonNull];
                    readDecimalValues(packed, 0, nonNull, scale);
                    scatter(packed, values, nulls, produced, fromPage);
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantDouble(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeDoubleBlock(blockFactory, produced, true, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return takeDoubleBlock(blockFactory, produced, false, nulls.toBitSet());
    }

    private void readDecimalValues(double[] values, int offset, int count, int scale) {
        int[] intScratch = (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) ? new int[1] : null;
        long[] longScratch = (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT64) ? new long[1] : null;
        BytesRef[] binScratch = (intScratch == null && longScratch == null) ? new BytesRef[1] : null;
        for (int i = 0; i < count; i++) {
            BigInteger unscaled = switch (info.parquetType()) {
                case INT32 -> {
                    readIntsDispatch(intScratch, 0, 1);
                    yield BigInteger.valueOf(intScratch[0]);
                }
                case INT64 -> {
                    readLongsDispatch(longScratch, 0, 1);
                    yield BigInteger.valueOf(longScratch[0]);
                }
                case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                    readBinariesDispatch(binScratch, 0, 1);
                    yield new BigInteger(binScratch[0].bytes, binScratch[0].offset, binScratch[0].length);
                }
                default -> throw new QlIllegalArgumentException("Unexpected DECIMAL backing type: " + info.parquetType());
            };
            values[offset + i] = new BigDecimal(unscaled, scale).doubleValue();
        }
    }

    private Block readFloat16Batch(int maxRows, BlockFactory blockFactory) {
        double[] values = buffers.doubles(maxRows);
        if (maxDefLevel == 0) {
            int produced = 0;
            int remaining = maxRows;
            while (remaining > 0 && ensurePage()) {
                int fromPage = Math.min(remaining, availableInPage());
                readFloat16Values(values, produced, fromPage);
                advancePosition(fromPage);
                produced += fromPage;
                remaining -= fromPage;
            }
            Block constant = ConstantBlockDetection.tryConstantDouble(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeDoubleBlock(blockFactory, produced, true, null);
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            if (nonNull > 0) {
                if (nonNull == fromPage) {
                    readFloat16Values(values, produced, nonNull);
                } else {
                    double[] packed = new double[nonNull];
                    readFloat16Values(packed, 0, nonNull);
                    scatter(packed, values, nulls, produced, fromPage);
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantDouble(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeDoubleBlock(blockFactory, produced, true, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return takeDoubleBlock(blockFactory, produced, false, nulls.toBitSet());
    }

    private void readFloat16Values(double[] values, int offset, int count) {
        BytesRef[] binaries = new BytesRef[count];
        readBinariesDispatch(binaries, 0, count);
        for (int i = 0; i < count; i++) {
            byte[] bytes = binaries[i].bytes;
            int off = binaries[i].offset;
            short float16Bits = (short) ((bytes[off + 1] & 0xFF) << 8 | (bytes[off] & 0xFF));
            values[offset + i] = Float.float16ToFloat(float16Bits);
        }
    }

    // --- BytesRef (KEYWORD/TEXT) ---

    private Block readBytesBatch(int maxRows, BlockFactory blockFactory) {
        boolean isUuid = info.logicalType() instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
        if (maxDefLevel == 0) {
            BytesRef[] allValues = new BytesRef[maxRows];
            int produced = 0;
            int remaining = maxRows;
            while (remaining > 0 && ensurePage()) {
                int fromPage = Math.min(remaining, availableInPage());
                BytesRef[] vals = readBinaryValues(fromPage);
                for (int i = 0; i < fromPage; i++) {
                    allValues[produced + i] = isUuid ? new BytesRef(ParquetColumnDecoding.formatUuid(vals[i].bytes)) : vals[i];
                }
                advancePosition(fromPage);
                produced += fromPage;
                remaining -= fromPage;
            }
            Block constant = ConstantBlockDetection.tryConstantBytesRef(allValues, produced, blockFactory);
            if (constant != null) {
                return constant;
            }
            try (var builder = blockFactory.newBytesRefBlockBuilder(produced)) {
                for (int i = 0; i < produced; i++) {
                    builder.appendBytesRef(allValues[i]);
                }
                return builder.build();
            }
        }
        BytesRef[] allValues = new BytesRef[maxRows];
        WordMask allNulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            WordMask pageNulls = buffers.valueSelection(fromPage);
            int nonNull = defDecoder.readBatch(fromPage, pageNulls, 0);
            BytesRef[] vals = nonNull > 0 ? readBinaryValues(nonNull) : null;
            int valIdx = 0;
            for (int i = 0; i < fromPage; i++) {
                if (pageNulls.get(i)) {
                    allNulls.set(produced + i);
                } else if (isUuid) {
                    allValues[produced + i] = new BytesRef(ParquetColumnDecoding.formatUuid(vals[valIdx++].bytes));
                } else {
                    allValues[produced + i] = vals[valIdx++];
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (allNulls.isEmpty() == false) {
            Block allNull = ConstantBlockDetection.tryAllNull(allNulls.toBitSet(), produced, blockFactory);
            if (allNull != null) {
                return allNull;
            }
        }
        try (var builder = blockFactory.newBytesRefBlockBuilder(produced)) {
            for (int i = 0; i < produced; i++) {
                if (allNulls.get(i)) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(allValues[i]);
                }
            }
            return builder.build();
        }
    }

    private BytesRef[] readBinaryValues(int count) {
        BytesRef[] values = new BytesRef[count];
        readBinariesDispatch(values, 0, count);
        return values;
    }

    // --- Datetime ---

    private Block readDatetimeBatch(int maxRows, BlockFactory blockFactory) {
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT96) {
            return readInt96Batch(maxRows, blockFactory);
        }
        boolean isDate = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
        long[] values = buffers.longs(maxRows);
        if (maxDefLevel == 0) {
            int produced = 0;
            int remaining = maxRows;
            while (remaining > 0 && ensurePage()) {
                int fromPage = Math.min(remaining, availableInPage());
                readDatetimeValues(values, produced, fromPage, isDate);
                advancePosition(fromPage);
                produced += fromPage;
                remaining -= fromPage;
            }
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeLongBlock(blockFactory, produced, true, null);
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            if (nonNull > 0) {
                if (nonNull == fromPage) {
                    readDatetimeValues(values, produced, nonNull, isDate);
                } else {
                    long[] packed = new long[nonNull];
                    readDatetimeValues(packed, 0, nonNull, isDate);
                    scatter(packed, values, nulls, produced, fromPage);
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeLongBlock(blockFactory, produced, true, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return takeLongBlock(blockFactory, produced, false, nulls.toBitSet());
    }

    private void readDatetimeValues(long[] values, int offset, int count, boolean isDate) {
        if (isDate) {
            int[] intValues = buffers.ints(count);
            readIntsDispatch(intValues, 0, count);
            for (int i = 0; i < count; i++) {
                values[offset + i] = intValues[i] * MILLIS_PER_DAY;
            }
        } else {
            readLongsDispatch(values, offset, count);
            LogicalTypeAnnotation logicalType = info.logicalType();
            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
                switch (ts.getUnit()) {
                    case MILLIS -> {
                    }
                    case MICROS -> {
                        for (int i = 0; i < count; i++) {
                            values[offset + i] = values[offset + i] / 1_000;
                        }
                    }
                    case NANOS -> {
                        for (int i = 0; i < count; i++) {
                            values[offset + i] = values[offset + i] / 1_000_000;
                        }
                    }
                }
            }
        }
    }

    private Block readInt96Batch(int maxRows, BlockFactory blockFactory) {
        long[] values = buffers.longs(maxRows);
        if (maxDefLevel == 0) {
            int produced = 0;
            int remaining = maxRows;
            while (remaining > 0 && ensurePage()) {
                int fromPage = Math.min(remaining, availableInPage());
                readInt96Values(values, produced, fromPage);
                advancePosition(fromPage);
                produced += fromPage;
                remaining -= fromPage;
            }
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeLongBlock(blockFactory, produced, true, null);
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            if (nonNull > 0) {
                if (nonNull == fromPage) {
                    readInt96Values(values, produced, nonNull);
                } else {
                    long[] packed = new long[nonNull];
                    readInt96Values(packed, 0, nonNull);
                    scatter(packed, values, nulls, produced, fromPage);
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            return takeLongBlock(blockFactory, produced, true, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return takeLongBlock(blockFactory, produced, false, nulls.toBitSet());
    }

    private void readInt96Values(long[] values, int offset, int count) {
        BytesRef[] binaries = new BytesRef[count];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBinaries(binaries, 0, count, dictionary);
        } else if (useFallbackReader) {
            for (int i = 0; i < count; i++) {
                org.apache.parquet.io.api.Binary bin = fallbackReader.readBytes();
                byte[] bytes = bin.getBytes();
                binaries[i] = new BytesRef(bytes, 0, bytes.length);
            }
        } else {
            plainDecoder.readFixedBinaries(binaries, 0, count, 12);
        }
        for (int i = 0; i < count; i++) {
            ByteBuffer buf = ByteBuffer.wrap(binaries[i].bytes, binaries[i].offset, binaries[i].length).order(ByteOrder.LITTLE_ENDIAN);
            long nanosOfDay = buf.getLong();
            int julianDay = buf.getInt();
            long epochDay = julianDay - JULIAN_EPOCH_OFFSET;
            values[offset + i] = epochDay * MILLIS_PER_DAY + nanosOfDay / NANOS_PER_MILLI;
        }
    }

    // --- Block construction helpers ---

    private Block takeLongBlock(BlockFactory blockFactory, int rowCount, boolean noNulls, BitSet nullBitSet) {
        long[] owned = buffers.takeLongs();
        if (noNulls) {
            return blockFactory.newLongArrayVector(owned, rowCount).asBlock();
        }
        return blockFactory.newLongArrayBlock(owned, rowCount, null, nullBitSet, Block.MvOrdering.UNORDERED);
    }

    private Block takeDoubleBlock(BlockFactory blockFactory, int rowCount, boolean noNulls, BitSet nullBitSet) {
        double[] owned = buffers.takeDoubles();
        if (noNulls) {
            return blockFactory.newDoubleArrayVector(owned, rowCount).asBlock();
        }
        return blockFactory.newDoubleArrayBlock(owned, rowCount, null, nullBitSet, Block.MvOrdering.UNORDERED);
    }

    // --- Scatter utilities ---

    private static void scatter(boolean[] packed, boolean[] output, WordMask nulls, int offset, int totalRows) {
        int pi = 0;
        for (int i = 0; i < totalRows; i++) {
            if (nulls.get(offset + i) == false) {
                output[offset + i] = packed[pi++];
            }
        }
    }

    private static void scatter(int[] packed, int[] output, WordMask nulls, int offset, int totalRows) {
        int pi = 0;
        for (int i = 0; i < totalRows; i++) {
            if (nulls.get(offset + i) == false) {
                output[offset + i] = packed[pi++];
            }
        }
    }

    private static void scatter(long[] packed, long[] output, WordMask nulls, int offset, int totalRows) {
        int pi = 0;
        for (int i = 0; i < totalRows; i++) {
            if (nulls.get(offset + i) == false) {
                output[offset + i] = packed[pi++];
            }
        }
    }

    private static void scatter(double[] packed, double[] output, WordMask nulls, int offset, int totalRows) {
        int pi = 0;
        for (int i = 0; i < totalRows; i++) {
            if (nulls.get(offset + i) == false) {
                output[offset + i] = packed[pi++];
            }
        }
    }
}
