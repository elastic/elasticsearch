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
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnBlockConversions;

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
 */
final class PageColumnReader {

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

    private ByteBuffer currentDefLevelBytes;
    private ByteBuffer currentValueBytes;
    private Encoding currentEncoding;
    private int currentPageValueCount;
    private int physicalPageValueCount;
    private int currentPageConsumed;
    private boolean currentPageIsV1;
    private long rowPositionInRowGroup;
    private boolean columnExhausted;
    private long batchStartRowPosition;

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
    }

    PageReader getPageReader() {
        return pageReader;
    }

    /**
     * Returns the number of logical row-group rows consumed by the last batch operation,
     * including rows in pages that were skipped by RowRanges. This allows the iterator to
     * correctly update {@code rowsRemainingInGroup} when page-level skipping is active.
     */
    long logicalRowsConsumed() {
        return rowPositionInRowGroup - batchStartRowPosition;
    }

    Block readBatch(int maxRows, BlockFactory blockFactory) {
        batchStartRowPosition = rowPositionInRowGroup;
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

    /**
     * Reads a batch and filters it using the survivor mask from late materialization. When no rows
     * survive, the column values are skipped without decode. When all rows survive, this is
     * equivalent to {@link #readBatch}. For partial survival, the full batch is decoded and then
     * compacted to contain only surviving rows.
     *
     * @param maxRows total rows in this batch (including eliminated rows)
     * @param blockFactory factory for building output Blocks
     * @param survivorMask boolean array where true = row survives; length must equal maxRows
     * @param survivorCount number of true entries in survivorMask (pre-computed for efficiency)
     * @return a Block containing only the surviving rows
     */
    Block readBatchFiltered(int maxRows, BlockFactory blockFactory, boolean[] survivorMask, int survivorCount) {
        batchStartRowPosition = rowPositionInRowGroup;
        if (survivorCount == maxRows) {
            return readBatch(maxRows, blockFactory);
        }
        if (survivorCount == 0) {
            skipRows(maxRows);
            return blockFactory.newConstantNullBlock(0);
        }
        Block fullBlock = readBatch(maxRows, blockFactory);
        try {
            return filterBlock(fullBlock, survivorMask, survivorCount, blockFactory);
        } finally {
            fullBlock.close();
        }
    }

    /**
     * Reads a batch where only rows in {@code selection} are materialized.
     * Non-selected values are consumed from the page stream but never stored.
     * The returned Block has exactly {@code selection.selectedCount()} positions.
     *
     * <p>This avoids the double-allocation pattern of {@link #readBatchFiltered} (which decodes
     * all rows into a full-sized array, then copies survivors into a smaller array). Instead,
     * a single right-sized array is allocated and populated directly.
     */
    Block readBatchWithSelection(int totalRows, BlockFactory blockFactory, RowSelection selection) {
        batchStartRowPosition = rowPositionInRowGroup;
        if (selection.isAllSelected()) {
            return readBatch(totalRows, blockFactory);
        }
        if (selection.isNoneSelected()) {
            skipRows(totalRows);
            return blockFactory.newConstantNullBlock(0);
        }
        loadDictionaryIfNeeded();
        return switch (info.esqlType()) {
            case BOOLEAN -> readBooleanBatchSelective(totalRows, blockFactory, selection);
            case INTEGER -> readIntBatchSelective(totalRows, blockFactory, selection);
            case LONG -> {
                if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                    yield readInt32AsLongBatchSelective(totalRows, blockFactory, selection);
                }
                yield readLongBatchSelective(totalRows, blockFactory, selection);
            }
            case DOUBLE -> readDoubleBatchSelective(totalRows, blockFactory, selection);
            case KEYWORD, TEXT -> readBytesBatchSelective(totalRows, blockFactory, selection);
            case DATETIME -> readDatetimeBatchSelective(totalRows, blockFactory, selection);
            default -> {
                skipRows(totalRows);
                yield blockFactory.newConstantNullBlock(selection.selectedCount());
            }
        };
    }

    static Block filterBlock(Block source, boolean[] mask, int survivorCount, BlockFactory blockFactory) {
        if (source instanceof LongBlock lb) {
            return filterLongBlock(lb, mask, survivorCount, blockFactory);
        } else if (source instanceof IntBlock ib) {
            return filterIntBlock(ib, mask, survivorCount, blockFactory);
        } else if (source instanceof DoubleBlock db) {
            return filterDoubleBlock(db, mask, survivorCount, blockFactory);
        } else if (source instanceof BytesRefBlock bb) {
            return filterBytesRefBlock(bb, mask, survivorCount, blockFactory);
        } else if (source instanceof BooleanBlock boolBlock) {
            return filterBooleanBlock(boolBlock, mask, survivorCount, blockFactory);
        }
        return blockFactory.newConstantNullBlock(survivorCount);
    }

    private static Block filterLongBlock(LongBlock source, boolean[] mask, int survivorCount, BlockFactory blockFactory) {
        long[] values = new long[survivorCount];
        BitSet nulls = null;
        int out = 0;
        for (int i = 0; i < mask.length; i++) {
            if (mask[i]) {
                if (source.isNull(i)) {
                    if (nulls == null) nulls = new BitSet(survivorCount);
                    nulls.set(out);
                } else {
                    values[out] = source.getLong(source.getFirstValueIndex(i));
                }
                out++;
            }
        }
        if (nulls == null) {
            return blockFactory.newLongArrayVector(values, survivorCount).asBlock();
        }
        return blockFactory.newLongArrayBlock(values, survivorCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private static Block filterIntBlock(IntBlock source, boolean[] mask, int survivorCount, BlockFactory blockFactory) {
        int[] values = new int[survivorCount];
        BitSet nulls = null;
        int out = 0;
        for (int i = 0; i < mask.length; i++) {
            if (mask[i]) {
                if (source.isNull(i)) {
                    if (nulls == null) nulls = new BitSet(survivorCount);
                    nulls.set(out);
                } else {
                    values[out] = source.getInt(source.getFirstValueIndex(i));
                }
                out++;
            }
        }
        if (nulls == null) {
            return blockFactory.newIntArrayVector(values, survivorCount).asBlock();
        }
        return blockFactory.newIntArrayBlock(values, survivorCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private static Block filterDoubleBlock(DoubleBlock source, boolean[] mask, int survivorCount, BlockFactory blockFactory) {
        double[] values = new double[survivorCount];
        BitSet nulls = null;
        int out = 0;
        for (int i = 0; i < mask.length; i++) {
            if (mask[i]) {
                if (source.isNull(i)) {
                    if (nulls == null) nulls = new BitSet(survivorCount);
                    nulls.set(out);
                } else {
                    values[out] = source.getDouble(source.getFirstValueIndex(i));
                }
                out++;
            }
        }
        if (nulls == null) {
            return blockFactory.newDoubleArrayVector(values, survivorCount).asBlock();
        }
        return blockFactory.newDoubleArrayBlock(values, survivorCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private static Block filterBytesRefBlock(BytesRefBlock source, boolean[] mask, int survivorCount, BlockFactory blockFactory) {
        try (var builder = blockFactory.newBytesRefBlockBuilder(survivorCount)) {
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < mask.length; i++) {
                if (mask[i]) {
                    if (source.isNull(i)) {
                        builder.appendNull();
                    } else {
                        builder.appendBytesRef(source.getBytesRef(source.getFirstValueIndex(i), scratch));
                    }
                }
            }
            return builder.build();
        }
    }

    private static Block filterBooleanBlock(BooleanBlock source, boolean[] mask, int survivorCount, BlockFactory blockFactory) {
        boolean[] values = new boolean[survivorCount];
        BitSet nulls = null;
        int out = 0;
        for (int i = 0; i < mask.length; i++) {
            if (mask[i]) {
                if (source.isNull(i)) {
                    if (nulls == null) nulls = new BitSet(survivorCount);
                    nulls.set(out);
                } else {
                    values[out] = source.getBoolean(source.getFirstValueIndex(i));
                }
                out++;
            }
        }
        if (nulls == null) {
            return blockFactory.newBooleanArrayVector(values, survivorCount).asBlock();
        }
        return blockFactory.newBooleanArrayBlock(values, survivorCount, null, nulls, Block.MvOrdering.UNORDERED);
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
            byte[] pageData = v1.getBytes().toByteArray();
            ByteBuffer pageBytes = ByteBuffer.wrap(pageData).order(ByteOrder.LITTLE_ENDIAN);

            if (maxDefLevel > 0) {
                int dlLength = pageBytes.getInt();
                byte[] dlData = new byte[Integer.BYTES + dlLength];
                pageBytes.position(pageBytes.position() - Integer.BYTES);
                pageBytes.get(dlData);
                currentDefLevelBytes = ByteBuffer.wrap(dlData).order(ByteOrder.LITTLE_ENDIAN);

                byte[] valueData = new byte[pageBytes.remaining()];
                pageBytes.get(valueData);
                currentValueBytes = ByteBuffer.wrap(valueData).order(ByteOrder.LITTLE_ENDIAN);
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
            defDecoder.init(ByteBuffer.allocate(0), currentPageValueCount, 0, false);
        }

        if (currentEncoding.usesDictionary()) {
            dictDecoder.init(currentValueBytes);
        } else {
            plainDecoder.init(currentValueBytes);
        }
    }

    private void applyIntraPageSkip() {
        if (rowRanges == null) {
            return;
        }
        long originalPageStart = rowPositionInRowGroup;
        long originalPageEnd = originalPageStart + currentPageValueCount;

        long firstSelected = rowRanges.firstSelectedInRange(originalPageStart, originalPageEnd);
        if (firstSelected < 0) {
            return;
        }

        int leadingSkip = (int) (firstSelected - originalPageStart);
        if (leadingSkip > 0) {
            int nonNullSkipped = defDecoder.skip(leadingSkip);
            skipValues(nonNullSkipped);
            currentPageConsumed += leadingSkip;
            rowPositionInRowGroup += leadingSkip;
        }

        long lastSelected = rowRanges.lastSelectedInRange(originalPageStart, originalPageEnd);
        if (lastSelected >= 0 && lastSelected < originalPageEnd - 1) {
            int trailingRows = (int) (originalPageEnd - 1 - lastSelected);
            currentPageValueCount -= trailingRows;
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
        batchStartRowPosition = rowPositionInRowGroup;
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

    // --- Boolean ---

    private Block readBooleanBatch(int maxRows, BlockFactory blockFactory) {
        boolean[] values = new boolean[maxRows];
        if (maxDefLevel == 0) {
            int produced = readNonNullBooleans(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantBoolean(values, produced, blockFactory);
            return constant != null ? constant : blockFactory.newBooleanArrayVector(values, produced).asBlock();
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : blockFactory.newBooleanArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return blockFactory.newBooleanArrayBlock(values, produced, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private int readNonNullBooleans(boolean[] values, int offset, int maxRows) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readBooleans(values, offset + produced, fromPage, dictionary);
            } else {
                plainDecoder.readBooleans(values, offset + produced, fromPage);
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readBooleanValues(boolean[] values, BitSet nulls, int offset, int totalRows, int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        if (nonNullCount == totalRows) {
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readBooleans(values, offset, nonNullCount, dictionary);
            } else {
                plainDecoder.readBooleans(values, offset, nonNullCount);
            }
            return;
        }
        boolean[] packed = new boolean[nonNullCount];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBooleans(packed, 0, nonNullCount, dictionary);
        } else {
            plainDecoder.readBooleans(packed, 0, nonNullCount);
        }
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Int ---

    private Block readIntBatch(int maxRows, BlockFactory blockFactory) {
        int[] values = new int[maxRows];
        if (maxDefLevel == 0) {
            int produced = readNonNullInts(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantInt(values, produced, blockFactory);
            return constant != null ? constant : blockFactory.newIntArrayVector(values, produced).asBlock();
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : blockFactory.newIntArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return blockFactory.newIntArrayBlock(values, produced, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private int readNonNullInts(int[] values, int offset, int maxRows) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readInts(values, offset + produced, fromPage, dictionary);
            } else {
                plainDecoder.readInts(values, offset + produced, fromPage);
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readIntValues(int[] values, BitSet nulls, int offset, int totalRows, int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        if (nonNullCount == totalRows) {
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readInts(values, offset, nonNullCount, dictionary);
            } else {
                plainDecoder.readInts(values, offset, nonNullCount);
            }
            return;
        }
        int[] packed = new int[nonNullCount];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readInts(packed, 0, nonNullCount, dictionary);
        } else {
            plainDecoder.readInts(packed, 0, nonNullCount);
        }
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Long ---

    private Block readLongBatch(int maxRows, BlockFactory blockFactory) {
        long[] values = new long[maxRows];
        if (maxDefLevel == 0) {
            int produced = readNonNullLongs(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, produced, true, false, null);
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, produced, true, false, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return ColumnBlockConversions.longColumn(blockFactory, values, produced, false, false, toBooleanArray(nulls, produced));
    }

    private int readNonNullLongs(long[] values, int offset, int maxRows) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readLongs(values, offset + produced, fromPage, dictionary);
            } else {
                plainDecoder.readLongs(values, offset + produced, fromPage);
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readLongValues(long[] values, BitSet nulls, int offset, int totalRows, int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        if (nonNullCount == totalRows) {
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readLongs(values, offset, nonNullCount, dictionary);
            } else {
                plainDecoder.readLongs(values, offset, nonNullCount);
            }
            return;
        }
        long[] packed = new long[nonNullCount];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readLongs(packed, 0, nonNullCount, dictionary);
        } else {
            plainDecoder.readLongs(packed, 0, nonNullCount);
        }
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Int32 widened to Long ---

    private Block readInt32AsLongBatch(int maxRows, BlockFactory blockFactory) {
        long[] values = new long[maxRows];
        if (maxDefLevel == 0) {
            int produced = readNonNullInt32AsLong(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, produced, true, false, null);
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, produced, true, false, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return ColumnBlockConversions.longColumn(blockFactory, values, produced, false, false, toBooleanArray(nulls, produced));
    }

    private int readNonNullInt32AsLong(long[] values, int offset, int maxRows) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int[] intValues = new int[fromPage];
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readInts(intValues, 0, fromPage, dictionary);
            } else {
                plainDecoder.readInts(intValues, 0, fromPage);
            }
            for (int i = 0; i < fromPage; i++) {
                values[offset + produced + i] = intValues[i];
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readInt32AsLongValues(long[] values, BitSet nulls, int offset, int totalRows, int nonNullCount) {
        if (nonNullCount == 0) {
            return;
        }
        int[] intPacked = new int[nonNullCount];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readInts(intPacked, 0, nonNullCount, dictionary);
        } else {
            plainDecoder.readInts(intPacked, 0, nonNullCount);
        }
        long[] longPacked = new long[nonNullCount];
        for (int i = 0; i < nonNullCount; i++) {
            longPacked[i] = intPacked[i];
        }
        if (nonNullCount == totalRows) {
            System.arraycopy(longPacked, 0, values, offset, nonNullCount);
        } else {
            scatter(longPacked, values, nulls, offset, totalRows);
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
        double[] values = new double[maxRows];
        if (maxDefLevel == 0) {
            int produced = readNonNullDoubles(values, 0, maxRows, isFloat);
            Block constant = ConstantBlockDetection.tryConstantDouble(values, produced, blockFactory);
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, produced, true, false, null);
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, produced, true, false, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return ColumnBlockConversions.doubleColumn(blockFactory, values, produced, false, false, toBooleanArray(nulls, produced));
    }

    private int readNonNullDoubles(double[] values, int offset, int maxRows, boolean isFloat) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            if (currentEncoding.usesDictionary()) {
                if (isFloat) {
                    dictDecoder.readFloats(values, offset + produced, fromPage, dictionary);
                } else {
                    dictDecoder.readDoubles(values, offset + produced, fromPage, dictionary);
                }
            } else {
                if (isFloat) {
                    plainDecoder.readFloats(values, offset + produced, fromPage);
                } else {
                    plainDecoder.readDoubles(values, offset + produced, fromPage);
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readDoubleValues(double[] values, BitSet nulls, int offset, int totalRows, int nonNullCount, boolean isFloat) {
        if (nonNullCount == 0) {
            return;
        }
        if (nonNullCount == totalRows) {
            if (currentEncoding.usesDictionary()) {
                if (isFloat) {
                    dictDecoder.readFloats(values, offset, nonNullCount, dictionary);
                } else {
                    dictDecoder.readDoubles(values, offset, nonNullCount, dictionary);
                }
            } else {
                if (isFloat) {
                    plainDecoder.readFloats(values, offset, nonNullCount);
                } else {
                    plainDecoder.readDoubles(values, offset, nonNullCount);
                }
            }
            return;
        }
        double[] packed = new double[nonNullCount];
        if (currentEncoding.usesDictionary()) {
            if (isFloat) {
                dictDecoder.readFloats(packed, 0, nonNullCount, dictionary);
            } else {
                dictDecoder.readDoubles(packed, 0, nonNullCount, dictionary);
            }
        } else {
            if (isFloat) {
                plainDecoder.readFloats(packed, 0, nonNullCount);
            } else {
                plainDecoder.readDoubles(packed, 0, nonNullCount);
            }
        }
        scatter(packed, values, nulls, offset, totalRows);
    }

    private Block readDecimalAsDoubleBatch(int maxRows, int scale, BlockFactory blockFactory) {
        double[] values = new double[maxRows];
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
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, produced, true, false, null);
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, produced, true, false, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return ColumnBlockConversions.doubleColumn(blockFactory, values, produced, false, false, toBooleanArray(nulls, produced));
    }

    private void readDecimalValues(double[] values, int offset, int count, int scale) {
        for (int i = 0; i < count; i++) {
            BigInteger unscaled = switch (info.parquetType()) {
                case INT32 -> {
                    int[] tmp = new int[1];
                    if (currentEncoding.usesDictionary()) {
                        dictDecoder.readInts(tmp, 0, 1, dictionary);
                    } else {
                        plainDecoder.readInts(tmp, 0, 1);
                    }
                    yield BigInteger.valueOf(tmp[0]);
                }
                case INT64 -> {
                    long[] tmp = new long[1];
                    if (currentEncoding.usesDictionary()) {
                        dictDecoder.readLongs(tmp, 0, 1, dictionary);
                    } else {
                        plainDecoder.readLongs(tmp, 0, 1);
                    }
                    yield BigInteger.valueOf(tmp[0]);
                }
                case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                    BytesRef[] tmp = new BytesRef[1];
                    if (currentEncoding.usesDictionary()) {
                        dictDecoder.readBinaries(tmp, 0, 1, dictionary);
                    } else if (info.parquetType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
                        plainDecoder.readFixedBinaries(tmp, 0, 1, descriptor.getPrimitiveType().getTypeLength());
                    } else {
                        plainDecoder.readBinaries(tmp, 0, 1);
                    }
                    yield new BigInteger(tmp[0].bytes, tmp[0].offset, tmp[0].length);
                }
                default -> throw new QlIllegalArgumentException("Unexpected DECIMAL backing type: " + info.parquetType());
            };
            values[offset + i] = new BigDecimal(unscaled, scale).doubleValue();
        }
    }

    private Block readFloat16Batch(int maxRows, BlockFactory blockFactory) {
        double[] values = new double[maxRows];
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
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, produced, true, false, null);
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : ColumnBlockConversions.doubleColumn(blockFactory, values, produced, true, false, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return ColumnBlockConversions.doubleColumn(blockFactory, values, produced, false, false, toBooleanArray(nulls, produced));
    }

    private void readFloat16Values(double[] values, int offset, int count) {
        BytesRef[] binaries = new BytesRef[count];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBinaries(binaries, 0, count, dictionary);
        } else {
            plainDecoder.readFixedBinaries(binaries, 0, count, 2);
        }
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
                    allValues[produced + i] = isUuid ? new BytesRef(OptimizedParquetColumnIterator.formatUuid(vals[i].bytes)) : vals[i];
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
        BitSet allNulls = new BitSet(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            BitSet pageNulls = new BitSet(fromPage);
            int nonNull = defDecoder.readBatch(fromPage, pageNulls, 0);
            BytesRef[] vals = nonNull > 0 ? readBinaryValues(nonNull) : null;
            int valIdx = 0;
            for (int i = 0; i < fromPage; i++) {
                if (pageNulls.get(i)) {
                    allNulls.set(produced + i);
                } else if (isUuid) {
                    allValues[produced + i] = new BytesRef(OptimizedParquetColumnIterator.formatUuid(vals[valIdx++].bytes));
                } else {
                    allValues[produced + i] = vals[valIdx++];
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (allNulls.isEmpty() == false) {
            Block allNull = ConstantBlockDetection.tryAllNull(allNulls, produced, blockFactory);
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
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBinaries(values, 0, count, dictionary);
        } else if (info.parquetType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
            plainDecoder.readFixedBinaries(values, 0, count, descriptor.getPrimitiveType().getTypeLength());
        } else {
            plainDecoder.readBinaries(values, 0, count);
        }
        return values;
    }

    // --- Datetime ---

    private Block readDatetimeBatch(int maxRows, BlockFactory blockFactory) {
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT96) {
            return readInt96Batch(maxRows, blockFactory);
        }
        boolean isDate = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
        long[] values = new long[maxRows];
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
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, produced, true, false, null);
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, produced, true, false, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return ColumnBlockConversions.longColumn(blockFactory, values, produced, false, false, toBooleanArray(nulls, produced));
    }

    private void readDatetimeValues(long[] values, int offset, int count, boolean isDate) {
        if (isDate) {
            int[] intValues = new int[count];
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readInts(intValues, 0, count, dictionary);
            } else {
                plainDecoder.readInts(intValues, 0, count);
            }
            for (int i = 0; i < count; i++) {
                values[offset + i] = intValues[i] * MILLIS_PER_DAY;
            }
        } else {
            if (currentEncoding.usesDictionary()) {
                dictDecoder.readLongs(values, offset, count, dictionary);
            } else {
                plainDecoder.readLongs(values, offset, count);
            }
            LogicalTypeAnnotation logicalType = info.logicalType();
            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
                switch (ts.getUnit()) {
                    case MILLIS -> {
                    } // no conversion needed
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
        long[] values = new long[maxRows];
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
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, produced, true, false, null);
        }
        BitSet nulls = new BitSet(maxRows);
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
            return constant != null ? constant : ColumnBlockConversions.longColumn(blockFactory, values, produced, true, false, null);
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls, produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        return ColumnBlockConversions.longColumn(blockFactory, values, produced, false, false, toBooleanArray(nulls, produced));
    }

    private void readInt96Values(long[] values, int offset, int count) {
        BytesRef[] binaries = new BytesRef[count];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBinaries(binaries, 0, count, dictionary);
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

    // --- Selective batch readers (filter-during-decode) ---

    private Block readBooleanBatchSelective(int totalRows, BlockFactory blockFactory, RowSelection selection) {
        int selectedCount = selection.selectedCount();
        boolean[] values = new boolean[selectedCount];
        if (maxDefLevel == 0) {
            readNonNullValuesSelective(totalRows, selection, (pageSel, fromPage, outOff) -> {
                if (currentEncoding.usesDictionary()) {
                    dictDecoder.readBooleansSelective(values, outOff, pageSel, fromPage, dictionary);
                } else {
                    plainDecoder.readBooleansSelective(values, outOff, pageSel, fromPage);
                }
                return pageSel.selectedCount();
            });
            return blockFactory.newBooleanArrayVector(values, selectedCount).asBlock();
        }
        BitSet nulls = new BitSet(selectedCount);
        readNullableValuesSelective(
            totalRows,
            selection,
            nulls,
            values,
            (vals, nonNullSel, totalNonNull, outOff, outNulls, pageSC) -> readAndScatterBooleans(
                (boolean[]) vals,
                outOff,
                outNulls,
                pageSC,
                nonNullSel,
                totalNonNull
            )
        );
        if (nulls.isEmpty()) {
            return blockFactory.newBooleanArrayVector(values, selectedCount).asBlock();
        }
        return blockFactory.newBooleanArrayBlock(values, selectedCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private void readAndScatterBooleans(boolean[] output, int outOff, BitSet nulls, int pageSelCount, RowSelection valSel, int totalNN) {
        boolean[] packed = new boolean[valSel.selectedCount()];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBooleansSelective(packed, 0, valSel, totalNN, dictionary);
        } else {
            plainDecoder.readBooleansSelective(packed, 0, valSel, totalNN);
        }
        scatterPacked(packed, output, outOff, nulls, pageSelCount);
    }

    private Block readIntBatchSelective(int totalRows, BlockFactory blockFactory, RowSelection selection) {
        int selectedCount = selection.selectedCount();
        int[] values = new int[selectedCount];
        if (maxDefLevel == 0) {
            readNonNullValuesSelective(totalRows, selection, (pageSel, fromPage, outOff) -> {
                if (currentEncoding.usesDictionary()) {
                    dictDecoder.readIntsSelective(values, outOff, pageSel, fromPage, dictionary);
                } else {
                    plainDecoder.readIntsSelective(values, outOff, pageSel, fromPage);
                }
                return pageSel.selectedCount();
            });
            return blockFactory.newIntArrayVector(values, selectedCount).asBlock();
        }
        BitSet nulls = new BitSet(selectedCount);
        readNullableValuesSelective(
            totalRows,
            selection,
            nulls,
            values,
            (vals, nonNullSel, totalNonNull, outOff, outNulls, pageSC) -> readAndScatterInts(
                (int[]) vals,
                outOff,
                outNulls,
                pageSC,
                nonNullSel,
                totalNonNull
            )
        );
        if (nulls.isEmpty()) {
            return blockFactory.newIntArrayVector(values, selectedCount).asBlock();
        }
        return blockFactory.newIntArrayBlock(values, selectedCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private void readAndScatterInts(int[] output, int outOff, BitSet nulls, int pageSelCount, RowSelection valSel, int totalNN) {
        int[] packed = new int[valSel.selectedCount()];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readIntsSelective(packed, 0, valSel, totalNN, dictionary);
        } else {
            plainDecoder.readIntsSelective(packed, 0, valSel, totalNN);
        }
        scatterPacked(packed, output, outOff, nulls, pageSelCount);
    }

    private Block readLongBatchSelective(int totalRows, BlockFactory blockFactory, RowSelection selection) {
        int selectedCount = selection.selectedCount();
        long[] values = new long[selectedCount];
        if (maxDefLevel == 0) {
            readNonNullValuesSelective(totalRows, selection, (pageSel, fromPage, outOff) -> {
                if (currentEncoding.usesDictionary()) {
                    dictDecoder.readLongsSelective(values, outOff, pageSel, fromPage, dictionary);
                } else {
                    plainDecoder.readLongsSelective(values, outOff, pageSel, fromPage);
                }
                return pageSel.selectedCount();
            });
            return blockFactory.newLongArrayVector(values, selectedCount).asBlock();
        }
        BitSet nulls = new BitSet(selectedCount);
        readNullableValuesSelective(
            totalRows,
            selection,
            nulls,
            values,
            (vals, nonNullSel, totalNonNull, outOff, outNulls, pageSC) -> readAndScatterLongs(
                (long[]) vals,
                outOff,
                outNulls,
                pageSC,
                nonNullSel,
                totalNonNull
            )
        );
        if (nulls.isEmpty()) {
            return blockFactory.newLongArrayVector(values, selectedCount).asBlock();
        }
        return blockFactory.newLongArrayBlock(values, selectedCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private void readAndScatterLongs(long[] output, int outOff, BitSet nulls, int pageSelCount, RowSelection valSel, int totalNN) {
        long[] packed = new long[valSel.selectedCount()];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readLongsSelective(packed, 0, valSel, totalNN, dictionary);
        } else {
            plainDecoder.readLongsSelective(packed, 0, valSel, totalNN);
        }
        scatterPacked(packed, output, outOff, nulls, pageSelCount);
    }

    private Block readInt32AsLongBatchSelective(int totalRows, BlockFactory blockFactory, RowSelection selection) {
        int selectedCount = selection.selectedCount();
        long[] values = new long[selectedCount];
        int[] intBuf = new int[selectedCount];
        if (maxDefLevel == 0) {
            readNonNullValuesSelective(totalRows, selection, (pageSel, fromPage, outOff) -> {
                int pageSelected = pageSel.selectedCount();
                if (currentEncoding.usesDictionary()) {
                    dictDecoder.readIntsSelective(intBuf, outOff, pageSel, fromPage, dictionary);
                } else {
                    plainDecoder.readIntsSelective(intBuf, outOff, pageSel, fromPage);
                }
                for (int i = 0; i < pageSelected; i++) {
                    values[outOff + i] = intBuf[outOff + i];
                }
                return pageSelected;
            });
            return blockFactory.newLongArrayVector(values, selectedCount).asBlock();
        }
        BitSet nulls = new BitSet(selectedCount);
        readNullableValuesSelective(
            totalRows,
            selection,
            nulls,
            intBuf,
            (vals, nonNullSel, totalNonNull, outOff, outNulls, pageSC) -> readAndScatterInts(
                (int[]) vals,
                outOff,
                outNulls,
                pageSC,
                nonNullSel,
                totalNonNull
            )
        );
        for (int i = 0; i < selectedCount; i++) {
            if (nulls.get(i) == false) {
                values[i] = intBuf[i];
            }
        }
        if (nulls.isEmpty()) {
            return blockFactory.newLongArrayVector(values, selectedCount).asBlock();
        }
        return blockFactory.newLongArrayBlock(values, selectedCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private Block readDoubleBatchSelective(int totalRows, BlockFactory blockFactory, RowSelection selection) {
        LogicalTypeAnnotation logical = info.logicalType();
        if (logical instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
            || logical instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
            return readBatchFiltered(totalRows, blockFactory, selection.toBooleanArray(), selection.selectedCount());
        }
        boolean isFloat = info.parquetType() == PrimitiveType.PrimitiveTypeName.FLOAT;
        int selectedCount = selection.selectedCount();
        double[] values = new double[selectedCount];
        if (maxDefLevel == 0) {
            readNonNullValuesSelective(totalRows, selection, (pageSel, fromPage, outOff) -> {
                if (currentEncoding.usesDictionary()) {
                    if (isFloat) {
                        dictDecoder.readFloatsSelective(values, outOff, pageSel, fromPage, dictionary);
                    } else {
                        dictDecoder.readDoublesSelective(values, outOff, pageSel, fromPage, dictionary);
                    }
                } else {
                    if (isFloat) {
                        plainDecoder.readFloatsSelective(values, outOff, pageSel, fromPage);
                    } else {
                        plainDecoder.readDoublesSelective(values, outOff, pageSel, fromPage);
                    }
                }
                return pageSel.selectedCount();
            });
            return blockFactory.newDoubleArrayVector(values, selectedCount).asBlock();
        }
        BitSet nulls = new BitSet(selectedCount);
        readNullableValuesSelective(
            totalRows,
            selection,
            nulls,
            values,
            (vals, nonNullSel, totalNonNull, outOff, outNulls, pageSC) -> readAndScatterDoubles(
                (double[]) vals,
                outOff,
                outNulls,
                pageSC,
                nonNullSel,
                totalNonNull,
                isFloat
            )
        );
        if (nulls.isEmpty()) {
            return blockFactory.newDoubleArrayVector(values, selectedCount).asBlock();
        }
        return blockFactory.newDoubleArrayBlock(values, selectedCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private void readAndScatterDoubles(
        double[] output,
        int outOff,
        BitSet nulls,
        int pageSelCount,
        RowSelection valSel,
        int totalNN,
        boolean isFloat
    ) {
        double[] packed = new double[valSel.selectedCount()];
        if (currentEncoding.usesDictionary()) {
            if (isFloat) {
                dictDecoder.readFloatsSelective(packed, 0, valSel, totalNN, dictionary);
            } else {
                dictDecoder.readDoublesSelective(packed, 0, valSel, totalNN, dictionary);
            }
        } else {
            if (isFloat) {
                plainDecoder.readFloatsSelective(packed, 0, valSel, totalNN);
            } else {
                plainDecoder.readDoublesSelective(packed, 0, valSel, totalNN);
            }
        }
        scatterPacked(packed, output, outOff, nulls, pageSelCount);
    }

    private Block readBytesBatchSelective(int totalRows, BlockFactory blockFactory, RowSelection selection) {
        int selectedCount = selection.selectedCount();
        boolean isUuid = info.logicalType() instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
        boolean isFixedLen = info.parquetType() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        BytesRef[] values = new BytesRef[selectedCount];
        if (maxDefLevel == 0) {
            readNonNullValuesSelective(totalRows, selection, (pageSel, fromPage, outOff) -> {
                int pageSelected = pageSel.selectedCount();
                if (currentEncoding.usesDictionary()) {
                    dictDecoder.readBinariesSelective(values, outOff, pageSel, fromPage, dictionary);
                } else if (isFixedLen) {
                    plainDecoder.readFixedBinariesSelective(
                        values,
                        outOff,
                        pageSel,
                        fromPage,
                        descriptor.getPrimitiveType().getTypeLength()
                    );
                } else {
                    plainDecoder.readBinariesSelective(values, outOff, pageSel, fromPage);
                }
                if (isUuid) {
                    for (int i = 0; i < pageSelected; i++) {
                        values[outOff + i] = new BytesRef(OptimizedParquetColumnIterator.formatUuid(values[outOff + i].bytes));
                    }
                }
                return pageSelected;
            });
        } else {
            BitSet nulls = new BitSet(selectedCount);
            readNullableValuesSelective(
                totalRows,
                selection,
                nulls,
                values,
                (vals, nonNullSel, totalNonNull, outOff, outNulls, pageSC) -> readAndScatterBytes(
                    (BytesRef[]) vals,
                    outOff,
                    outNulls,
                    pageSC,
                    nonNullSel,
                    totalNonNull,
                    isUuid,
                    isFixedLen
                )
            );
            if (nulls.isEmpty() == false) {
                try (var builder = blockFactory.newBytesRefBlockBuilder(selectedCount)) {
                    for (int i = 0; i < selectedCount; i++) {
                        if (nulls.get(i)) {
                            builder.appendNull();
                        } else {
                            builder.appendBytesRef(values[i]);
                        }
                    }
                    return builder.build();
                }
            }
        }
        try (var builder = blockFactory.newBytesRefBlockBuilder(selectedCount)) {
            for (int i = 0; i < selectedCount; i++) {
                builder.appendBytesRef(values[i]);
            }
            return builder.build();
        }
    }

    private void readAndScatterBytes(
        BytesRef[] output,
        int outOff,
        BitSet nulls,
        int pageSelCount,
        RowSelection valSel,
        int totalNN,
        boolean isUuid,
        boolean isFixedLen
    ) {
        BytesRef[] packed = new BytesRef[valSel.selectedCount()];
        if (currentEncoding.usesDictionary()) {
            dictDecoder.readBinariesSelective(packed, 0, valSel, totalNN, dictionary);
        } else if (isFixedLen) {
            plainDecoder.readFixedBinariesSelective(packed, 0, valSel, totalNN, descriptor.getPrimitiveType().getTypeLength());
        } else {
            plainDecoder.readBinariesSelective(packed, 0, valSel, totalNN);
        }
        if (isUuid) {
            for (int i = 0; i < packed.length; i++) {
                packed[i] = new BytesRef(OptimizedParquetColumnIterator.formatUuid(packed[i].bytes));
            }
        }
        scatterPacked(packed, output, outOff, nulls, pageSelCount);
    }

    private Block readDatetimeBatchSelective(int totalRows, BlockFactory blockFactory, RowSelection selection) {
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT96) {
            return readBatchFiltered(totalRows, blockFactory, selection.toBooleanArray(), selection.selectedCount());
        }
        boolean isDate = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
        int selectedCount = selection.selectedCount();
        long[] values = new long[selectedCount];
        if (maxDefLevel == 0) {
            if (isDate) {
                int[] intBuf = new int[selectedCount];
                readNonNullValuesSelective(totalRows, selection, (pageSel, fromPage, outOff) -> {
                    int pageSelected = pageSel.selectedCount();
                    if (currentEncoding.usesDictionary()) {
                        dictDecoder.readIntsSelective(intBuf, outOff, pageSel, fromPage, dictionary);
                    } else {
                        plainDecoder.readIntsSelective(intBuf, outOff, pageSel, fromPage);
                    }
                    return pageSelected;
                });
                for (int i = 0; i < selectedCount; i++) {
                    values[i] = intBuf[i] * MILLIS_PER_DAY;
                }
            } else {
                readNonNullValuesSelective(totalRows, selection, (pageSel, fromPage, outOff) -> {
                    if (currentEncoding.usesDictionary()) {
                        dictDecoder.readLongsSelective(values, outOff, pageSel, fromPage, dictionary);
                    } else {
                        plainDecoder.readLongsSelective(values, outOff, pageSel, fromPage);
                    }
                    return pageSel.selectedCount();
                });
                applyTimestampConversion(values, selectedCount);
            }
            return blockFactory.newLongArrayVector(values, selectedCount).asBlock();
        }
        BitSet nulls = new BitSet(selectedCount);
        if (isDate) {
            int[] intBuf = new int[selectedCount];
            readNullableValuesSelective(
                totalRows,
                selection,
                nulls,
                intBuf,
                (vals, nonNullSel, totalNonNull, outOff, outNulls, pageSC) -> readAndScatterInts(
                    (int[]) vals,
                    outOff,
                    outNulls,
                    pageSC,
                    nonNullSel,
                    totalNonNull
                )
            );
            for (int i = 0; i < selectedCount; i++) {
                if (nulls.get(i) == false) {
                    values[i] = intBuf[i] * MILLIS_PER_DAY;
                }
            }
        } else {
            readNullableValuesSelective(
                totalRows,
                selection,
                nulls,
                values,
                (vals, nonNullSel, totalNonNull, outOff, outNulls, pageSC) -> readAndScatterLongs(
                    (long[]) vals,
                    outOff,
                    outNulls,
                    pageSC,
                    nonNullSel,
                    totalNonNull
                )
            );
            applyTimestampConversion(values, selectedCount);
        }
        if (nulls.isEmpty()) {
            return blockFactory.newLongArrayVector(values, selectedCount).asBlock();
        }
        return blockFactory.newLongArrayBlock(values, selectedCount, null, nulls, Block.MvOrdering.UNORDERED);
    }

    private void applyTimestampConversion(long[] values, int count) {
        LogicalTypeAnnotation logicalType = info.logicalType();
        if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation ts) {
            switch (ts.getUnit()) {
                case MILLIS -> {
                }
                case MICROS -> {
                    for (int i = 0; i < count; i++) {
                        values[i] = values[i] / 1_000;
                    }
                }
                case NANOS -> {
                    for (int i = 0; i < count; i++) {
                        values[i] = values[i] / 1_000_000;
                    }
                }
            }
        }
    }

    /**
     * Drives the page loop for non-nullable selective reads. The callback is invoked once
     * per page with the page-local selection and output offset.
     */
    private void readNonNullValuesSelective(int totalRows, RowSelection selection, SelectivePageReader pageReader) {
        int remaining = totalRows;
        int consumed = 0;
        int outOffset = 0;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            RowSelection pageSel = selection.slice(consumed, fromPage);
            if (pageSel.isNoneSelected()) {
                skipValues(fromPage); // non-nullable: value count == row count
            } else {
                int produced = pageReader.read(pageSel, fromPage, outOffset);
                outOffset += produced;
            }
            advancePosition(fromPage);
            consumed += fromPage;
            remaining -= fromPage;
        }
    }

    @FunctionalInterface
    private interface SelectivePageReader {
        int read(RowSelection pageSel, int fromPage, int outOffset);
    }

    /**
     * Drives the page loop for nullable selective reads. Handles def levels, null tracking,
     * and value reading for selected rows only.
     *
     * <p>For nullable columns, def levels are decoded for ALL rows (to advance the RLE stream).
     * A value-level selection vector maps selected-and-non-null row positions to positions
     * in the non-null value stream, enabling the value decoder to skip non-selected values.
     * Selected non-null values are read packed, then scattered into the output array at the
     * correct non-null positions.
     */
    private void readNullableValuesSelective(
        int totalRows,
        RowSelection selection,
        BitSet nulls,
        Object values,
        NullableSelectivePageReader pageReader
    ) {
        int remaining = totalRows;
        int consumed = 0;
        int outOffset = 0;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            RowSelection pageSel = selection.slice(consumed, fromPage);
            int pageSelectedCount = pageSel.selectedCount();
            if (pageSel.isNoneSelected()) {
                int nonNullSkipped = defDecoder.skip(fromPage);
                skipValues(nonNullSkipped);
            } else {
                BitSet valueSel = new BitSet(fromPage);
                int[] metrics = defDecoder.readBatchSelectiveWithValueTracking(fromPage, nulls, outOffset, pageSel, valueSel);
                int totalNonNull = metrics[0];
                int selectedNonNull = metrics[1];

                if (totalNonNull == 0) {
                    // No values to read from the value stream
                } else if (selectedNonNull > 0) {
                    RowSelection nonNullValueSelection = new RowSelection(valueSel, totalNonNull, selectedNonNull);
                    pageReader.read(values, nonNullValueSelection, totalNonNull, outOffset, nulls, pageSelectedCount);
                } else {
                    skipValues(totalNonNull);
                }
                outOffset += pageSelectedCount;
            }
            advancePosition(fromPage);
            consumed += fromPage;
            remaining -= fromPage;
        }
    }

    @FunctionalInterface
    private interface NullableSelectivePageReader {
        /**
         * Read values selectively and scatter into output positions, skipping null positions.
         *
         * @param values output array (typed — caller casts)
         * @param nonNullSelection selection over the non-null value stream
         * @param totalNonNull total non-null values in this page chunk
         * @param outOffset starting output offset for this page's selected rows
         * @param nulls null bitmap for the output
         * @param pageSelectedCount number of selected rows in this page chunk
         */
        void read(Object values, RowSelection nonNullSelection, int totalNonNull, int outOffset, BitSet nulls, int pageSelectedCount);
    }

    // --- Scatter utilities ---

    private static void scatterPacked(boolean[] packed, boolean[] output, int outOff, BitSet nulls, int pageSelCount) {
        int pi = 0;
        for (int i = 0; i < pageSelCount; i++) {
            if (nulls.get(outOff + i) == false) {
                output[outOff + i] = packed[pi++];
            }
        }
    }

    private static void scatterPacked(int[] packed, int[] output, int outOff, BitSet nulls, int pageSelCount) {
        int pi = 0;
        for (int i = 0; i < pageSelCount; i++) {
            if (nulls.get(outOff + i) == false) {
                output[outOff + i] = packed[pi++];
            }
        }
    }

    private static void scatterPacked(long[] packed, long[] output, int outOff, BitSet nulls, int pageSelCount) {
        int pi = 0;
        for (int i = 0; i < pageSelCount; i++) {
            if (nulls.get(outOff + i) == false) {
                output[outOff + i] = packed[pi++];
            }
        }
    }

    private static void scatterPacked(double[] packed, double[] output, int outOff, BitSet nulls, int pageSelCount) {
        int pi = 0;
        for (int i = 0; i < pageSelCount; i++) {
            if (nulls.get(outOff + i) == false) {
                output[outOff + i] = packed[pi++];
            }
        }
    }

    private static void scatterPacked(BytesRef[] packed, BytesRef[] output, int outOff, BitSet nulls, int pageSelCount) {
        int pi = 0;
        for (int i = 0; i < pageSelCount; i++) {
            if (nulls.get(outOff + i) == false) {
                output[outOff + i] = packed[pi++];
            }
        }
    }

    private static void scatter(boolean[] packed, boolean[] output, BitSet nulls, int offset, int totalRows) {
        int pi = 0;
        for (int i = 0; i < totalRows; i++) {
            if (nulls.get(offset + i) == false) {
                output[offset + i] = packed[pi++];
            }
        }
    }

    private static void scatter(int[] packed, int[] output, BitSet nulls, int offset, int totalRows) {
        int pi = 0;
        for (int i = 0; i < totalRows; i++) {
            if (nulls.get(offset + i) == false) {
                output[offset + i] = packed[pi++];
            }
        }
    }

    private static void scatter(long[] packed, long[] output, BitSet nulls, int offset, int totalRows) {
        int pi = 0;
        for (int i = 0; i < totalRows; i++) {
            if (nulls.get(offset + i) == false) {
                output[offset + i] = packed[pi++];
            }
        }
    }

    private static void scatter(double[] packed, double[] output, BitSet nulls, int offset, int totalRows) {
        int pi = 0;
        for (int i = 0; i < totalRows; i++) {
            if (nulls.get(offset + i) == false) {
                output[offset + i] = packed[pi++];
            }
        }
    }

    private static boolean[] toBooleanArray(BitSet nulls, int length) {
        boolean[] result = new boolean[length];
        for (int i = nulls.nextSetBit(0); i >= 0; i = nulls.nextSetBit(i + 1)) {
            result[i] = true;
        }
        return result;
    }
}
