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
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.compute.data.Utf8Sanitizer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.DeclaredTypeCoercions;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Page-level batch column reader that bypasses {@code ColumnReadStoreImpl} and works directly
 * with {@link PageReader} and {@link DataPage} objects. For each batch request, it:
 * <ol>
 *   <li>Reads data pages from the {@link PageReader}</li>
 *   <li>Splits V1/V2 pages into def-level and value streams</li>
 *   <li>Bulk-decodes def levels via {@link DefinitionLevelDecoder} → null {@link WordMask}</li>
 *   <li>Bulk-decodes values via {@link PlainValueDecoder} or {@link DictionaryValueDecoder}</li>
 *   <li>Assembles into ESQL {@link Block}s</li>
 * </ol>
 *
 * <p>Only handles flat columns (maxRepLevel == 0). List columns must use the row-at-a-time path.
 *
 * <p>When {@link RowRanges} are provided, pages whose row span does not overlap the selected
 * ranges are skipped entirely in {@link #loadNextPage()}. This is a safety net for the
 * sequential builder path (when prefetch is unavailable and the builder cannot pre-filter
 * pages) and a no-op for the filtered builder path.
 *
 * <p>For dictionary-encoded BINARY/UTF8 columns, {@code readBytesBatch} takes an ordinal
 * fast path that emits an {@link OrdinalBytesRefBlock} so downstream {@code BlockHash} can
 * hash only the {@code k} dictionary entries instead of the {@code N} row values. The
 * dictionary bytes are deep-copied into a {@link BytesRefArray} once per chunk and that
 * array is shared across every batch in the chunk via its atomic ref-count
 * ({@link BytesRefArray} extends {@code AbstractRefCounted}). Each emitted batch creates a
 * fresh {@code BytesRefArrayVector} wrapper that holds one ref to the shared array; the
 * wrapper itself is single-driver-owned (its non-thread-safe ref counter is never shared)
 * and decRefs the shared array when the block is closed. This decoupling lets pages be
 * buffered and consumed asynchronously by a different driver thread (see
 * {@code AsyncExternalSourceOperatorFactory} which calls
 * {@code page.allowPassingToDifferentDriver()}) without racing the producer's per-batch
 * ref-count updates against the consumer's close. UUID-typed columns skip the ordinal path
 * because their values need per-row byte formatting that would have to be applied to
 * dictionary entries instead.
 *
 * <p>The cached {@link BytesRefArray} is released when this reader's chunk changes
 * (currently never — chunk is fixed for the reader's lifetime, so this is defensive) or
 * when the reader is {@link #close() closed}. Closing is driven by
 * {@code OptimizedParquetColumnIterator}, which closes any previous readers when it
 * advances row groups (in {@code initColumnReaders}) and when the iterator itself is
 * closed.
 *
 * <p>If a non-dictionary page is encountered mid-batch (rare in practice — Parquet writers
 * keep encoding consistent across all data pages of a column chunk), the partial ordinal
 * batch is resolved through the dictionary and the remainder is read via the materialized
 * binary path.
 */
final class PageColumnReader implements Releasable {

    private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private final PageReader pageReader;
    private final ColumnDescriptor descriptor;
    private final ColumnInfo info;
    private final RowRanges rowRanges;
    /** Per-value declared-coercion failure sink ({@code null} = strict); see the 5-arg constructor. */
    @Nullable
    private final SkipWarnings coercionWarnings;
    private final int maxDefLevel;

    private Dictionary dictionary;
    private boolean dictionaryLoaded;

    /**
     * Cached deep-copy of the current dictionary's entries as a ref-counted {@link BytesRefArray}.
     * Built lazily on the first ordinal batch of the chunk and reused (via {@code incRef}) for every
     * subsequent batch. A defensive {@link #cachedDictArraySource} reference guards against the
     * (currently impossible) case of the underlying {@link Dictionary} changing within a single
     * reader's lifetime — if it does, the old cache is released and rebuilt.
     */
    private BytesRefArray cachedDictArray;
    private Dictionary cachedDictArraySource;

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
        this(pageReader, descriptor, info, rowRanges, null);
    }

    /**
     * @param coercionWarnings sink for per-value declared-coercion failures (nulled cell +
     *                         response Warning header), shared across the read so the warning cap
     *                         is per read. {@code null} = strict: a coercion failure propagates.
     */
    PageColumnReader(
        PageReader pageReader,
        ColumnDescriptor descriptor,
        ColumnInfo info,
        RowRanges rowRanges,
        @Nullable SkipWarnings coercionWarnings
    ) {
        this.pageReader = pageReader;
        this.descriptor = descriptor;
        this.info = info;
        this.rowRanges = rowRanges;
        this.coercionWarnings = coercionWarnings;
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
        // Declared-type coercion beyond the fused pairs: decode the column at the file's own type
        // with the arms below, then coerce the block to the declared type. Per-value failures
        // follow the read's error policy: a live coercionWarnings sink nulls the cell + emits a
        // response Warning, a null sink (fail_fast) fails the read.
        DataType declared = info.esqlType();
        DataType fileType = info.fileEsqlType();
        if (fileType != null
            && declared != fileType
            && DeclaredTypeCoercions.fusedInDecode(fileType, declared) == false
            && DeclaredTypeCoercions.supports(fileType, declared)) {
            Block physical = readBatchAs(fileType, maxRows, blockFactory);
            try {
                return DeclaredTypeCoercions.castBlock(
                    physical,
                    fileType,
                    declared,
                    info.dateFormatter(),
                    blockFactory,
                    String.join(".", descriptor.getPath()),
                    coercionWarnings
                );
            } finally {
                physical.close();
            }
        }
        return readBatchAs(declared, maxRows, blockFactory);
    }

    private Block readBatchAs(DataType type, int maxRows, BlockFactory blockFactory) {
        // WARNING: the dispatching logic below is duplicated in ParquetFormatReader#readColumnBlock
        // KEEP IN SYNC!
        return switch (type) {
            case BOOLEAN -> readBooleanBatch(maxRows, blockFactory);
            case INTEGER -> readIntBatch(maxRows, blockFactory);
            case LONG, UNSIGNED_LONG -> {
                if (info.logicalType() instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation time) {
                    if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                        // TIME_MILLIS: physical INT32, widen to long (raw ms value, no unit conversion)
                        yield readInt32AsLongBatch(maxRows, blockFactory, true);
                    }
                    yield readLongBatch(maxRows, blockFactory, ParquetColumnDecoding.timeNanoMultiplier(time), false);
                }
                if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32) {
                    var logicalType = (LogicalTypeAnnotation.IntLogicalTypeAnnotation) info.logicalType();
                    // A plain INT32 with no logical-type annotation is historically "signed"
                    yield readInt32AsLongBatch(maxRows, blockFactory, logicalType == null || logicalType.isSigned());
                }
                // A 64-bit unsigned column maps to UNSIGNED_LONG, which ESQL stores sign-flip-encoded
                // (value ^ 2^63) so signed-long ordering matches unsigned ordering. The output edge always
                // decodes UNSIGNED_LONG blocks, so the read path must emit the encoded form here.
                yield readLongBatch(maxRows, blockFactory, 1L, type == DataType.UNSIGNED_LONG);
            }
            case DOUBLE -> readDoubleBatch(maxRows, blockFactory);
            case KEYWORD, TEXT -> readBytesBatch(maxRows, blockFactory);
            case DATETIME -> readDatetimeBatch(maxRows, blockFactory);
            case DATE_NANOS -> readDateNanosBatch(maxRows, blockFactory);
            default -> {
                skipRows(maxRows);
                yield blockFactory.newConstantNullBlock(maxRows);
            }
        };
    }

    /**
     * Filters a block to retain only the positions specified by {@code positions}.
     * Takes ownership of {@code source}: the source block is closed after filtering
     * and the caller owns the returned block.
     *
     * @param source        the block to filter; ownership is transferred to this method
     * @param positions     the positions to retain (ascending, no duplicates)
     * @param survivorCount the number of valid entries in {@code positions}
     * @param blockFactory  the factory used to create replacement blocks
     * @return a new block containing only the selected positions
     */
    static Block filterBlock(Block source, int[] positions, int survivorCount, BlockFactory blockFactory) {
        if (survivorCount == source.getPositionCount()) {
            return source;
        }
        if (survivorCount == 0) {
            source.close();
            return blockFactory.newConstantNullBlock(0);
        }
        if (positions.length != survivorCount) {
            positions = Arrays.copyOf(positions, survivorCount);
        }
        Block filtered = source.filter(false, positions);
        source.close();
        return filtered;
    }

    /**
     * Reads a batch and filters it to retain only the surviving positions.
     * When all rows survive, delegates directly to {@link #readBatch}. When none
     * survive, skips the rows and returns an empty constant null block.
     *
     * @param maxRows           the total number of rows in the batch
     * @param blockFactory      the factory used to create blocks
     * @param survivorPositions the positions that survived filtering (ascending, no duplicates)
     * @param survivorCount     the number of valid entries in {@code survivorPositions}
     * @return a block containing only the surviving rows
     */
    Block readBatchFiltered(int maxRows, BlockFactory blockFactory, int[] survivorPositions, int survivorCount) {
        if (survivorCount == maxRows) {
            return readBatch(maxRows, blockFactory);
        }
        if (survivorCount == 0) {
            skipRows(maxRows);
            return blockFactory.newConstantNullBlock(0);
        }
        Block full = readBatch(maxRows, blockFactory);
        try {
            // filterBlock closes `full` on success; on the `source.filter(...)` allocation
            // failure it does NOT, so we explicitly release the source here to avoid leaking
            // the full-batch block (and its breaker reservation) back up to the caller, which
            // never saw the reference.
            return filterBlock(full, survivorPositions, survivorCount, blockFactory);
        } catch (RuntimeException e) {
            Releasables.closeExpectNoException(full);
            throw e;
        }
    }

    /**
     * Reads only the rows at the given positions within a batch of {@code sourceRows} total
     * rows, interleaving {@link #skipRows} and {@link #readBatch} calls. Produces exactly
     * {@code survivorCount} output rows. Unlike {@link #readBatchFiltered}, this method is
     * safe when the reader has {@link RowRanges} that cause page skipping in
     * {@link #loadNextPage()}, because every skip/read call advances the reader's cursor
     * in the same sequential order as a plain read — no post-read coordinate translation
     * is needed.
     *
     * @param sourceRows        total rows in the source batch (defines the coordinate space)
     * @param blockFactory      factory for block construction
     * @param survivorPositions ascending positions of rows to read (in {@code [0, sourceRows)})
     * @param survivorCount     number of valid entries in {@code survivorPositions}
     * @return a block with exactly {@code survivorCount} positions
     */
    Block readBatchSparse(int sourceRows, BlockFactory blockFactory, int[] survivorPositions, int survivorCount) {
        assert survivorCount <= sourceRows : "survivorCount [" + survivorCount + "] > sourceRows [" + sourceRows + "]";
        assert survivorCount <= survivorPositions.length
            : "survivorCount [" + survivorCount + "] > array length [" + survivorPositions.length + "]";
        assert survivorCount == 0 || survivorPositions[survivorCount - 1] < sourceRows
            : "survivor position ["
                + (survivorCount > 0 ? survivorPositions[survivorCount - 1] : -1)
                + "] >= sourceRows ["
                + sourceRows
                + "]";

        if (survivorCount == 0) {
            skipRows(sourceRows);
            return blockFactory.newConstantNullBlock(0);
        }
        if (survivorCount == sourceRows) {
            return readBatch(sourceRows, blockFactory);
        }

        // Fast path: all survivors are contiguous — single skip/read/skip, no chunk list.
        if (survivorPositions[survivorCount - 1] - survivorPositions[0] == survivorCount - 1) {
            int leadingGap = survivorPositions[0];
            if (leadingGap > 0) {
                skipRows(leadingGap);
            }
            Block result = readBatch(survivorCount, blockFactory);
            int trailing = sourceRows - survivorPositions[survivorCount - 1] - 1;
            if (trailing > 0) {
                skipRows(trailing);
            }
            return result;
        }

        // General path: decompose survivorPositions into (skip, read) runs of contiguous
        // positions. Each run produces a small Block via readBatch; chunks are combined
        // at the end.
        List<Block> chunks = new ArrayList<>();
        int cursor = 0;
        int runStart = 0;

        try {
            while (runStart < survivorCount) {
                int pos = survivorPositions[runStart];
                int gap = pos - cursor;
                if (gap > 0) {
                    skipRows(gap);
                    cursor += gap;
                }

                // Find the end of the contiguous run
                int runEnd = runStart + 1;
                while (runEnd < survivorCount && survivorPositions[runEnd] == survivorPositions[runEnd - 1] + 1) {
                    runEnd++;
                }
                int runLength = runEnd - runStart;

                chunks.add(readBatch(runLength, blockFactory));
                cursor += runLength;
                runStart = runEnd;
            }

            // Skip any trailing rows after the last survivor
            int trailing = sourceRows - cursor;
            if (trailing > 0) {
                skipRows(trailing);
            }

            if (chunks.size() == 1) {
                return chunks.get(0);
            }
            // Exact-count guard: the sparse loop must have decoded exactly survivorCount rows across
            // the chunks. This caught a real page-skip miscount and stays local to this path because
            // the gather path in ParquetColumnExtractor has no such fixed 1:1 relationship.
            assert sumPositions(chunks) == survivorCount : "chunk total " + sumPositions(chunks) + " != expected " + survivorCount;
            // BlockChunks.concat resolves the element type from the first non-NULL chunk (so a
            // null-leading run cannot poison a ConstantNullBlock builder) and closes the chunks on
            // success; on a throw the catch below releases them.
            return BlockChunks.concat(chunks, blockFactory);
        } catch (RuntimeException e) {
            for (Block chunk : chunks) {
                Releasables.closeExpectNoException(chunk);
            }
            throw e;
        }
    }

    private static int sumPositions(List<Block> chunks) {
        int total = 0;
        for (Block b : chunks) {
            total += b.getPositionCount();
        }
        return total;
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
                throw new IllegalArgumentException("Unexpected page type: " + page.getClass().getName());
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
            throw new IllegalArgumentException("Failed to read V1 page bytes: " + e.getMessage(), e);
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
            throw new IllegalArgumentException("Failed to read V2 page bytes: " + e.getMessage(), e);
        }
    }

    private void initDecoders() {
        if (maxDefLevel > 0 && currentDefLevelBytes != null) {
            defDecoder.init(currentDefLevelBytes, maxDefLevel, currentPageIsV1);
        } else {
            defDecoder.init(EMPTY_BYTE_BUFFER.duplicate(), 0, false);
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
                byte[] bytes = UninitializedArrays.newByteArray(currentValueBytes.remaining());
                currentValueBytes.duplicate().get(bytes);
                fallbackReader.initFromPage(currentPageValueCount, bytes, 0);
            } catch (IOException e) {
                throw new IllegalArgumentException(
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
        long target = rowPositionInRowGroup + count;
        while (rowPositionInRowGroup < target) {
            if (ensurePage() == false) {
                break;
            }
            if (rowPositionInRowGroup >= target) {
                // ensurePage advanced past target via a firstRowIndex jump in loadNextPage
                // (page-filtered prefetch with a survivor-range gap wider than `count`).
                // Falling through would compute a negative fromPage and corrupt decoder /
                // page-consumed state.
                break;
            }
            int fromPage = (int) Math.min(target - rowPositionInRowGroup, availableInPage());
            int nonNullSkipped = defDecoder.skip(fromPage);
            skipValues(nonNullSkipped);
            advancePosition(fromPage);
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
            default -> throw new IllegalArgumentException("Unsupported Parquet type for skip: " + info.parquetType());
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
        boolean[] values = UninitializedArrays.newBooleanArray(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullBooleans(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantBoolean(values, produced, blockFactory);
            if (constant != null) return constant;
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newBooleanArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
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
        boolean[] packed = UninitializedArrays.newBooleanArray(nonNullCount);
        readBooleansDispatch(packed, 0, nonNullCount);
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Int ---

    private Block readIntBatch(int maxRows, BlockFactory blockFactory) {
        int[] values = UninitializedArrays.newIntArray(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullInts(values, 0, maxRows);
            Block constant = ConstantBlockDetection.tryConstantInt(values, produced, blockFactory);
            if (constant != null) return constant;
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newIntArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
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
        int[] packed = UninitializedArrays.newIntArray(nonNullCount);
        readIntsDispatch(packed, 0, nonNullCount);
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Long ---

    /**
     * Reads a Parquet INT64 column into a {@code LONG}/{@code UNSIGNED_LONG} block.
     *
     * @param multiplier     factor applied to each value, used to normalize Parquet TIME_* units to nanoseconds
     *                       ({@code 1L} when no conversion is needed).
     * @param encodeUnsigned when {@code true} the column is an {@code unsigned_long}; the decoded values are sign-flip-encoded
     *                       ({@code value ^ 2^63}) in place before constant detection and block creation, mirroring the indexing
     *                       path so the always-decoding output edge produces the true unsigned value. Encoding before constant
     *                       detection keeps a constant {@code unsigned_long} column correctly encoded. Mutually exclusive with a
     *                       non-unit {@code multiplier} (a column is either a TIME type or {@code unsigned_long}).
     */
    private Block readLongBatch(int maxRows, BlockFactory blockFactory, long multiplier, boolean encodeUnsigned) {
        long[] values = UninitializedArrays.newLongArray(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullLongs(values, 0, maxRows);
            if (encodeUnsigned) {
                ParquetColumnDecoding.encodeUnsignedLongInPlace(values, produced);
            } else if (multiplier != 1) {
                for (int i = 0; i < produced; i++) {
                    values[i] *= multiplier;
                }
            }
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
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
        if (encodeUnsigned) {
            ParquetColumnDecoding.encodeUnsignedLongInPlace(values, produced);
        } else if (multiplier != 1) {
            for (int i = 0; i < produced; i++) {
                values[i] *= multiplier;
            }
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
        return blockFactory.newLongArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
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
        long[] packed = UninitializedArrays.newLongArray(nonNullCount);
        readLongsDispatch(packed, 0, nonNullCount);
        scatter(packed, values, nulls, offset, totalRows);
    }

    // --- Int32 widened to Long ---

    private Block readInt32AsLongBatch(int maxRows, BlockFactory blockFactory, boolean signed) {
        long[] values = UninitializedArrays.newLongArray(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullInt32AsLong(values, 0, maxRows, signed);
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            readInt32AsLongValues(values, nulls, produced, fromPage, nonNull, signed);
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
        return blockFactory.newLongArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
    }

    private int readNonNullInt32AsLong(long[] values, int offset, int maxRows, boolean signed) {
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int[] intValues = buffers.ints(fromPage);
            readIntsDispatch(intValues, 0, fromPage);
            for (int i = 0; i < fromPage; i++) {
                values[offset + produced + i] = signed ? intValues[i] : Integer.toUnsignedLong(intValues[i]);
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        return produced;
    }

    private void readInt32AsLongValues(long[] values, WordMask nulls, int offset, int totalRows, int nonNullCount, boolean signed) {
        if (nonNullCount == 0) {
            return;
        }
        int[] intPacked = buffers.ints(nonNullCount);
        readIntsDispatch(intPacked, 0, nonNullCount);
        if (nonNullCount == totalRows) {
            for (int i = 0; i < nonNullCount; i++) {
                values[offset + i] = signed ? intPacked[i] : Integer.toUnsignedLong(intPacked[i]);
            }
        } else {
            int pi = 0;
            for (int i = 0; i < totalRows; i++) {
                if (nulls.get(offset + i) == false) {
                    int packed = intPacked[pi++];
                    values[offset + i] = signed ? packed : Integer.toUnsignedLong(packed);
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
        double[] values = UninitializedArrays.newDoubleArray(maxRows);
        if (maxDefLevel == 0) {
            int produced = readNonNullDoubles(values, 0, maxRows, isFloat);
            Block constant = ConstantBlockDetection.tryConstantDouble(values, produced, blockFactory);
            if (constant != null) return constant;
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newDoubleArrayVector(values, produced).asBlock();
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newDoubleArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
        return blockFactory.newDoubleArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
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
        double[] packed = UninitializedArrays.newDoubleArray(nonNullCount);
        if (isFloat) {
            readFloatsDispatch(packed, 0, nonNullCount);
        } else {
            readDoublesDispatch(packed, 0, nonNullCount);
        }
        scatter(packed, values, nulls, offset, totalRows);
    }

    private Block readDecimalAsDoubleBatch(int maxRows, int scale, BlockFactory blockFactory) {
        double[] values = UninitializedArrays.newDoubleArray(maxRows);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newDoubleArrayVector(values, produced).asBlock();
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
                    double[] packed = UninitializedArrays.newDoubleArray(nonNull);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newDoubleArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
        return blockFactory.newDoubleArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
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
                default -> throw new IllegalArgumentException("Unexpected DECIMAL backing type: " + info.parquetType());
            };
            values[offset + i] = new BigDecimal(unscaled, scale).doubleValue();
        }
    }

    private Block readFloat16Batch(int maxRows, BlockFactory blockFactory) {
        double[] values = UninitializedArrays.newDoubleArray(maxRows);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newDoubleArrayVector(values, produced).asBlock();
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
                    double[] packed = UninitializedArrays.newDoubleArray(nonNull);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newDoubleArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
        return blockFactory.newDoubleArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
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
        // Dictionary-encoded chunk: emit an OrdinalBytesRefBlock so downstream BlockHash can
        // hash only k dictionary entries instead of N row values. Skip for UUID, where each
        // value needs per-row byte formatting that the ordinal path does not perform.
        if (dictionary != null && isUuid == false) {
            return readBytesBatchAsOrdinals(maxRows, blockFactory);
        }
        if (maxDefLevel == 0) {
            BytesRef[] allValues = new BytesRef[maxRows];
            int produced = 0;
            int remaining = maxRows;
            while (remaining > 0 && ensurePage()) {
                int fromPage = Math.min(remaining, availableInPage());
                BytesRef[] vals = readBinaryValues(fromPage);
                for (int i = 0; i < fromPage; i++) {
                    allValues[produced + i] = isUuid
                        ? new BytesRef(ParquetColumnDecoding.formatUuid(vals[i].bytes, vals[i].offset, vals[i].length))
                        : Utf8Sanitizer.sanitize(vals[i]);
                }
                advancePosition(fromPage);
                produced += fromPage;
                remaining -= fromPage;
            }
            Block constant = ConstantBlockDetection.tryConstantBytesRef(allValues, produced, blockFactory);
            if (constant != null) {
                return constant;
            }
            return buildBytesRefBlock(allValues, null, produced, blockFactory);
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
                    BytesRef uuidRef = vals[valIdx++];
                    allValues[produced + i] = new BytesRef(ParquetColumnDecoding.formatUuid(uuidRef.bytes, uuidRef.offset, uuidRef.length));
                } else {
                    allValues[produced + i] = Utf8Sanitizer.sanitize(vals[valIdx++]);
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
        return buildBytesRefBlock(allValues, allNulls, produced, blockFactory);
    }

    /**
     * Builds a {@code BytesRefBlock} from already-materialized values, pre-sizing the byte storage to
     * the exact total byte size so the backing {@code BytesRefArray} does not regrow as values are
     * appended. {@code nulls} may be {@code null} when no position is null; otherwise a set bit marks a
     * null position that is skipped when sizing and appended as null.
     */
    private static Block buildBytesRefBlock(BytesRef[] values, WordMask nulls, int count, BlockFactory blockFactory) {
        long byteHint = 0;
        for (int i = 0; i < count; i++) {
            if (nulls == null || nulls.get(i) == false) {
                byteHint += values[i].length;
            }
        }
        try (var builder = blockFactory.newBytesRefBlockBuilder(count, byteHint)) {
            for (int i = 0; i < count; i++) {
                if (nulls != null && nulls.get(i)) {
                    builder.appendNull();
                } else {
                    builder.appendBytesRef(values[i]);
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

    private Block readBytesBatchAsOrdinals(int maxRows, BlockFactory blockFactory) {
        int[] ordinals = UninitializedArrays.newIntArray(maxRows);
        WordMask nulls = maxDefLevel > 0 ? buffers.nullsMask(maxRows) : null;
        int produced = 0;
        int remaining = maxRows;
        boolean fallback = false;
        while (remaining > 0 && ensurePage()) {
            if (currentEncoding.usesDictionary() == false) {
                fallback = true;
                break;
            }
            int fromPage = Math.min(remaining, availableInPage());
            if (nulls == null) {
                dictDecoder.readIndices(ordinals, produced, fromPage);
            } else {
                int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
                if (nonNull == fromPage) {
                    dictDecoder.readIndices(ordinals, produced, fromPage);
                } else if (nonNull > 0) {
                    int[] packed = UninitializedArrays.newIntArray(nonNull);
                    dictDecoder.readIndices(packed, 0, nonNull);
                    int pi = 0;
                    for (int i = 0; i < fromPage; i++) {
                        if (nulls.get(produced + i) == false) {
                            ordinals[produced + i] = packed[pi++];
                        }
                    }
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        if (fallback) {
            return finishMaterializedFallback(ordinals, nulls, produced, remaining, blockFactory);
        }
        return buildOrdinalResult(ordinals, nulls, produced, blockFactory);
    }

    private Block buildOrdinalResult(int[] ordinals, WordMask nulls, int produced, BlockFactory blockFactory) {
        BytesRef[] dict = dictDecoder.getDictionaryBytesRefs(dictionary);
        if (produced == 0) {
            return blockFactory.newConstantNullBlock(0);
        }
        if (nulls != null && nulls.popCount() == produced) {
            return blockFactory.newConstantNullBlock(produced);
        }
        // Non-null, all-equal indices: emit a constant block to skip the ordinal indirection.
        if (nulls == null || nulls.isEmpty()) {
            int constantOrdinal = detectConstantOrdinal(ordinals, produced);
            if (constantOrdinal >= 0) {
                return blockFactory.newConstantBytesRefBlockWith(Utf8Sanitizer.sanitize(dict[constantOrdinal]), produced);
            }
        }
        IntBlock ordinalsBlock = null;
        BytesRefVector dictVector = null;
        boolean success = false;
        try {
            ordinalsBlock = buildOrdinalsBlock(ordinals, nulls, produced, blockFactory);
            dictVector = buildDictionaryVector(dict, blockFactory);
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ordinalsBlock, dictVector);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(ordinalsBlock, dictVector);
            }
        }
    }

    private static int detectConstantOrdinal(int[] ordinals, int produced) {
        if (produced == 0) {
            return -1;
        }
        int first = ordinals[0];
        for (int i = 1; i < produced; i++) {
            if (ordinals[i] != first) {
                return -1;
            }
        }
        return first;
    }

    private IntBlock buildOrdinalsBlock(int[] ordinals, WordMask nulls, int produced, BlockFactory blockFactory) {
        int[] sized = produced < ordinals.length ? Arrays.copyOf(ordinals, produced) : ordinals;
        if (nulls == null || nulls.isEmpty()) {
            return blockFactory.newIntArrayVector(sized, produced).asBlock();
        }
        return blockFactory.newIntArrayBlock(sized, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
    }

    private BytesRefVector buildDictionaryVector(BytesRef[] entries, BlockFactory blockFactory) {
        BytesRefArray array = ensureCachedDictArray(entries, blockFactory);
        // newBytesRefArrayVector takes ownership of one ref of the array: its closeInternal calls
        // values.close() -> decRef(). The class Javadoc on BytesRefArrayVector that says it "does
        // not take ownership" is misleading on the refcount side and applies only to the
        // breaker accounting (which the factory adjusts here). To keep our cached ref alive
        // across batches, incRef before handing the array to the wrapper.
        array.incRef();
        boolean success = false;
        try {
            BytesRefVector vector = blockFactory.newBytesRefArrayVector(array, entries.length);
            success = true;
            return vector;
        } finally {
            if (success == false) {
                array.decRef();
            }
        }
    }

    private BytesRefArray ensureCachedDictArray(BytesRef[] entries, BlockFactory blockFactory) {
        if (cachedDictArray != null && cachedDictArraySource == dictionary) {
            return cachedDictArray;
        }
        // First call for this reader, or — and this should never happen given parquet-mr keeps
        // one Dictionary per chunk and a PageColumnReader reads exactly one chunk — the
        // Dictionary identity changed mid-reader. Assert in dev/test builds so a violated
        // invariant fails loudly; rebuild rather than reuse a stale cache in production.
        assert cachedDictArray == null : "PageColumnReader saw a Dictionary identity change mid-chunk; reader should have been replaced";
        releaseCachedDictArray();
        BytesRefArray array = new BytesRefArray(entries.length, blockFactory.bigArrays());
        boolean success = false;
        try {
            for (BytesRef entry : entries) {
                // KEYWORD ordinal dictionary path only (buildDictionaryVector -> here); sanitize each
                // dictionary entry once per chunk so the OrdinalBytesRefBlock never exposes malformed UTF-8.
                array.append(Utf8Sanitizer.sanitize(entry));
            }
            cachedDictArray = array;
            cachedDictArraySource = dictionary;
            success = true;
            return array;
        } finally {
            if (success == false) {
                array.close();
            }
        }
    }

    private void releaseCachedDictArray() {
        if (cachedDictArray != null) {
            cachedDictArray.decRef();
            cachedDictArray = null;
            cachedDictArraySource = null;
        }
    }

    @Override
    public void close() {
        releaseCachedDictArray();
    }

    private Block finishMaterializedFallback(int[] ordinals, WordMask nulls, int produced, int remaining, BlockFactory blockFactory) {
        BytesRef[] dict = dictDecoder.getDictionaryBytesRefs(dictionary);
        int total = produced + remaining;
        BytesRef[] all = new BytesRef[total];
        WordMask combinedNulls = nulls;
        for (int i = 0; i < produced; i++) {
            if (combinedNulls != null && combinedNulls.get(i)) {
                continue;
            }
            all[i] = dict[ordinals[i]];
        }
        int filled = produced;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            if (combinedNulls == null) {
                BytesRef[] vals = readBinaryValues(fromPage);
                System.arraycopy(vals, 0, all, filled, fromPage);
            } else {
                WordMask pageNulls = buffers.valueSelection(fromPage);
                int nonNull = defDecoder.readBatch(fromPage, pageNulls, 0);
                BytesRef[] vals = nonNull > 0 ? readBinaryValues(nonNull) : null;
                int valIdx = 0;
                for (int i = 0; i < fromPage; i++) {
                    if (pageNulls.get(i)) {
                        combinedNulls.set(filled + i);
                    } else {
                        all[filled + i] = vals[valIdx++];
                    }
                }
            }
            advancePosition(fromPage);
            filled += fromPage;
            remaining -= fromPage;
        }
        if (combinedNulls != null && combinedNulls.isEmpty() == false) {
            Block allNull = ConstantBlockDetection.tryAllNull(combinedNulls.toBitSet(), filled, blockFactory);
            if (allNull != null) {
                return allNull;
            }
        }
        // KEYWORD materialized fallback: {@code all} mixes raw dictionary entries with scalar values read
        // after the chunk fell off dictionary encoding (this path is never taken for UUID, see readBytesBatch),
        // so sanitize each position once before building. sanitize returns the input unchanged when it is
        // already well-formed, so shared dictionary entries are not copied or mutated.
        for (int i = 0; i < filled; i++) {
            if (combinedNulls == null || combinedNulls.get(i) == false) {
                all[i] = Utf8Sanitizer.sanitize(all[i]);
            }
        }
        return buildBytesRefBlock(all, combinedNulls, filled, blockFactory);
    }

    // --- Datetime ---

    private Block readDatetimeBatch(int maxRows, BlockFactory blockFactory) {
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.INT96) {
            return readInt96Batch(maxRows, blockFactory);
        }
        if (info.parquetType() == PrimitiveType.PrimitiveTypeName.BINARY) {
            // Declared string->datetime coercion: decode the column natively as bytes (dictionary and plain paths
            // both reused as-is), then parse each value with the column's declared format (ISO default) via the
            // shared scalar. An unparseable value follows the read's error policy through onCoercionFailure:
            // a live sink nulls the position + warns, a null sink (fail_fast) fails the read.
            Block bytes = readBytesBatch(maxRows, blockFactory);
            try {
                return ParquetColumnDecoding.bytesBlockToDatetimeMillis(
                    bytes,
                    info.dateFormatter(),
                    blockFactory,
                    String.join(".", descriptor.getPath()),
                    coercionWarnings
                );
            } finally {
                bytes.close();
            }
        }
        boolean isDate = info.parquetType() == PrimitiveType.PrimitiveTypeName.INT32;
        long[] values = UninitializedArrays.newLongArray(maxRows);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
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
                    long[] packed = UninitializedArrays.newLongArray(nonNull);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
        return blockFactory.newLongArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
    }

    private void readDatetimeValues(long[] values, int offset, int count, boolean isDate) {
        if (isDate) {
            int[] intValues = buffers.ints(count);
            readIntsDispatch(intValues, 0, count);
            for (int i = 0; i < count; i++) {
                values[offset + i] = ParquetColumnDecoding.dateDaysToMillis(intValues[i]);
            }
        } else {
            readLongsDispatch(values, offset, count);
            LogicalTypeAnnotation logicalType = info.logicalType();
            if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                for (int i = 0; i < count; i++) {
                    values[offset + i] = ParquetColumnDecoding.convertTimestampToMillis(values[offset + i], logicalType);
                }
            }
        }
    }

    /**
     * Optimized-path counterpart of {@link ParquetFormatReader}'s {@code readDateNanosColumn}: decodes an INT64
     * {@code TIMESTAMP(MICROS|NANOS)} column into a {@code DATE_NANOS} block of epoch-nanoseconds. {@code NANOS}
     * passes through; {@code MICROS} is scaled ×1_000. {@code MICROS} values whose scaled instant falls outside the
     * representable {@code date_nanos} range (~1677-2262) are emitted as null (with one deduplicated warning) rather
     * than silently wrapping. The common in-range case keeps the existing no-null fast vector path; a null mask is
     * only materialised when a value actually overflows.
     */
    private Block readDateNanosBatch(int maxRows, BlockFactory blockFactory) {
        boolean micros = ParquetColumnDecoding.isMicrosTimestamp(info.logicalType());
        long[] values = UninitializedArrays.newLongArray(maxRows);
        if (maxDefLevel == 0) {
            int produced = 0;
            int remaining = maxRows;
            while (remaining > 0 && ensurePage()) {
                int fromPage = Math.min(remaining, availableInPage());
                readLongsDispatch(values, produced, fromPage);
                advancePosition(fromPage);
                produced += fromPage;
                remaining -= fromPage;
            }
            WordMask overflow = scaleDateNanosDense(values, produced, micros);
            if (overflow == null) {
                Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
                if (constant != null) return constant;
                if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
                return blockFactory.newLongArrayVector(values, produced).asBlock();
            }
            ParquetColumnDecoding.warnTimestampOutOfRange(info);
            Block allNull = ConstantBlockDetection.tryAllNull(overflow.toBitSet(), produced, blockFactory);
            if (allNull != null) {
                return allNull;
            }
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayBlock(values, produced, null, overflow.toBitSet(), Block.MvOrdering.UNORDERED);
        }
        WordMask nulls = buffers.nullsMask(maxRows);
        int produced = 0;
        int remaining = maxRows;
        while (remaining > 0 && ensurePage()) {
            int fromPage = Math.min(remaining, availableInPage());
            int nonNull = defDecoder.readBatch(fromPage, nulls, produced);
            if (nonNull > 0) {
                if (nonNull == fromPage) {
                    readLongsDispatch(values, produced, nonNull);
                } else {
                    long[] packed = UninitializedArrays.newLongArray(nonNull);
                    readLongsDispatch(packed, 0, nonNull);
                    scatter(packed, values, nulls, produced, fromPage);
                }
            }
            advancePosition(fromPage);
            produced += fromPage;
            remaining -= fromPage;
        }
        boolean anyOverflow = scaleDateNanosMasked(values, produced, micros, nulls);
        if (anyOverflow) {
            ParquetColumnDecoding.warnTimestampOutOfRange(info);
        }
        if (nulls.isEmpty()) {
            Block constant = ConstantBlockDetection.tryConstantLong(values, produced, blockFactory);
            if (constant != null) return constant;
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
        return blockFactory.newLongArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
    }

    /**
     * Scales a dense (no definition-level nulls) {@code MICROS} timestamp array to epoch-nanoseconds in place. A
     * {@code NANOS} column is already in its final unit and is left untouched. Returns a mask of positions that
     * overflowed the representable {@code date_nanos} range (for the caller to null out), or {@code null} when
     * nothing overflowed (the common case, which keeps the no-null fast path).
     */
    private WordMask scaleDateNanosDense(long[] values, int count, boolean micros) {
        if (micros == false) {
            return null;
        }
        WordMask overflow = null;
        for (int i = 0; i < count; i++) {
            long raw = values[i];
            if (ParquetColumnDecoding.microsOverflowsNanos(raw)) {
                if (overflow == null) {
                    overflow = buffers.nullsMask(count);
                }
                overflow.set(i);
            } else {
                values[i] = raw * ParquetColumnDecoding.NANOS_PER_MICRO;
            }
        }
        return overflow;
    }

    /**
     * Scales the non-null {@code MICROS} timestamp positions to epoch-nanoseconds in place, extending {@code nulls}
     * with any positions that overflow the representable {@code date_nanos} range. A {@code NANOS} column needs no
     * scaling. Returns whether any overflow occurred.
     */
    private static boolean scaleDateNanosMasked(long[] values, int count, boolean micros, WordMask nulls) {
        if (micros == false) {
            return false;
        }
        boolean anyOverflow = false;
        for (int i = 0; i < count; i++) {
            if (nulls.get(i)) {
                continue;
            }
            long raw = values[i];
            if (ParquetColumnDecoding.microsOverflowsNanos(raw)) {
                nulls.set(i);
                anyOverflow = true;
            } else {
                values[i] = raw * ParquetColumnDecoding.NANOS_PER_MICRO;
            }
        }
        return anyOverflow;
    }

    private Block readInt96Batch(int maxRows, BlockFactory blockFactory) {
        long[] values = UninitializedArrays.newLongArray(maxRows);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
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
                    long[] packed = UninitializedArrays.newLongArray(nonNull);
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
            if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
            return blockFactory.newLongArrayVector(values, produced).asBlock();
        }
        Block allNull = ConstantBlockDetection.tryAllNull(nulls.toBitSet(), produced, blockFactory);
        if (allNull != null) {
            return allNull;
        }
        if (needsShrinking(produced, maxRows)) values = Arrays.copyOf(values, produced);
        return blockFactory.newLongArrayBlock(values, produced, null, nulls.toBitSet(), Block.MvOrdering.UNORDERED);
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
            values[offset + i] = ParquetColumnDecoding.int96ToEpochMillis(binaries[i].bytes, binaries[i].offset, binaries[i].length);
        }
    }

    private static boolean needsShrinking(int produced, int maxRows) {
        // Heuristic: resize if we save at least half the size
        return produced < maxRows / 2;
    }

    // --- Block construction helpers ---
    // Primitive batch methods decode directly into a fresh array that becomes the Block's
    // backing store. For full batches (produced == maxRows) this is zero-copy; partial
    // last batches are right-sized via Arrays.copyOf. DecodeBuffers scratch arrays are
    // used only as intermediaries that are consumed within a single batch (e.g. int-to-long
    // widening in readInt32AsLongBatch) and never handed to BlockFactory.

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
