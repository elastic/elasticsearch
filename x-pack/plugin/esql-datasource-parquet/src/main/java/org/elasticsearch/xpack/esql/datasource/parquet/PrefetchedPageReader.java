/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * {@link PageReader} backed by an in-memory queue of compressed {@link DataPage}s plus an
 * optional compressed {@link DictionaryPage}. Built by {@code PrefetchedRowGroupBuilder} from
 * prefetched column chunk bytes; replaces parquet-mr's {@code ColumnChunkPageReadStore$ColumnChunkPageReader}
 * for the optimized iterator's read path.
 *
 * <p>Mirrors parquet-mr's {@code readPage()} contract: each call pops one compressed
 * {@link DataPage} from the queue and returns its decompressed equivalent, preserving the
 * original page type ({@link DataPageV1} stays V1, {@link DataPageV2} stays V2 with only the
 * data portion decompressed). Encryption and CRC verification are not supported.
 */
final class PrefetchedPageReader implements PageReader {

    /**
     * A compressed data page paired with its {@code firstRowIndex}. Required because
     * parquet-mr's public {@link DataPageV2} constructor does not accept {@code firstRowIndex}
     * (only the package-private one does), so we cannot rely on the {@link DataPage} carrying
     * it through. {@code firstRowIndex == -1} means "unknown"; {@link PageColumnReader} treats
     * an empty {@code getFirstRowIndex()} as "page starts at the current cursor".
     */
    record CompressedPage(DataPage page, long firstRowIndex) {}

    private final BytesInputDecompressor decompressor;
    private final long valueCount;
    private final Deque<CompressedPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;

    private DictionaryPage cachedDictionaryPage;
    private boolean dictionaryDecompressed;

    PrefetchedPageReader(
        BytesInputDecompressor decompressor,
        List<CompressedPage> compressedPages,
        DictionaryPage compressedDictionaryPage,
        long valueCount
    ) {
        this.decompressor = decompressor;
        this.compressedPages = new ArrayDeque<>(compressedPages);
        this.compressedDictionaryPage = compressedDictionaryPage;
        this.valueCount = valueCount;
    }

    @Override
    public long getTotalValueCount() {
        return valueCount;
    }

    @Override
    public DataPage readPage() {
        CompressedPage entry = compressedPages.poll();
        if (entry == null) {
            return null;
        }
        DataPage page = entry.page();
        if (page instanceof DataPageV1 v1) {
            return decompressV1(v1);
        }
        if (page instanceof DataPageV2 v2) {
            return decompressV2(v2, entry.firstRowIndex());
        }
        throw new ParquetDecodingException("Unexpected page type: " + page.getClass().getName());
    }

    @Override
    public DictionaryPage readDictionaryPage() {
        if (compressedDictionaryPage == null) {
            return null;
        }
        if (dictionaryDecompressed) {
            return cachedDictionaryPage;
        }
        try {
            BytesInput decompressed = decompressor.decompress(
                compressedDictionaryPage.getBytes(),
                compressedDictionaryPage.getUncompressedSize()
            );
            cachedDictionaryPage = new DictionaryPage(
                decompressed,
                compressedDictionaryPage.getUncompressedSize(),
                compressedDictionaryPage.getDictionarySize(),
                compressedDictionaryPage.getEncoding()
            );
        } catch (IOException e) {
            throw new ParquetDecodingException("Could not decompress dictionary page", e);
        }
        // Set the cache flag only after a successful decompression. If decompression throws,
        // the next call will retry instead of silently returning a null cachedDictionaryPage.
        dictionaryDecompressed = true;
        return cachedDictionaryPage;
    }

    private DataPageV1 decompressV1(DataPageV1 v1) {
        try {
            BytesInput decompressed = decompressor.decompress(v1.getBytes(), v1.getUncompressedSize());
            int indexRowCount = v1.getIndexRowCount().orElse(-1);
            long firstRowIndex = v1.getFirstRowIndex().orElse(-1L);
            if (firstRowIndex >= 0 && indexRowCount >= 0) {
                return new DataPageV1(
                    decompressed,
                    v1.getValueCount(),
                    v1.getUncompressedSize(),
                    firstRowIndex,
                    indexRowCount,
                    v1.getStatistics(),
                    v1.getRlEncoding(),
                    v1.getDlEncoding(),
                    v1.getValueEncoding()
                );
            }
            return new DataPageV1(
                decompressed,
                v1.getValueCount(),
                v1.getUncompressedSize(),
                v1.getStatistics(),
                v1.getRlEncoding(),
                v1.getDlEncoding(),
                v1.getValueEncoding()
            );
        } catch (IOException e) {
            throw new ParquetDecodingException("Could not decompress V1 data page", e);
        }
    }

    private DataPageV2 decompressV2(DataPageV2 v2, long firstRowIndex) {
        if (v2.isCompressed() == false) {
            // Parquet-mr's writer can produce a V2 page where only the data portion was eligible
            // for compression but compression chose not to compress (is_compressed=false). Expose
            // it as already-uncompressed so downstream decoders skip the decompressor entirely.
            return DataPageV2.uncompressed(
                v2.getRowCount(),
                v2.getNullCount(),
                v2.getValueCount(),
                firstRowIndex,
                v2.getRepetitionLevels(),
                v2.getDefinitionLevels(),
                v2.getDataEncoding(),
                v2.getData(),
                v2.getStatistics()
            );
        }
        try {
            int rlBytes = (int) v2.getRepetitionLevels().size();
            int dlBytes = (int) v2.getDefinitionLevels().size();
            int uncompressedDataSize = v2.getUncompressedSize() - rlBytes - dlBytes;
            BytesInput decompressedData = decompressor.decompress(v2.getData(), uncompressedDataSize);
            return DataPageV2.uncompressed(
                v2.getRowCount(),
                v2.getNullCount(),
                v2.getValueCount(),
                firstRowIndex,
                v2.getRepetitionLevels(),
                v2.getDefinitionLevels(),
                v2.getDataEncoding(),
                decompressedData,
                v2.getStatistics()
            );
        } catch (IOException e) {
            throw new ParquetDecodingException("Could not decompress V2 data page", e);
        }
    }
}
