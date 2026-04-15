/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.SeekableInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * A {@link PageReader} that reads only pages whose row span overlaps with the given
 * {@link RowRanges}. Non-surviving pages incur zero I/O, zero decompression, and
 * zero memory allocation — the reader seeks past them using the {@link OffsetIndex}.
 *
 * <p>This replaces parquet-java's {@code ColumnChunkPageReader} which eagerly parses
 * all page headers and queues all compressed payloads at row-group open time. By
 * reading directly from a {@link SeekableInputStream} (backed by prefetched data
 * from {@link ParquetStorageObjectAdapter}), only surviving pages are touched.
 */
final class SelectivePageReader implements PageReader, Closeable {

    private final SeekableInputStream input;
    private final OffsetIndex offsetIndex;
    private final RowRanges rowRanges;
    private final CompressionCodecFactory.BytesInputDecompressor decompressor;
    private final ColumnChunkMetaData columnMeta;
    private final long rowGroupRowCount;
    private final long totalValueCount;
    private int currentPageIdx;
    private boolean dictionaryRead;

    SelectivePageReader(
        SeekableInputStream input,
        OffsetIndex offsetIndex,
        RowRanges rowRanges,
        CompressionCodecFactory.BytesInputDecompressor decompressor,
        ColumnChunkMetaData columnMeta,
        long rowGroupRowCount
    ) {
        this.input = input;
        this.offsetIndex = offsetIndex;
        this.rowRanges = rowRanges;
        this.decompressor = decompressor;
        this.columnMeta = columnMeta;
        this.rowGroupRowCount = rowGroupRowCount;
        this.totalValueCount = columnMeta.getValueCount();
        this.currentPageIdx = 0;
        this.dictionaryRead = false;
    }

    @Override
    public DictionaryPage readDictionaryPage() {
        if (dictionaryRead) {
            return null;
        }
        dictionaryRead = true;
        long dictOffset = columnMeta.getDictionaryPageOffset();
        if (dictOffset <= 0) {
            return null;
        }
        try {
            input.seek(dictOffset);
            PageHeader header = Util.readPageHeader(input);
            if (header.getType() != PageType.DICTIONARY_PAGE) {
                return null;
            }
            int compressedSize = header.getCompressed_page_size();
            byte[] raw = new byte[compressedSize];
            input.readFully(raw);
            BytesInput decompressed = decompressor.decompress(BytesInput.from(raw), header.getUncompressed_page_size());
            return new DictionaryPage(
                decompressed,
                header.getDictionary_page_header().getNum_values(),
                fromFormatEncoding(header.getDictionary_page_header().getEncoding())
            );
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read dictionary page for column " + columnMeta.getPath(), e);
        }
    }

    @Override
    public long getTotalValueCount() {
        return totalValueCount;
    }

    @Override
    public DataPage readPage() {
        int pageCount = offsetIndex.getPageCount();
        while (currentPageIdx < pageCount) {
            int pageIdx = currentPageIdx++;

            long pageRowStart = offsetIndex.getFirstRowIndex(pageIdx);
            long pageRowEnd = (pageIdx + 1 < pageCount) ? offsetIndex.getFirstRowIndex(pageIdx + 1) : rowGroupRowCount;

            if (rowRanges != null && rowRanges.overlaps(pageRowStart, pageRowEnd) == false) {
                continue;
            }

            try {
                long pageFileOffset = offsetIndex.getOffset(pageIdx);
                input.seek(pageFileOffset);
                PageHeader header = Util.readPageHeader(input);

                int compressedSize = header.getCompressed_page_size();
                int uncompressedSize = header.getUncompressed_page_size();
                byte[] raw = new byte[compressedSize];
                input.readFully(raw);

                return switch (header.getType()) {
                    case DATA_PAGE -> readDataPageV1(header, BytesInput.from(raw), compressedSize, uncompressedSize, pageIdx);
                    case DATA_PAGE_V2 -> readDataPageV2(header, BytesInput.from(raw), compressedSize, uncompressedSize, pageIdx);
                    default -> throw new IOException("Unexpected page type at page " + pageIdx + ": " + header.getType());
                };
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read page " + (pageIdx) + " for column " + columnMeta.getPath(), e);
            }
        }
        return null;
    }

    private DataPageV1 readDataPageV1(PageHeader header, BytesInput compressedBytes, int compressedSize, int uncompressedSize, int pageIdx)
        throws IOException {
        DataPageHeader dataHeader = header.getData_page_header();
        BytesInput decompressed = decompressor.decompress(compressedBytes, uncompressedSize);
        return new DataPageV1(
            decompressed,
            dataHeader.getNum_values(),
            uncompressedSize,
            offsetIndex.getFirstRowIndex(pageIdx),
            indexRowCount(pageIdx),
            fromFormatStatistics(dataHeader),
            fromFormatEncoding(dataHeader.getRepetition_level_encoding()),
            fromFormatEncoding(dataHeader.getDefinition_level_encoding()),
            fromFormatEncoding(dataHeader.getEncoding())
        );
    }

    private DataPageV2 readDataPageV2(PageHeader header, BytesInput compressedBytes, int compressedSize, int uncompressedSize, int pageIdx)
        throws IOException {
        DataPageHeaderV2 v2Header = header.getData_page_header_v2();
        int rlSize = v2Header.getRepetition_levels_byte_length();
        int dlSize = v2Header.getDefinition_levels_byte_length();
        byte[] allBytes = compressedBytes.toByteArray();
        BytesInput rlBytes = BytesInput.from(allBytes, 0, rlSize);
        BytesInput dlBytes = BytesInput.from(allBytes, rlSize, dlSize);

        int dataCompressedSize = compressedSize - rlSize - dlSize;
        int dataUncompressedSize = uncompressedSize - rlSize - dlSize;
        BytesInput dataBytes = BytesInput.from(allBytes, rlSize + dlSize, dataCompressedSize);

        boolean isCompressed = v2Header.isIs_compressed();
        BytesInput decompressedData = isCompressed ? decompressor.decompress(dataBytes, dataUncompressedSize) : dataBytes;

        return DataPageV2.uncompressed(
            v2Header.getNum_rows(),
            v2Header.getNum_nulls(),
            v2Header.getNum_values(),
            offsetIndex.getFirstRowIndex(pageIdx),
            rlBytes,
            dlBytes,
            fromFormatEncoding(v2Header.getEncoding()),
            decompressedData,
            fromFormatStatistics(v2Header)
        );
    }

    private int indexRowCount(int pageIdx) {
        long pageStart = offsetIndex.getFirstRowIndex(pageIdx);
        long pageEnd = (pageIdx + 1 < offsetIndex.getPageCount()) ? offsetIndex.getFirstRowIndex(pageIdx + 1) : rowGroupRowCount;
        return (int) (pageEnd - pageStart);
    }

    private static Encoding fromFormatEncoding(org.apache.parquet.format.Encoding formatEncoding) {
        return Encoding.valueOf(formatEncoding.name());
    }

    private static org.apache.parquet.column.statistics.Statistics<?> fromFormatStatistics(DataPageHeader dataHeader) {
        return null;
    }

    private static org.apache.parquet.column.statistics.Statistics<?> fromFormatStatistics(DataPageHeaderV2 v2Header) {
        return null;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }
}
