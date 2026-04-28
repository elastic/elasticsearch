/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

/**
 * Builds a {@link PrefetchedPageReadStore} from prefetched (or, as a fallback, sync-fetched)
 * column chunk bytes for a single Parquet row group. Replaces parquet-mr's
 * {@code internalReadFilteredRowGroup} on the optimized iterator's read path: bytes for skipped
 * pages are never read, so filtered prefetch actually saves I/O end-to-end.
 *
 * <p>For each projected column:
 * <ul>
 *   <li><b>Filtered path</b> (rowRanges + offset index present): walk the offset index and pick
 *       the pages whose row span overlaps the selection; slice each page's bytes from the
 *       prefetched chunks and parse the page header from the slice prefix.</li>
 *   <li><b>Sequential path</b> (no offset index, or no row ranges, or no prefetched chunks):
 *       walk the column chunk linearly via {@link Util#readPageHeader(InputStream)} until
 *       {@code totalSize} bytes are consumed.</li>
 * </ul>
 *
 * <p>Encrypted columns are not supported and surface as {@link IllegalArgumentException} (HTTP
 * 400) so user-supplied encrypted files produce a clear client-side error rather than a 500.
 * Pages with CRC verification enabled are also unsupported; the optimized iterator never
 * enables CRC, so a request to handle one is treated as a programmer error.
 */
final class PrefetchedRowGroupBuilder {

    private static final ParquetMetadataConverter METADATA_CONVERTER = new ParquetMetadataConverter();

    private PrefetchedRowGroupBuilder() {}

    /**
     * Builds an in-memory {@link PrefetchedPageReadStore} for the row group.
     *
     * @param block metadata for the row group
     * @param rowGroupOrdinal ordinal of the row group within the file (used in error messages
     *            and for offset-index lookup against {@code preloadedMetadata})
     * @param projectedSchema schema describing the projected columns; used to resolve
     *            {@link ColumnDescriptor}s that are stable across the iterator's lifetime
     * @param projectedColumnPaths dotted paths of the columns to materialize; columns outside
     *            this set are skipped
     * @param rowRanges optional surviving row ranges produced by
     *            {@link ColumnIndexRowRangesComputer}; when {@code null} the entire row group
     *            is loaded
     * @param preloadedMetadata source of {@link OffsetIndex} entries for filtered-path columns
     * @param prefetchedChunks coalesced byte ranges produced by {@link ColumnChunkPrefetcher},
     *            keyed by file offset; {@code null} forces the sync fallback through
     *            {@code storageObject}
     * @param storageObject sync fallback used when {@code prefetchedChunks} is {@code null}
     *            or when an individual column lacks an offset index in the filtered path
     * @param codecFactory shared {@link PlainCompressionCodecFactory} used to build per-column
     *            decompressors
     */
    static PrefetchedPageReadStore build(
        BlockMetaData block,
        int rowGroupOrdinal,
        MessageType projectedSchema,
        Set<String> projectedColumnPaths,
        RowRanges rowRanges,
        PreloadedRowGroupMetadata preloadedMetadata,
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> prefetchedChunks,
        StorageObject storageObject,
        CompressionCodecFactory codecFactory
    ) {
        Map<String, ColumnDescriptor> descriptorsByPath = new HashMap<>();
        for (ColumnDescriptor desc : projectedSchema.getColumns()) {
            descriptorsByPath.put(String.join(".", desc.getPath()), desc);
        }

        Map<ColumnDescriptor, PrefetchedPageReader> readers = new HashMap<>();
        for (ColumnChunkMetaData column : block.getColumns()) {
            String path = column.getPath().toDotString();
            if (projectedColumnPaths.contains(path) == false) {
                continue;
            }
            if (column.isEncrypted()) {
                // User-facing error: the file uses a feature we don't support. Use a plain
                // IllegalArgumentException so it maps to HTTP 400 rather than 500 (a
                // QlIllegalArgumentException would be wrong here per its Javadoc — that one is
                // reserved for bugs and is mapped to 500).
                throw new IllegalArgumentException(
                    "Encrypted columns are not supported by the optimized Parquet reader: column ["
                        + path
                        + "] in row group ["
                        + rowGroupOrdinal
                        + "]"
                );
            }
            ColumnDescriptor descriptor = descriptorsByPath.get(path);
            if (descriptor == null) {
                continue;
            }
            BytesInputDecompressor decompressor = codecFactory.getDecompressor(column.getCodec());
            OffsetIndex offsetIndex = preloadedMetadata != null ? preloadedMetadata.getOffsetIndex(rowGroupOrdinal, path) : null;

            ColumnPageBytesSource source = sourceFor(column, prefetchedChunks, storageObject, rowGroupOrdinal);
            PrimitiveType primitiveType = descriptor.getPrimitiveType();
            PrefetchedPageReader reader;
            if (rowRanges != null && offsetIndex != null && source.supportsRandomAccess()) {
                reader = buildFiltered(
                    column,
                    primitiveType,
                    offsetIndex,
                    rowRanges,
                    source,
                    block.getRowCount(),
                    decompressor,
                    rowGroupOrdinal
                );
            } else {
                reader = buildSequential(column, primitiveType, source, decompressor, rowGroupOrdinal);
            }
            readers.put(descriptor, reader);
        }
        return new PrefetchedPageReadStore(readers, block.getRowCount());
    }

    /**
     * Filtered path: pick pages whose row span overlaps {@code rowRanges} and slice each page's
     * bytes directly out of the prefetched chunks.
     *
     * <p>For repeated columns (maxRepLevel > 0), this assumes pages are row-aligned per
     * the OffsetIndex - i.e. firstRowIndex(p) is the first row whose values start in
     * page p. parquet-mr's writers always break pages on row boundaries, and
     * parquet-mr's own internalReadFilteredRowGroup makes the same assumption. Files
     * written by non-conformant writers may put a row's values across a page boundary;
     * our overlap-based page selection still drops extra non-matching rows via the
     * RECHECK filter applied upstream (see ColumnIndexRowRangesComputer doc), so
     * correctness is preserved but we may keep an extra page that contains no
     * matching rows. If a real-world non-conformant writer ever shows up: either
     * prefetch+include the previous/next page, or fall back to full-chunk fetch for
     * that column.
     */
    private static PrefetchedPageReader buildFiltered(
        ColumnChunkMetaData column,
        PrimitiveType primitiveType,
        OffsetIndex offsetIndex,
        RowRanges rowRanges,
        ColumnPageBytesSource source,
        long rowGroupRowCount,
        BytesInputDecompressor decompressor,
        int rowGroupOrdinal
    ) {
        DictionaryPage dictPage = readDictionaryPageIfPresent(column, source, rowGroupOrdinal);
        long valueCount = 0;
        List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>();
        int pageCount = offsetIndex.getPageCount();
        for (int p = 0; p < pageCount; p++) {
            long pageStartRow = offsetIndex.getFirstRowIndex(p);
            long pageEndRow = (p + 1 < pageCount) ? offsetIndex.getFirstRowIndex(p + 1) : rowGroupRowCount;
            if (rowRanges.overlaps(pageStartRow, pageEndRow) == false) {
                continue;
            }
            long fileOffset = offsetIndex.getOffset(p);
            int compressedPageSize = offsetIndex.getCompressedPageSize(p);
            ByteBuffer pageBytes = source.slice(fileOffset, compressedPageSize, column.getPath().toDotString(), p);
            DataPage decoded = parseDataPageFromSlice(
                pageBytes,
                primitiveType,
                pageStartRow,
                (int) (pageEndRow - pageStartRow),
                column.getPath().toDotString(),
                rowGroupOrdinal
            );
            // Pair every queued page with its firstRowIndex from the offset index. V1 pages also
            // carry it via DataPageV1's 9-arg constructor, but pairing here is cheap and keeps
            // V1 and V2 on a single, uniform code path through PrefetchedPageReader.
            pages.add(new PrefetchedPageReader.CompressedPage(decoded, pageStartRow));
            valueCount += decoded.getValueCount();
        }
        return new PrefetchedPageReader(decompressor, pages, dictPage, valueCount);
    }

    /**
     * Sequential path: walk the entire column chunk page-by-page until {@code totalSize} bytes
     * are consumed. Used when no offset index is available, no row ranges were supplied, or
     * the prefetched chunks could not satisfy the column.
     */
    private static PrefetchedPageReader buildSequential(
        ColumnChunkMetaData column,
        PrimitiveType primitiveType,
        ColumnPageBytesSource source,
        BytesInputDecompressor decompressor,
        int rowGroupOrdinal
    ) {
        long startingPos = column.getStartingPos();
        long totalSize = column.getTotalSize();
        long totalValues = column.getValueCount();
        try (PageHeaderStream stream = source.openStream(startingPos, totalSize, column.getPath().toDotString())) {
            DictionaryPage dictPage = null;
            long valueCount = 0;
            List<PrefetchedPageReader.CompressedPage> pages = new ArrayList<>();
            // Loop on value count rather than byte count: total_compressed_size in the column
            // metadata occasionally undercounts actual on-disk page bytes (CRC/padding), so a
            // byte-based loop can spuriously continue into bytes that belong to the next column.
            // parquet-mr uses the same value-count termination in ColumnChunkPageReadStore.
            while (valueCount < totalValues) {
                PageHeader header = Util.readPageHeader(stream);
                int compressedPageSize = header.getCompressed_page_size();
                ByteBuffer payload = stream.readPayload(compressedPageSize);

                if (header.type == PageType.DICTIONARY_PAGE) {
                    if (dictPage != null) {
                        throw new IllegalStateException(
                            "Multiple dictionary pages found for column ["
                                + column.getPath().toDotString()
                                + "] in row group ["
                                + rowGroupOrdinal
                                + "]"
                        );
                    }
                    dictPage = makeDictionaryPage(header, payload);
                } else if (header.type == PageType.DATA_PAGE || header.type == PageType.DATA_PAGE_V2) {
                    DataPage page = makeDataPage(header, payload, primitiveType, -1L, -1);
                    // Sequential path: no offset index, so we cannot recover firstRowIndex per
                    // page. Pair with -1; PageColumnReader treats that as "page starts at the
                    // current cursor" and falls back to its rowRanges overlap check (which is
                    // why rowRanges must remain non-null on the sequential builder path).
                    pages.add(new PrefetchedPageReader.CompressedPage(page, -1L));
                    valueCount += page.getValueCount();
                }
            }
            return new PrefetchedPageReader(decompressor, pages, dictPage, valueCount);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Failed to read column [" + column.getPath().toDotString() + "] in row group [" + rowGroupOrdinal + "]: " + e.getMessage(),
                e
            );
        }
    }

    private static DictionaryPage readDictionaryPageIfPresent(
        ColumnChunkMetaData column,
        ColumnPageBytesSource source,
        int rowGroupOrdinal
    ) {
        if (column.hasDictionaryPage() == false) {
            return null;
        }
        long dictOffset = column.getDictionaryPageOffset();
        // The dictionary slice extends from dictOffset up to the first data page; the prefetcher
        // includes the whole [dictOffset, firstDataPageOffset) span so the slice is safe. We
        // always use getFirstDataPageOffset() rather than getStartingPos() because (a) when the
        // dictionary precedes the data, getStartingPos() returns the dictionary offset (which
        // would yield a 0-byte span here), and (b) we need the data-page boundary as the upper
        // bound of the dictionary slice regardless.
        long firstDataOffset = column.getFirstDataPageOffset();
        long dictSpan = firstDataOffset - dictOffset;
        if (dictSpan <= 0) {
            return null;
        }
        ByteBuffer dictSlice = source.slice(dictOffset, (int) dictSpan, column.getPath().toDotString(), -1);
        ByteArrayInputStream stream = bufferAsStream(dictSlice);
        try {
            int before = stream.available();
            PageHeader header = Util.readPageHeader(stream);
            int headerLen = before - stream.available();
            int compressedPageSize = header.getCompressed_page_size();
            ByteBuffer payload = sliceFromBuffer(dictSlice, headerLen, compressedPageSize);
            if (header.type != PageType.DICTIONARY_PAGE) {
                throw new IllegalStateException(
                    "Expected dictionary page at offset ["
                        + dictOffset
                        + "] for column ["
                        + column.getPath().toDotString()
                        + "] in row group ["
                        + rowGroupOrdinal
                        + "] but got ["
                        + header.type
                        + "]"
                );
            }
            return makeDictionaryPage(header, payload);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Failed to parse dictionary page for column ["
                    + column.getPath().toDotString()
                    + "] in row group ["
                    + rowGroupOrdinal
                    + "]",
                e
            );
        }
    }

    private static DataPage parseDataPageFromSlice(
        ByteBuffer pageSlice,
        PrimitiveType primitiveType,
        long firstRowIndex,
        int indexRowCount,
        String path,
        int rowGroupOrdinal
    ) {
        ByteArrayInputStream stream = bufferAsStream(pageSlice);
        try {
            int before = stream.available();
            PageHeader header = Util.readPageHeader(stream);
            int headerLen = before - stream.available();
            int compressedPageSize = header.getCompressed_page_size();
            ByteBuffer payload = sliceFromBuffer(pageSlice, headerLen, compressedPageSize);
            return makeDataPage(header, payload, primitiveType, firstRowIndex, indexRowCount);
        } catch (IOException e) {
            throw new UncheckedIOException(
                "Failed to parse page header for column [" + path + "] in row group [" + rowGroupOrdinal + "]: " + e.getMessage(),
                e
            );
        }
    }

    private static DataPage makeDataPage(
        PageHeader header,
        ByteBuffer payload,
        PrimitiveType primitiveType,
        long firstRowIndex,
        int indexRowCount
    ) {
        if (header.type == PageType.DATA_PAGE) {
            DataPageHeader dph = header.data_page_header;
            Encoding rl = METADATA_CONVERTER.getEncoding(dph.repetition_level_encoding);
            Encoding dl = METADATA_CONVERTER.getEncoding(dph.definition_level_encoding);
            Encoding values = METADATA_CONVERTER.getEncoding(dph.encoding);
            // Per-page statistics in the page header are not consumed downstream (PageColumnReader
            // and the optimized iterator only look at row-group level stats and the column index),
            // so building an empty Statistics is cheaper than parsing header.statistics.
            Statistics<?> stats = Statistics.getBuilderForReading(primitiveType).build();
            BytesInput compressed = bytesInputFrom(payload);
            if (firstRowIndex >= 0 && indexRowCount >= 0) {
                return new DataPageV1(
                    compressed,
                    dph.num_values,
                    header.uncompressed_page_size,
                    firstRowIndex,
                    indexRowCount,
                    stats,
                    rl,
                    dl,
                    values
                );
            }
            return new DataPageV1(compressed, dph.num_values, header.uncompressed_page_size, stats, rl, dl, values);
        }
        if (header.type == PageType.DATA_PAGE_V2) {
            DataPageHeaderV2 dph = header.data_page_header_v2;
            Encoding dataEnc = METADATA_CONVERTER.getEncoding(dph.encoding);
            // See note on per-page stats above (V1 branch).
            Statistics<?> stats = Statistics.getBuilderForReading(primitiveType).build();
            int rlBytes = dph.repetition_levels_byte_length;
            int dlBytes = dph.definition_levels_byte_length;
            int compressedPayloadSize = header.compressed_page_size;
            int dataCompressedSize = compressedPayloadSize - rlBytes - dlBytes;
            BytesInput rlInput = bytesInputFromSlice(payload, 0, rlBytes);
            BytesInput dlInput = bytesInputFromSlice(payload, rlBytes, dlBytes);
            BytesInput dataInput = bytesInputFromSlice(payload, rlBytes + dlBytes, dataCompressedSize);
            // DataPageV2's firstRowIndex-aware constructor is package-private and the
            // DataPageV2.uncompressed(... firstRowIndex ...) factory marks the page as
            // already-decompressed, which we need to defer until PrefetchedPageReader.decompressV2
            // runs. We therefore use the public constructor which drops firstRowIndex, and pair
            // each DataPage with its firstRowIndex via PrefetchedPageReader.CompressedPage so the
            // decompression step can splice it back in via DataPageV2.uncompressed(... firstRowIndex
            // ..., decompressedData, ...). PageColumnReader DOES consult page.getFirstRowIndex()
            // for V2 pages (it's defined on the DataPage base class), so without this pairing
            // the filtered builder path would silently drop all queued V2 pages whose
            // firstRowIndex > 0.
            return new DataPageV2(
                dph.num_rows,
                dph.num_nulls,
                dph.num_values,
                rlInput,
                dlInput,
                dataEnc,
                dataInput,
                header.uncompressed_page_size,
                stats,
                dph.is_compressed
            );
        }
        throw new IllegalStateException("Unexpected page type while building data page: " + header.type);
    }

    private static DictionaryPage makeDictionaryPage(PageHeader header, ByteBuffer payload) {
        DictionaryPageHeader dph = header.dictionary_page_header;
        Encoding encoding = METADATA_CONVERTER.getEncoding(dph.encoding);
        return new DictionaryPage(bytesInputFrom(payload), header.uncompressed_page_size, dph.num_values, encoding);
    }

    private static ColumnPageBytesSource sourceFor(
        ColumnChunkMetaData column,
        NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks,
        StorageObject storageObject,
        int rowGroupOrdinal
    ) {
        if (chunks != null) {
            return new PrefetchedSource(chunks, rowGroupOrdinal);
        }
        if (storageObject == null) {
            throw new IllegalStateException(
                "No prefetched data and no storage object available for column ["
                    + column.getPath().toDotString()
                    + "] in row group ["
                    + rowGroupOrdinal
                    + "]"
            );
        }
        return new StorageObjectSource(storageObject, rowGroupOrdinal);
    }

    /**
     * Source of column page bytes. Two implementations: prefetched (random-access slices into
     * heap buffers) and storage-object (sequential streaming with no random access).
     */
    private interface ColumnPageBytesSource {
        ByteBuffer slice(long fileOffset, int length, String columnPath, int pageIndex);

        boolean supportsRandomAccess();

        PageHeaderStream openStream(long fileOffset, long totalLength, String columnPath) throws IOException;
    }

    private static final class PrefetchedSource implements ColumnPageBytesSource {
        private final NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks;
        private final int rowGroupOrdinal;

        PrefetchedSource(NavigableMap<Long, ColumnChunkPrefetcher.PrefetchedChunk> chunks, int rowGroupOrdinal) {
            this.chunks = chunks;
            this.rowGroupOrdinal = rowGroupOrdinal;
        }

        @Override
        public ByteBuffer slice(long fileOffset, int length, String columnPath, int pageIndex) {
            Map.Entry<Long, ColumnChunkPrefetcher.PrefetchedChunk> entry = chunks.floorEntry(fileOffset);
            if (entry == null || entry.getValue().covers(fileOffset, length) == false) {
                throw new IllegalStateException(
                    "prefetched data does not cover offset="
                        + fileOffset
                        + " length="
                        + length
                        + " column="
                        + columnPath
                        + " page="
                        + pageIndex
                        + " rowGroup="
                        + rowGroupOrdinal
                );
            }
            ColumnChunkPrefetcher.PrefetchedChunk chunk = entry.getValue();
            int offsetInChunk = (int) (fileOffset - chunk.offset());
            ByteBuffer src = chunk.data().duplicate();
            src.position(offsetInChunk);
            src.limit(offsetInChunk + length);
            return src.slice();
        }

        @Override
        public boolean supportsRandomAccess() {
            return true;
        }

        @Override
        public PageHeaderStream openStream(long fileOffset, long totalLength, String columnPath) {
            ByteBuffer slice = slice(fileOffset, (int) totalLength, columnPath, -1);
            return new ByteBufferPageHeaderStream(slice);
        }
    }

    private static final class StorageObjectSource implements ColumnPageBytesSource {
        private final StorageObject storageObject;
        private final int rowGroupOrdinal;

        StorageObjectSource(StorageObject storageObject, int rowGroupOrdinal) {
            this.storageObject = storageObject;
            this.rowGroupOrdinal = rowGroupOrdinal;
        }

        @Override
        public ByteBuffer slice(long fileOffset, int length, String columnPath, int pageIndex) {
            // Filtered path requires random access into prefetched data; without prefetched chunks
            // we always take the sequential path and never hit this method.
            throw new UnsupportedOperationException(
                "StorageObjectSource does not support random-access slicing (column=" + columnPath + " rowGroup=" + rowGroupOrdinal + ")"
            );
        }

        @Override
        public boolean supportsRandomAccess() {
            return false;
        }

        @Override
        public PageHeaderStream openStream(long fileOffset, long totalLength, String columnPath) throws IOException {
            return new InputStreamPageHeaderStream(storageObject.newStream(fileOffset, totalLength), totalLength);
        }
    }

    /**
     * {@link InputStream} that supports a cheap {@link #readPayload(int)} for the data
     * immediately following a parsed {@code PageHeader}. The two implementations below either
     * slice an in-memory buffer directly (zero-copy) or copy from the upstream delegate into
     * a fresh heap buffer.
     */
    private abstract static class PageHeaderStream extends InputStream {
        abstract ByteBuffer readPayload(int length) throws IOException;
    }

    /**
     * Pure in-memory variant: wraps a prefetched buffer and reads page headers + payloads via
     * cursor advance, no I/O at all.
     */
    private static final class ByteBufferPageHeaderStream extends PageHeaderStream {
        private final ByteBuffer buffer;

        ByteBufferPageHeaderStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            if (buffer.hasRemaining() == false) {
                return -1;
            }
            return buffer.get() & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (len == 0) {
                return 0;
            }
            int available = buffer.remaining();
            if (available == 0) {
                return -1;
            }
            int toRead = Math.min(len, available);
            buffer.get(b, off, toRead);
            return toRead;
        }

        @Override
        public int available() {
            return buffer.remaining();
        }

        @Override
        ByteBuffer readPayload(int length) {
            if (buffer.remaining() < length) {
                throw new IllegalStateException("Need " + length + " payload bytes but only " + buffer.remaining() + " remain");
            }
            ByteBuffer slice = buffer.slice();
            slice.limit(length);
            buffer.position(buffer.position() + length);
            return slice;
        }

        @Override
        public void close() {}
    }

    /**
     * Sync fallback variant: reads page headers from the underlying input stream and copies the
     * subsequent payload into a heap buffer (we have no random-access view of the column).
     */
    private static final class InputStreamPageHeaderStream extends PageHeaderStream {
        private final InputStream delegate;
        private long remaining;

        InputStreamPageHeaderStream(InputStream delegate, long totalLength) {
            this.delegate = delegate;
            this.remaining = totalLength;
        }

        @Override
        public int read() throws IOException {
            int b = delegate.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = delegate.read(b, off, len);
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(Integer.MAX_VALUE, Math.max(0, remaining));
        }

        @Override
        ByteBuffer readPayload(int length) throws IOException {
            byte[] buf = new byte[length];
            int total = 0;
            while (total < length) {
                int n = delegate.read(buf, total, length - total);
                if (n < 0) {
                    throw new IOException("Unexpected end of stream after " + total + " of " + length + " payload bytes");
                }
                total += n;
            }
            remaining -= length;
            return ByteBuffer.wrap(buf);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    // ---------------- ByteBuffer / BytesInput helpers ----------------

    private static ByteArrayInputStream bufferAsStream(ByteBuffer buffer) {
        // Util.readPageHeader needs an InputStream; copying a small header-prefix is cheaper
        // than wrapping the buffer in a ByteBufferInputStream just to track available()/read().
        // We keep the buffer for the payload slice afterwards.
        if (buffer.hasArray()) {
            return new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        byte[] copy = new byte[buffer.remaining()];
        buffer.duplicate().get(copy);
        return new ByteArrayInputStream(copy);
    }

    private static ByteBuffer sliceFromBuffer(ByteBuffer source, int offsetFromPosition, int length) {
        ByteBuffer dup = source.duplicate();
        dup.position(source.position() + offsetFromPosition);
        dup.limit(dup.position() + length);
        return dup.slice();
    }

    private static BytesInput bytesInputFrom(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return BytesInput.from(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        byte[] copy = new byte[buffer.remaining()];
        buffer.duplicate().get(copy);
        return BytesInput.from(copy);
    }

    private static BytesInput bytesInputFromSlice(ByteBuffer source, int offsetFromPosition, int length) {
        if (length == 0) {
            return BytesInput.empty();
        }
        if (source.hasArray()) {
            return BytesInput.from(source.array(), source.arrayOffset() + source.position() + offsetFromPosition, length);
        }
        byte[] copy = new byte[length];
        ByteBuffer dup = source.duplicate();
        dup.position(source.position() + offsetFromPosition);
        dup.get(copy);
        return BytesInput.from(copy);
    }
}
