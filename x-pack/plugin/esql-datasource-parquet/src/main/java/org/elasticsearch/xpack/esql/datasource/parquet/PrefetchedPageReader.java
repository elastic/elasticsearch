/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.io.ParquetDecodingException;
import org.elasticsearch.compute.data.arrow.DirectBuffers;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

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
 *
 * <p>Decompression uses one reusable {@link ArrowBuf} borrowed from the supplied
 * {@link DirectBuffers}, exposed via {@link ArrowBuf#nioBuffer(long, int)} for the
 * direct-to-direct JNI fast path. {@link #readPage()} overwrites that buffer. It grows only
 * when a later page needs more capacity — breaker residency is the high-water-mark page, not
 * the current page — and is returned to the pool on {@link #close()} so the next query can
 * reuse it without a malloc. Heap-backed compressed input still copies through a per-page
 * scratch buffer; production prefetch already yields direct slices, so that path is idle.
 * Returned {@link DataPage}s and {@link BytesInput}s alias the current buffer and must not be
 * used after the next {@link #readPage()} or {@link #close()}.
 */
final class PrefetchedPageReader implements PageReader, Releasable {

    /**
     * A compressed data page paired with its {@code firstRowIndex}. Required because
     * parquet-mr's public {@link DataPageV2} constructor does not accept {@code firstRowIndex}
     * (only the package-private one does), so we cannot rely on the {@link DataPage} carrying
     * it through. {@code firstRowIndex == -1} means "unknown"; {@link PageColumnReader} treats
     * an empty {@code getFirstRowIndex()} as "page starts at the current cursor".
     */
    record CompressedPage(DataPage page, long firstRowIndex) {}

    private final BytesInputDecompressor decompressor;
    private final DirectBuffers buffers;
    private final long valueCount;
    private final Deque<CompressedPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    // Reusable decompress-output buffer. Grown when a page needs more capacity; returned to
    // buffers.pool() on close() so the next query reuses it. Overwritten in place across pages
    // so we do not malloc/free per page — that churn is what retained glibc arenas after the
    // allocator balance had already returned to baseline.
    private ArrowBuf reusableDecompBuf;

    private DictionaryPage cachedDictionaryPage;
    private boolean dictionaryDecompressed;
    // AtomicBoolean (rather than a plain volatile flag) so concurrent close() callers race
    // on a single compareAndSet and only one thread actually releases the owned buffers.
    private final AtomicBoolean closed = new AtomicBoolean();

    PrefetchedPageReader(
        BytesInputDecompressor decompressor,
        DirectBuffers buffers,
        List<CompressedPage> compressedPages,
        DictionaryPage compressedDictionaryPage,
        long valueCount
    ) {
        this.decompressor = decompressor;
        this.buffers = Objects.requireNonNull(buffers, "buffers");
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
            // Queue drained. The last page returned stays live (the consumer may still be decoding
            // it) until close() releases the reusable buffer; there is no new page to decode.
            return null;
        }
        // Previous page is dead. Both consumers of this reader ask for pages strictly sequentially
        // and are done with the current page's bytes before they ask for the next one:
        // - PageColumnReader#loadNextPage (flat columns) runs its remainder-skip off the current
        // value/def-level buffers BEFORE calling readPage(), and reassigns those buffers to the
        // new page immediately after;
        // - parquet-mr's ColumnReaderBase (list columns, via ColumnReadStoreImpl) calls readPage()
        // only from checkRead() once the current page is fully consumed, re-initializes all of
        // its level/value readers from the new page before any further read, and its consumers
        // (ParquetColumnDecoding#readListRow) copy each value to the heap before the consume()
        // that can cross a page boundary.
        // Overwrite the reusable decompress buffer rather than free+malloc: per-page free fed
        // glibc arena retention even when the allocator balance returned to baseline. Live
        // working set stays O(one page); parking a new buffer per page until close() is the
        // accumulation the OOM-killer saw.
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
            // Use the heap decompressor path (not decompressToDirectBuffer) so the
            // returned BytesInput is backed by a plain byte[] rather than an ArrowBuf.
            // DictionaryPageReader (parquet-mr) caches this DictionaryPage indefinitely
            // in a ConcurrentHashMap; if the decompressed bytes aliased an ArrowBuf they
            // would become dangling as soon as the owning PrefetchedPageReader is closed
            // at row-group rollover. The compressed input (compressedDictionaryPage.getBytes())
            // is also heap-backed — PrefetchedRowGroupBuilder.makeDictionaryPage eagerly
            // copies it from the PrefetchedChunk's direct buffer, which can be released
            // before this call. Dictionary pages are small; neither copy is performance-critical.
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
            BytesInput decompressed = decompressToDirectBuffer(v1.getBytes(), v1.getUncompressedSize());
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
            if (uncompressedDataSize == 0) {
                // Spark's Parquet writer stores all-null V2 pages with an empty data buffer
                // rather than a compressed representation of zero bytes. Decompression libraries
                // (Snappy, Zstd, ...) reject empty input, so skip decompression entirely.
                return DataPageV2.uncompressed(
                    v2.getRowCount(),
                    v2.getNullCount(),
                    v2.getValueCount(),
                    firstRowIndex,
                    v2.getRepetitionLevels(),
                    v2.getDefinitionLevels(),
                    v2.getDataEncoding(),
                    BytesInput.empty(),
                    v2.getStatistics()
                );
            }
            BytesInput decompressedData = decompressToDirectBuffer(v2.getData(), uncompressedDataSize);
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

    /**
     * Decompresses {@code compressed} into a direct {@link ByteBuffer}, then wraps the result
     * as a {@link BytesInput}. Both the input and output sides are direct so each codec takes its
     * direct-to-direct JNI fast path (Zstd: {@code decompressDirectByteBuffer}; Snappy:
     * {@code Snappy.uncompress(ByteBuffer, ByteBuffer)}), avoiding
     * {@code GetPrimitiveArrayCritical} JNI pinning and the G1GC evacuation failures it causes.
     *
     * <p>For the prefetched path, {@link ColumnChunkPrefetcher} promotes each S3 response buffer
     * from heap to direct once at fetch time (one copy per coalesced range), so page slices
     * derived from it are already direct and the conditional copy below is a no-op. The copy
     * remains as a safety net for the non-prefetched sync fallback path.
     *
     * <p>Uncompressed Parquet files take a short-circuit: when the decompressor is the pass-through
     * {@link PlainCompressionCodecFactory.NoopDecompressor} and the input slice is already direct,
     * the input is returned as-is. This avoids one {@code ByteBuffer.allocateDirect(pageSize)} plus
     * a full page memcopy per V1 data page (and per dictionary page) — wasted work since
     * {@code NoopDecompressor} would just copy the input into the output buffer verbatim. DataPageV2
     * already has its own {@code isCompressed()=false} early exit upstream of this method; V1 has no
     * equivalent flag in the page header, so the marker check on the decompressor instance is the
     * only signal we have at the page-read layer. See elastic/esql-planning#804.
     */
    private BytesInput decompressToDirectBuffer(BytesInput compressed, int decompressedSize) throws IOException {
        ByteBuffer input = compressed.toByteBuffer();
        if (decompressor instanceof PlainCompressionCodecFactory.NoopDecompressor && input.isDirect()) {
            if (input.remaining() != decompressedSize) {
                throw new ParquetDecodingException(
                    "Uncompressed page size mismatch: input has "
                        + input.remaining()
                        + " bytes but page header declares "
                        + decompressedSize
                );
            }
            return BytesInput.from(input);
        }
        // Scratch buffer used only when the input is on the heap and must be copied to direct
        // memory to take the codec's direct-to-direct JNI fast path. decompress() consumes it
        // synchronously, so it is released in the finally below rather than held on the reader.
        ArrowBuf scratch = null;
        try {
            if (input.isDirect() == false) {
                scratch = buffers.buffer(input.remaining());
                ByteBuffer directInput = scratch.nioBuffer(0, input.remaining());
                directInput.put(input);
                directInput.flip();
                input = directInput;
            }
            ByteBuffer output = allocateDirect(decompressedSize);
            decompressor.decompress(input, Math.toIntExact(compressed.size()), output, decompressedSize);
            output.flip();
            return BytesInput.from(output);
        } finally {
            if (scratch != null) {
                scratch.close();
            }
        }
    }

    /**
     * Direct decompress output. Explicit {@code (index, length)}: ArrowBuf may round capacity up,
     * but the codec's size sanity check expects {@code remaining() ==} declared decompressed size.
     */
    private ByteBuffer allocateDirect(int size) {
        ArrowBuf current = reusableDecompBuf;
        if (current != null && current.capacity() >= size) {
            return current.nioBuffer(0, size);
        }
        reusableDecompBuf = null;
        if (current != null) {
            // Known undersized: close it. Returning it then immediately borrowing a larger
            // size would poll the same buf and free it anyway, and would also close other
            // idle undersized buffers still useful to other columns.
            current.close();
        }
        reusableDecompBuf = buffers.borrow(size);
        return reusableDecompBuf.nioBuffer(0, size);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        // Drop the cached dictionary page reference. It is deliberately heap-backed (see
        // readDictionaryPage), so this is reference hygiene, not a buffer-lifetime requirement.
        cachedDictionaryPage = null;
        ArrowBuf buf = reusableDecompBuf;
        reusableDecompBuf = null;
        buffers.returnBuf(buf);
    }
}
