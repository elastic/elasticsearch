/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.io.ParquetDecodingException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.datasources.spi.DirectMemoryDebug;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
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
 * <p>Decompression buffers are allocated as {@link ArrowBuf}s from the supplied
 * {@link BufferAllocator}, exposed to the codec via {@link ArrowBuf#nioBuffer(long, int)} to
 * preserve the direct-to-direct JNI fast path. The reader owns these buffers; {@link #close()}
 * releases them back to the allocator (which routes the accounting through the circuit breaker).
 * The {@link DataPage}s and {@link BytesInput}s returned from {@link #readPage()} alias these
 * buffers and must not be used after the reader is closed.
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
    private final BufferAllocator allocator;
    private final long valueCount;
    private final Deque<CompressedPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;
    private final List<Releasable> ownedBuffers = new ArrayList<>();

    private DictionaryPage cachedDictionaryPage;
    private boolean dictionaryDecompressed;
    // AtomicBoolean (rather than a plain volatile flag) so concurrent close() callers race
    // on a single compareAndSet and only one thread actually releases the owned buffers.
    private final AtomicBoolean closed = new AtomicBoolean();

    PrefetchedPageReader(
        BytesInputDecompressor decompressor,
        BufferAllocator allocator,
        List<CompressedPage> compressedPages,
        DictionaryPage compressedDictionaryPage,
        long valueCount
    ) {
        this.decompressor = decompressor;
        this.allocator = allocator;
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
        // synchronously, so it is released in the finally below rather than registered with the
        // reader — otherwise it would sit in ownedBuffers for the rest of the row group.
        ArrowBuf scratch = null;
        try {
            if (input.isDirect() == false) {
                scratch = allocator.buffer(input.remaining());
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
     * Allocate a direct {@link ByteBuffer} of the requested size, backed by an {@link ArrowBuf}
     * owned by this reader. The buffer is breaker-accounted via the allocator's listener and is
     * released on {@link #close()}.
     */
    private ByteBuffer allocateDirect(int size) {
        ArrowBuf buf = allocator.buffer(size);
        // Use the explicit (index, length) overload: ArrowBuf may round capacity up, but the
        // codec's size sanity check expects remaining() == declared decompressed size.
        ByteBuffer view = buf.nioBuffer(0, size);
        // Poison the region just before release (assertions only) so a decompressed-page BytesInput
        // that aliases this buffer and is read after the reader is closed fails deterministically.
        ownedBuffers.add(() -> {
            DirectMemoryDebug.poison(view);
            buf.close();
        });
        return view;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        // Drop the cached dictionary BytesInput; it aliases an ArrowBuf we're about to release.
        cachedDictionaryPage = null;
        try {
            Releasables.close(ownedBuffers);
        } finally {
            ownedBuffers.clear();
        }
    }
}
