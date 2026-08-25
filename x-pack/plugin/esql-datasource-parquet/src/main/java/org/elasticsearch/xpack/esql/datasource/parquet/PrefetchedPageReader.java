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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.UninitializedArrays;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
 * <p>Compressed data pages decompress onto a grow-only heap {@code byte[]} via
 * {@link PlainCompressionCodecFactory.HeapDestDecompressor#decompressInto} when the codec
 * supports it. The dest's live capacity is charged to {@code breaker} until {@link #close()};
 * same-size pages do not re-charge. A grow charges the new size before dropping the old
 * charge so peak heap and breaker match while both arrays are live. Dictionary pages still
 * allocate via {@link BytesInputDecompressor#decompress(BytesInput, int)} and keep that
 * charge until close. Uncompressed pages alias the prefetched I/O bytes and are not charged
 * — those bytes are already accounted by the prefetch circuit breaker. Failed decompress
 * after a successful grow keeps the charge (the array is still referenced). A trip in
 * {@code addEstimateBytesAndMaybeBreak} leaves the previous array and charge unchanged.
 *
 * <p>{@link #readPage()} / {@link #readDictionaryPage()} run on the iterator thread.
 * {@link #close()} may race another {@link #close()} (hence {@link #closed}) and may race
 * a read on cancel. Charge counters are {@link java.util.concurrent.atomic.AtomicLong} so
 * that race cannot silently skew the breaker.
 */
final class PrefetchedPageReader implements PageReader, Releasable {

    private static final String DECOMP_BREAKER_LABEL = "parquet page decompression";

    /**
     * A compressed data page paired with its {@code firstRowIndex}. Required because
     * parquet-mr's public {@link DataPageV2} constructor does not accept {@code firstRowIndex}
     * (only the package-private one does), so we cannot rely on the {@link DataPage} carrying
     * it through. {@code firstRowIndex == -1} means "unknown"; {@link PageColumnReader} treats
     * an empty {@code getFirstRowIndex()} as "page starts at the current cursor".
     */
    record CompressedPage(DataPage page, long firstRowIndex) {}

    private final BytesInputDecompressor decompressor;
    private final PlainCompressionCodecFactory.HeapDestDecompressor heapDest;
    private final CircuitBreaker breaker;
    private final long valueCount;
    private final Deque<CompressedPage> compressedPages;
    private final DictionaryPage compressedDictionaryPage;

    private DictionaryPage cachedDictionaryPage;
    private boolean dictionaryDecompressed;
    /**
     * Grow-only dest for {@link PlainCompressionCodecFactory.HeapDestDecompressor}. Never shrunk.
     * Returned data-page {@link BytesInput}s alias this array for the life of the current page.
     */
    private byte[] reusableDecompBuf;
    /** Live capacity of {@link #reusableDecompBuf}, or the current allocating-decompress page. */
    private final AtomicLong dataPageCharge = new AtomicLong();
    private final AtomicLong dictCharge = new AtomicLong();
    // AtomicBoolean (rather than a plain volatile flag) so concurrent close() callers race
    // on a single compareAndSet and only one thread actually releases the breaker charge.
    private final AtomicBoolean closed = new AtomicBoolean();

    PrefetchedPageReader(
        BytesInputDecompressor decompressor,
        CircuitBreaker breaker,
        List<CompressedPage> compressedPages,
        DictionaryPage compressedDictionaryPage,
        long valueCount
    ) {
        this.decompressor = decompressor;
        this.heapDest = decompressor instanceof PlainCompressionCodecFactory.HeapDestDecompressor h ? h : null;
        this.breaker = breaker;
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
        if (closed.get()) {
            throw new ParquetDecodingException("PrefetchedPageReader closed");
        }
        CompressedPage entry = compressedPages.poll();
        if (entry == null) {
            // Queue drained. The last page's breaker charge stays until close(); there is no new
            // page to decode.
            return null;
        }
        // Previous page dest is overwritten on the reuse path. Consumers must be done with the
        // current page's bytes before they ask for the next one:
        // - PageColumnReader primitives/skip finish the current page before readPage(); BINARY
        // consumers copy into the ESQL block (appendBytesRef / constant deepCopy) before the
        // next ensurePage()/readPage(). The BytesInput returned here aliases reusableDecompBuf.
        // - parquet-mr's ColumnReaderBase (list columns, via ColumnReadStoreImpl) calls readPage()
        // only from checkRead() once the current page is fully consumed, re-initializes all of
        // its level/value readers from the new page before any further read, and its consumers
        // (ParquetColumnDecoding#readListRow) copy each value to the heap before the consume()
        // that can cross a page boundary.
        if (heapDest == null) {
            releaseCharge(dataPageCharge);
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
        int uncompressedSize = compressedDictionaryPage.getUncompressedSize();
        boolean charge = isNoopDecompressor() == false;
        if (charge) {
            breaker.addEstimateBytesAndMaybeBreak(uncompressedSize, DECOMP_BREAKER_LABEL);
        }
        boolean success = false;
        try {
            // Heap decompressor path so the returned BytesInput is a plain byte[] rather than an
            // alias of the prefetched chunk. DictionaryPageReader (parquet-mr) caches this
            // DictionaryPage indefinitely in a ConcurrentHashMap; if the decompressed bytes
            // aliased a prefetch buffer they would outlive this reader's close at row-group
            // rollover. The compressed input is also heap-backed —
            // PrefetchedRowGroupBuilder.makeDictionaryPage eagerly copies it from the
            // PrefetchedChunk. Uncompressed dictionaries skip the breaker charge and alias that
            // heap copy.
            BytesInput decompressed = decompressor.decompress(compressedDictionaryPage.getBytes(), uncompressedSize);
            cachedDictionaryPage = new DictionaryPage(
                decompressed,
                uncompressedSize,
                compressedDictionaryPage.getDictionarySize(),
                compressedDictionaryPage.getEncoding()
            );
            dictCharge.set(charge ? uncompressedSize : 0);
            // Charge is owned by dictCharge now; close() or releaseCharge below uncharges it.
            success = true;
            if (closed.get()) {
                releaseCharge(dictCharge);
                throw new ParquetDecodingException("PrefetchedPageReader closed");
            }
        } catch (IOException e) {
            throw new ParquetDecodingException("Could not decompress dictionary page", e);
        } finally {
            if (success == false && charge) {
                breaker.addWithoutBreaking(-uncompressedSize);
            }
        }
        // Set the cache flag only after a successful decompression. If decompression throws,
        // the next call will retry instead of silently returning a null cachedDictionaryPage.
        dictionaryDecompressed = true;
        return cachedDictionaryPage;
    }

    private DataPageV1 decompressV1(DataPageV1 v1) {
        try {
            BytesInput decompressed = decompressToHeap(v1.getBytes(), v1.getUncompressedSize());
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
            BytesInput decompressedData = decompressToHeap(v2.getData(), uncompressedDataSize);
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

    private BytesInput decompressToHeap(BytesInput compressed, int decompressedSize) throws IOException {
        // V1 has no isCompressed flag; NoopDecompressor is the only signal at this layer.
        // Alias I/O bytes and skip the breaker — prefetch already charged them. See #804.
        if (isNoopDecompressor()) {
            if (compressed.size() != decompressedSize) {
                throw new ParquetDecodingException(
                    "Uncompressed page size mismatch: input has "
                        + compressed.size()
                        + " bytes but page header declares "
                        + decompressedSize
                );
            }
            return compressed;
        }
        if (heapDest != null) {
            byte[] dest = ensureDecompCapacity(decompressedSize);
            try {
                heapDest.decompressInto(compressed, dest, decompressedSize);
            } catch (IOException e) {
                if (closed.get()) {
                    dropReusableDest();
                }
                throw e;
            }
            BytesInput decompressed = BytesInput.from(dest, 0, decompressedSize);
            if (closed.get()) {
                dropReusableDest();
                throw new ParquetDecodingException("PrefetchedPageReader closed");
            }
            return decompressed;
        }
        breaker.addEstimateBytesAndMaybeBreak(decompressedSize, DECOMP_BREAKER_LABEL);
        boolean success = false;
        try {
            BytesInput decompressed = decompressor.decompress(compressed, decompressedSize);
            dataPageCharge.set(decompressedSize);
            // Charge is owned by dataPageCharge now; close() or releaseCharge below uncharges it.
            success = true;
            if (closed.get()) {
                releaseCharge(dataPageCharge);
                throw new ParquetDecodingException("PrefetchedPageReader closed");
            }
            return decompressed;
        } finally {
            if (success == false) {
                breaker.addWithoutBreaking(-decompressedSize);
            }
        }
    }

    private byte[] ensureDecompCapacity(int needed) {
        byte[] buf = reusableDecompBuf;
        if (buf != null && buf.length >= needed) {
            return buf;
        }
        // Charge the new size on top of any live old charge so breaker tracks peak heap while
        // both arrays exist; drop the old charge after the new array is published.
        breaker.addEstimateBytesAndMaybeBreak(needed, DECOMP_BREAKER_LABEL);
        byte[] next;
        try {
            next = UninitializedArrays.newByteArray(needed);
        } catch (Error | RuntimeException e) {
            breaker.addWithoutBreaking(-needed);
            throw e;
        }
        reusableDecompBuf = next;
        long previous = dataPageCharge.getAndSet(needed);
        if (previous != 0) {
            breaker.addWithoutBreaking(-previous);
        }
        if (closed.get()) {
            dropReusableDest();
            throw new ParquetDecodingException("PrefetchedPageReader closed");
        }
        return next;
    }

    private void dropReusableDest() {
        reusableDecompBuf = null;
        releaseCharge(dataPageCharge);
    }

    private boolean isNoopDecompressor() {
        return decompressor instanceof PlainCompressionCodecFactory.NoopDecompressor;
    }

    private void releaseCharge(AtomicLong charge) {
        long bytes = charge.getAndSet(0);
        if (bytes != 0) {
            breaker.addWithoutBreaking(-bytes);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        // Drop the cached dictionary page reference. It is heap-backed (see readDictionaryPage),
        // so this is reference hygiene plus breaker release, not a native-buffer lifetime.
        cachedDictionaryPage = null;
        dropReusableDest();
        releaseCharge(dictCharge);
    }
}
