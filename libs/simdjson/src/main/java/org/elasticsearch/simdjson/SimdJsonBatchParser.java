/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.internal.BitIndexes;
import org.elasticsearch.simdjson.internal.StructuralIndexer;

/**
 * Batch-aware JSON parser that runs native stage 1 (structural indexing + UTF-8 validation)
 * over a contiguous buffer containing multiple JSON documents, then provides per-document
 * structural index windows for direct walking by {@link SimdJsonDirectWalker}.
 *
 * <p>Stage 1 is run in chunks of at most {@link #CHUNK_BYTE_LIMIT} bytes to keep the
 * structural index working set in L2 cache. When the caller processes documents in offset
 * order, chunks are materialized lazily — stage 1 is re-run only when a document falls
 * outside the currently-indexed byte range.
 *
 * <p>Each instance owns a {@link StructuralIndexer} that delegates stage 1 to the native
 * simdjson C++ library. The indexer is created at construction time and released when this
 * parser is {@linkplain #close() closed}.
 *
 * <p><strong>Usage (chunked — preferred for large batches):</strong>
 * <pre>{@code
 *   try (SimdJsonBatchParser batch = new SimdJsonBatchParser(256 * 1024)) {
 *       SimdJsonDirectWalker walker = new SimdJsonDirectWalker(nameTable.makeChild());
 *       batch.beginBatch(buffer, totalLen);
 *       for (int i = 0; i < docCount; i++) {
 *           batch.prepareDocumentWindowChunked(docOffsets[i], docLens[i]);
 *           walker.walkDocument(buffer, docLens[i], batch, handler);
 *       }
 *       walker.releaseNames();
 *   }
 * }</pre>
 *
 * <p><strong>Usage (explicit stage 1 — for single-doc or pre-indexed bodies):</strong>
 * <pre>{@code
 *   batch.stage1(buffer, offset, len);
 *   batch.prepareDocumentWindow(0, len);
 *   walker.walkDocument(buffer, len, batch, handler);
 *   walker.releaseNames();
 * }</pre>
 *
 * <p><strong>No trailing padding required.</strong> All code paths — native stage 1, the string
 * parser, field name resolution, and field name hashing — have scalar tail fallbacks that
 * stay within buffer bounds. Callers may pass exact-length buffers.
 *
 * <p><strong>Not thread-safe.</strong> Each thread must own its own instance.
 */
public class SimdJsonBatchParser implements AutoCloseable {

    /**
     * Maximum number of bytes to index in a single stage 1 pass. Smaller values keep the
     * structural index working set in L1/L2 cache; larger values amortize native FFI overhead.
     * On ARM NEON (64 KB L1d), values around 32–64 KB may outperform the default 256 KB.
     * Override at startup with {@code -Des.simdjson.chunk_byte_limit=65536}.
     */
    public static final int CHUNK_BYTE_LIMIT = Integer.getInteger("es.simdjson.chunk_byte_limit", 256 * 1024);

    @FunctionalInterface
    interface Stage1Function {
        void index(byte[] buffer, int offset, int len, BitIndexes bitIndexes);
    }

    private final BitIndexes bitIndexes;
    private final Stage1Function stage1Function;
    private final StructuralIndexer indexer;

    private boolean stage1Done;
    private int savedSentinelPos = -1;
    private int savedSentinelValue;
    private int nextSearchFrom;

    private byte[] batchBuffer;
    private int batchTotalLen;
    private int indexedRangeStart;
    private int indexedRangeEnd;

    /**
     * Creates a batch parser backed by a native {@link StructuralIndexer}.
     *
     * @param capacity maximum total batch size in bytes (sum of all documents)
     */
    public SimdJsonBatchParser(int capacity) {
        SimdJsonSupport.isSupported();
        int indexCapacity = Math.max(capacity / 4, 1024);
        bitIndexes = new BitIndexes(indexCapacity);
        indexer = new StructuralIndexer(capacity);
        stage1Function = indexer::index;
    }

    /**
     * Package-private constructor for testing with a custom stage 1 implementation
     * (e.g. a scalar fallback that doesn't require the native library).
     */
    SimdJsonBatchParser(int capacity, Stage1Function stage1Function) {
        int indexCapacity = Math.max(capacity / 4, 1024);
        bitIndexes = new BitIndexes(indexCapacity);
        this.indexer = null;
        this.stage1Function = stage1Function;
    }

    /**
     * Runs stage 1 (SIMD structural indexing + UTF-8 validation) over
     * {@code buffer[0..len)}. Equivalent to {@code stage1(buffer, 0, len)}.
     *
     * <p>The buffer must have at least 64 bytes of readable space past {@code len}.
     *
     * @see #stage1(byte[], int, int)
     */
    public void stage1(byte[] buffer, int len) {
        stage1(buffer, 0, len);
    }

    /**
     * Runs stage 1 (SIMD structural indexing + UTF-8 validation) over
     * {@code buffer[offset..offset+len)}.
     *
     * <p>The structural indices are stored as absolute positions within {@code buffer}
     * (i.e. they include {@code offset}).
     *
     * <p>After this call, use {@link #prepareDocumentWindow} to set up per-document windows.
     *
     * @param buffer the source buffer
     * @param offset start offset of the JSON data
     * @param len    length of the JSON data in bytes
     */
    public void stage1(byte[] buffer, int offset, int len) {
        stage1Function.index(buffer, offset, len, bitIndexes);
        this.stage1Done = true;
        this.nextSearchFrom = 0;
        this.savedSentinelPos = -1;
        this.indexedRangeStart = offset;
        this.indexedRangeEnd = offset + len;
        this.batchBuffer = buffer;
        this.batchTotalLen = offset + len;
    }

    /**
     * Prepares a batch for chunked processing. No stage 1 is run immediately — it will be
     * triggered lazily by {@link #prepareDocumentWindowChunked} when the first document is
     * requested. Documents must be processed in ascending offset order.
     *
     * @param buffer   the contiguous buffer holding all documents
     * @param totalLen total valid length within the buffer (documents span {@code [0..totalLen)})
     */
    public void beginBatch(byte[] buffer, int totalLen) {
        beginBatch(buffer, 0, totalLen);
    }

    /**
     * Like {@link #beginBatch(byte[], int)} but for a sub-range of the buffer.
     */
    public void beginBatch(byte[] buffer, int offset, int totalLen) {
        this.batchBuffer = buffer;
        this.batchTotalLen = offset + totalLen;
        this.indexedRangeStart = offset;
        this.indexedRangeEnd = offset;
        this.stage1Done = false;
        this.savedSentinelPos = -1;
        this.nextSearchFrom = 0;
    }

    /**
     * Prepares the structural index window for the document at
     * {@code buffer[docOffset..docOffset+docLen)}, automatically running stage 1
     * on a new chunk if the document falls outside the currently-indexed range.
     *
     * <p>Documents must be prepared in ascending offset order.
     *
     * @param docOffset byte offset of the document start within the batch buffer
     * @param docLen    length of the document in bytes (must not exceed {@link #CHUNK_BYTE_LIMIT})
     */
    public void prepareDocumentWindowChunked(int docOffset, int docLen) {
        if (docLen > CHUNK_BYTE_LIMIT) {
            throw new IllegalArgumentException("docLen [" + docLen + "] exceeds CHUNK_BYTE_LIMIT [" + CHUNK_BYTE_LIMIT + "]");
        }
        int docEnd = docOffset + docLen;
        if (stage1Done == false || docOffset >= indexedRangeEnd || docEnd > indexedRangeEnd) {
            indexChunkAt(docOffset);
        }
        prepareDocumentWindow(docOffset, docLen);
    }

    /**
     * Runs stage 1 over a chunk starting at {@code chunkStart}. The chunk extends up to
     * {@code CHUNK_BYTE_LIMIT} bytes or the end of the batch, whichever comes first.
     */
    private void indexChunkAt(int chunkStart) {
        restoreSentinel();
        int remaining = batchTotalLen - chunkStart;
        int chunkLen = Math.min(remaining, CHUNK_BYTE_LIMIT);
        stage1Function.index(batchBuffer, chunkStart, chunkLen, bitIndexes);
        this.stage1Done = true;
        this.nextSearchFrom = 0;
        this.savedSentinelPos = -1;
        this.indexedRangeStart = chunkStart;
        this.indexedRangeEnd = chunkStart + chunkLen;
    }

    /**
     * Prepares the structural index window for the document at
     * {@code buffer[docOffset..docOffset+docLen)}. Stage 1 must have been run first
     * via {@link #stage1(byte[], int, int)}. After this call, the document can be
     * walked with {@link SimdJsonDirectWalker#walkDocument}.
     *
     * <p>Documents must be prepared in ascending offset order.
     *
     * @param docOffset byte offset of the document start within the batch buffer
     * @param docLen    length of the document in bytes
     */
    public void prepareDocumentWindow(int docOffset, int docLen) {
        if (!stage1Done) {
            throw new IllegalStateException("stage1() must be called before prepareDocumentWindow()");
        }

        restoreSentinel();

        int totalIndices = bitIndexes.writeCount();
        int from = bitIndexes.findFirstIndexAtOrAfter(nextSearchFrom, docOffset);
        int docEnd = docOffset + docLen;

        int to = from;
        while (to < totalIndices && bitIndexes.getIndexAt(to) < docEnd) {
            to++;
        }
        nextSearchFrom = to;

        if (to <= totalIndices) {
            savedSentinelPos = to;
            savedSentinelValue = (to < totalIndices) ? bitIndexes.getIndexAt(to) : 0;
            bitIndexes.writeSentinel(to, bitIndexes.getIndexAt(from));
        } else {
            savedSentinelPos = -1;
        }

        bitIndexes.setReadWindow(from, to);
    }

    /**
     * Returns the underlying {@link BitIndexes} for direct access by the walker.
     */
    BitIndexes bitIndexes() {
        return bitIndexes;
    }

    /** Restores any sentinel that was written by a previous {@link #prepareDocumentWindow} call. */
    private void restoreSentinel() {
        if (savedSentinelPos >= 0) {
            bitIndexes.writeSentinel(savedSentinelPos, savedSentinelValue);
            savedSentinelPos = -1;
        }
    }

    /**
     * Ensures the internal {@link BitIndexes} can hold at least {@code minCapacity} entries.
     * Used internally when the batch buffer is larger than the initial capacity estimate.
     */
    void ensureIndexCapacity(int minCapacity) {
        bitIndexes.ensureCapacity(minCapacity);
    }

    @Override
    public void close() {
        if (indexer != null) {
            indexer.close();
        }
    }
}
