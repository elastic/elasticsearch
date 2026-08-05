/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.codec.zstd.ZstdCompressionMode;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_ALL_SINGLE_SLOT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_DOCS_CONTIGUOUS;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_VALUES_COMPRESSED;

/**
 * Writes one column (sub-field) directly into an existing {@link IndexOutput}.
 *
 * <p>Blocks are written straight into the caller-supplied output — no temp files, no splice.
 * The caller retains ownership of the output; {@link #close()} is a no-op.
 *
 * <p>One instance is reused for every column of a field: call {@link #reset(IndexOutput)} at each column
 * boundary rather than allocating a new writer. The block-accumulation arrays are pre-sized from
 * the configured thresholds (~{@code 2 * 4 * maxDocsPerBlock + targetBlockBytes} bytes) and the
 * ZSTD compressor is stateful to allocate, so allocating per column turned into hundreds of
 * megabytes of short-lived garbage per flush on fields with a large sub-field cardinality.
 *
 * <h2>On-disk column structure</h2>
 *
 * <pre>
 * [ block 0 ][ block 1 ] ... [ block N-1 ]
 * [int firstDocId, int blockStartRelative] x N   — block index, 8 bytes/entry
 * </pre>
 *
 * <p>{@code blockStartRelative} is the byte offset of the block from the start of this column.
 */
final class FieldBlockWriter implements Closeable {

    private final int targetBlockBytes;
    private final int maxDocsPerBlock;
    private final int minCompressBytes;
    private final Compressor compressor = new ZstdCompressionMode(1).newCompressor();

    /**
     * Scratch buffer for the docId-delta / slot-count prefix written before the value payload
     * inside the compressed region. Upper-bound per block: 10 * maxDocsPerBlock bytes
     * (5 bytes/vint × (numDocs-1) deltas + 5 bytes/vint × numDocs slot counts).
     */
    private byte[] prefixBuf;

    /** Absolute data-file offset of the first block for this column. Updated by {@link #reset}. */
    private long columnStartOffset;
    /** The externally-owned output this column is being written into. */
    private IndexOutput currentOut;

    // Current block accumulation. Pre-sized to the configured thresholds so that typical blocks
    // require no grows; ArrayUtil.grow handles the overshoot case (one doc's payload past the limit).
    private int[] blockDocIds;
    private int[] blockSlotCounts;
    private byte[] blockPayload;
    private int numDocsInBlock = 0;
    private int blockPayloadLen = 0;

    // Block index accumulated across all flushed blocks (8 bytes/entry).
    private int[] blockFirstDocIds = new int[8];
    private int[] blockStartsRelative = new int[8];
    private int numBlocks = 0;
    /** Total bytes written so far for block data (= block-index base). */
    int totalBlockBytes = 0;

    // Sizing hints for the reader.
    int maxUncompressedBlockLen = 0;
    int maxDocsPerBlockSeen = 0;

    private boolean finished = false;

    /**
     * Constructs a writer that emits blocks directly into {@code out}. No temp files are created;
     * {@link #close()} is a no-op. The caller must keep {@code out} open until after
     * {@link #finish()} returns.
     *
     * <p>Prefer allocating one instance and calling {@link #reset(IndexOutput)} at each column
     * boundary to avoid per-column array and compressor allocation.
     */
    FieldBlockWriter(IndexOutput out, int targetBlockBytes, int maxDocsPerBlock, int minCompressBytes) {
        this.targetBlockBytes = targetBlockBytes;
        this.maxDocsPerBlock = maxDocsPerBlock;
        this.minCompressBytes = minCompressBytes;
        this.columnStartOffset = out.getFilePointer();
        this.currentOut = out;
        this.blockDocIds = new int[maxDocsPerBlock];
        this.blockSlotCounts = new int[maxDocsPerBlock];
        this.blockPayload = new byte[targetBlockBytes];
        this.prefixBuf = new byte[10 * maxDocsPerBlock];
    }

    /**
     * Resets this writer to start a new column in {@code out}.
     *
     * <p>Must only be called immediately after {@link #finish()} has been called (or before any
     * slots have been added for the very first column). Reuses all pre-allocated arrays and the
     * ZSTD compressor from the previous column.
     */
    void reset(IndexOutput out) {
        assert finished : "reset() called on an unfinished writer";
        this.currentOut = out;
        this.columnStartOffset = out.getFilePointer();
        this.numDocsInBlock = 0;
        this.blockPayloadLen = 0;
        this.numBlocks = 0;
        this.totalBlockBytes = 0;
        this.maxUncompressedBlockLen = 0;
        this.maxDocsPerBlockSeen = 0;
        this.finished = false;
    }

    /**
     * Bulk-appends all slots for {@code docId} from an already-encoded payload region.
     *
     * <p>The payload slice must contain exactly {@code slotCount} encoded slots in the columnar
     * {@code [vint prefix][value bytes]} framing (prefix 0 = null, prefix N+1 = N value bytes).
     * The block-flush trigger fires before appending, at the doc boundary, ensuring that no
     * document's slots are ever split across two blocks.
     *
     * @param docId      the target document ID (must be ≥ the last docId passed to this writer)
     * @param slotCount  number of slots in the payload slice
     * @param payload    source byte array containing the encoded slots
     * @param payloadOff offset within {@code payload} where the slots begin
     * @param payloadLen total byte length of all slots for this document
     */
    void addDocSlots(int docId, int slotCount, byte[] payload, int payloadOff, int payloadLen) throws IOException {
        if (numDocsInBlock > 0 && (numDocsInBlock >= maxDocsPerBlock || blockPayloadLen >= targetBlockBytes)) {
            flushCurrentBlock();
        }

        blockDocIds = ArrayUtil.grow(blockDocIds, numDocsInBlock + 1);
        blockSlotCounts = ArrayUtil.grow(blockSlotCounts, numDocsInBlock + 1);
        blockDocIds[numDocsInBlock] = docId;
        blockSlotCounts[numDocsInBlock] = slotCount;
        numDocsInBlock++;

        blockPayload = ArrayUtil.grow(blockPayload, blockPayloadLen + payloadLen);
        System.arraycopy(payload, payloadOff, blockPayload, blockPayloadLen, payloadLen);
        blockPayloadLen += payloadLen;
    }

    /**
     * Finishes the column: flushes the tail block and appends the block index (8 bytes per entry).
     * Must be called exactly once, after all slots have been added.
     */
    void finish() throws IOException {
        assert finished == false : "finish() called twice";
        flushCurrentBlock();
        for (int b = 0; b < numBlocks; b++) {
            currentOut.writeInt(blockFirstDocIds[b]);
            currentOut.writeInt(blockStartsRelative[b]);
        }
        finished = true;
    }

    /**
     * Returns the {@link ColumnAddress} describing this column's on-disk position.
     * Must be called after {@link #finish()}.
     */
    ColumnAddress columnAddress() {
        assert finished : "columnAddress() requires finish()";
        return new ColumnAddress(columnStartOffset, totalBlockBytes, numBlocks);
    }

    /**
     * Describes the on-disk position of a written column.
     *
     * @param columnStartOffset        absolute data-file offset of the first block
     * @param blockIndexRelativeOffset byte offset of the block index from {@code columnStartOffset}
     * @param numBlocks                number of blocks
     */
    record ColumnAddress(long columnStartOffset, int blockIndexRelativeOffset, int numBlocks) {}

    /** No-op: the output is externally owned. */
    @Override
    public void close() {}

    private void flushCurrentBlock() throws IOException {
        if (numDocsInBlock == 0) return;

        maxDocsPerBlockSeen = Math.max(maxDocsPerBlockSeen, numDocsInBlock);

        blockFirstDocIds = ArrayUtil.grow(blockFirstDocIds, numBlocks + 1);
        blockStartsRelative = ArrayUtil.grow(blockStartsRelative, numBlocks + 1);
        blockFirstDocIds[numBlocks] = blockDocIds[0];
        blockStartsRelative[numBlocks] = totalBlockBytes;

        final long blockStart = currentOut.getFilePointer();

        boolean contiguous = true;
        for (int i = 1; i < numDocsInBlock && contiguous; i++) {
            if (blockDocIds[i] != blockDocIds[i - 1] + 1) contiguous = false;
        }
        boolean allSingleSlot = true;
        for (int i = 0; i < numDocsInBlock && allSingleSlot; i++) {
            if (blockSlotCounts[i] != 1) allSingleSlot = false;
        }

        // Build the combined payload: [docDelta × (n-1)][slotCount × n][value bytes].
        // The docId and slot-count arrays are now inside the (optionally) compressed region so
        // that ZSTD can exploit their redundancy alongside the value bytes. The old layout wrote
        // them raw before the compression boundary, which made them incompressible overhead.
        //
        // We encode the prefix into the separate prefixBuf scratch buffer, then shift the value
        // payload right in blockPayload to make room, and copy the prefix in. This keeps a single
        // contiguous buffer for the compressor.
        // Upper-bound: (numDocs-1) * 5 + numDocs * 5 = 10 * numDocs bytes.
        if (prefixBuf.length < 10 * numDocsInBlock) {
            prefixBuf = new byte[10 * numDocsInBlock];
        }
        int prefixLen = 0;
        if (contiguous == false) {
            for (int i = 1; i < numDocsInBlock; i++) {
                prefixLen = writeVIntToArray(prefixBuf, prefixLen, blockDocIds[i] - blockDocIds[i - 1] - 1);
            }
        }
        if (allSingleSlot == false) {
            for (int i = 0; i < numDocsInBlock; i++) {
                prefixLen = writeVIntToArray(prefixBuf, prefixLen, blockSlotCounts[i]);
            }
        }
        final int totalPayloadLen = prefixLen + blockPayloadLen;
        // Grow blockPayload to hold both prefix and value bytes, shift the value bytes right,
        // then copy the prefix into the freed front region.
        if (prefixLen > 0) {
            blockPayload = ArrayUtil.grow(blockPayload, totalPayloadLen);
            System.arraycopy(blockPayload, 0, blockPayload, prefixLen, blockPayloadLen);
            System.arraycopy(prefixBuf, 0, blockPayload, 0, prefixLen);
        }
        maxUncompressedBlockLen = Math.max(maxUncompressedBlockLen, totalPayloadLen);

        final boolean compress = totalPayloadLen >= minCompressBytes;

        byte flags = 0;
        if (compress) flags |= FLAG_VALUES_COMPRESSED;
        if (contiguous) flags |= FLAG_DOCS_CONTIGUOUS;
        if (allSingleSlot) flags |= FLAG_ALL_SINGLE_SLOT;

        currentOut.writeByte(flags);
        writeVInt(currentOut, numDocsInBlock);

        writeVInt(currentOut, totalPayloadLen);
        if (compress) {
            compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(blockPayload, 0, totalPayloadLen))), currentOut);
        } else {
            currentOut.writeBytes(blockPayload, 0, totalPayloadLen);
        }

        totalBlockBytes += (int) (currentOut.getFilePointer() - blockStart);
        numBlocks++;

        numDocsInBlock = 0;
        blockPayloadLen = 0;
    }

    private static void writeVInt(IndexOutput out, int v) throws IOException {
        while ((v & ~0x7F) != 0) {
            out.writeByte((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.writeByte((byte) v);
    }

    /**
     * Encodes {@code v} as a VInt into {@code buf} starting at {@code off}.
     * Returns the new offset after the encoded bytes.
     */
    static int writeVIntToArray(byte[] buf, int off, int v) {
        while ((v & ~0x7F) != 0) {
            buf[off++] = (byte) ((v & 0x7F) | 0x80);
            v >>>= 7;
        }
        buf[off++] = (byte) v;
        return off;
    }
}
