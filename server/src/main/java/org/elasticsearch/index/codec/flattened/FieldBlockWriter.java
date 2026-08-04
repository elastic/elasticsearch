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
import java.util.Collections;

import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_ALL_SINGLE_SLOT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_DOCS_CONTIGUOUS;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_VALUES_COMPRESSED;

/**
 * Writes one column (sub-field) directly into an existing {@link IndexOutput}.
 *
 * <p>Blocks are written straight into the caller-supplied output — no temp files, no splice.
 * The caller retains ownership of the output; {@link #close()} is a no-op.
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

    /** Absolute data-file offset of the first block for this column. */
    private final long columnStartOffset;
    /** The externally-owned output this column is being written into. */
    private final IndexOutput currentOut;

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

        maxUncompressedBlockLen = Math.max(maxUncompressedBlockLen, blockPayloadLen);
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
        final boolean compress = blockPayloadLen >= minCompressBytes;

        byte flags = 0;
        if (compress) flags |= FLAG_VALUES_COMPRESSED;
        if (contiguous) flags |= FLAG_DOCS_CONTIGUOUS;
        if (allSingleSlot) flags |= FLAG_ALL_SINGLE_SLOT;

        currentOut.writeByte(flags);
        writeVInt(currentOut, numDocsInBlock);

        if (contiguous == false) {
            for (int i = 1; i < numDocsInBlock; i++) {
                writeVInt(currentOut, blockDocIds[i] - blockDocIds[i - 1] - 1);
            }
        }
        if (allSingleSlot == false) {
            for (int i = 0; i < numDocsInBlock; i++) {
                writeVInt(currentOut, blockSlotCounts[i]);
            }
        }

        writeVInt(currentOut, blockPayloadLen);
        if (compress) {
            compressor.compress(
                new ByteBuffersDataInput(Collections.singletonList(ByteBuffer.wrap(blockPayload, 0, blockPayloadLen))),
                currentOut
            );
        } else {
            currentOut.writeBytes(blockPayload, 0, blockPayloadLen);
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
