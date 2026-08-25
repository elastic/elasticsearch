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
import org.apache.lucene.store.ByteArrayDataOutput;
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
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_META_COMPRESSED;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_NO_NULL_VALUES;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_VALUES_COMPRESSED;

/**
 * Writes one column (sub-field) directly into an existing {@link IndexOutput}.
 *
 * <p>Blocks are written straight into the caller-supplied output — no temp files, no splice.
 * The caller retains ownership of the output; {@link #close()} is a no-op.
 *
 * <p>One instance is reused for every column of a field: call {@link #reset(IndexOutput)} at each column
 * boundary rather than allocating a new writer. The block-accumulation arrays are pre-sized from
 * the configured thresholds ({@code 2 * 4 * maxDocsPerBlock + targetBlockBytes} bytes) and the
 * ZSTD compressor is expensive to allocate, so allocating per column produces hundreds of
 * megabytes of short-lived garbage per flush on fields with large sub-field cardinality.
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
     * Reusable scratch buffer for the metadata region (slot counts + encoded lengths). Allocated
     * once in the constructor and reused across blocks/columns; never cleared by {@link #reset}.
     */
    private byte[] metaScratch;
    /** Reusable writer over {@link #metaScratch}; reset at the start of each block flush. */
    private final ByteArrayDataOutput metaOut = new ByteArrayDataOutput();

    /** Absolute data-file offset of the first block for this column. Updated by {@link #reset}. */
    private long columnStartOffset;
    /** The externally-owned output this column is being written into. */
    private IndexOutput currentOut;

    // Current block accumulation.
    //
    // blockDocIds[0..numDocsInBlock) — docId per document
    // blockSlotCounts[0..numDocsInBlock) — slot count per document
    // blockSlotLens[0..numSlotsInBlock) — value length per slot (-1 = null)
    // blockValues[0..blockValuesLen) — concatenated raw value bytes (no per-slot framing)
    private int[] blockDocIds;
    private int[] blockSlotCounts;
    private int[] blockSlotLens;
    private byte[] blockValues;
    private int numDocsInBlock = 0;
    private int numSlotsInBlock = 0;
    private int blockValuesLen = 0;

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
        this.blockSlotLens = new int[maxDocsPerBlock];
        this.blockValues = new byte[targetBlockBytes];
        this.metaScratch = new byte[maxDocsPerBlock];
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
        this.numSlotsInBlock = 0;
        this.blockValuesLen = 0;
        this.numBlocks = 0;
        this.totalBlockBytes = 0;
        this.maxUncompressedBlockLen = 0;
        this.maxDocsPerBlockSeen = 0;
        this.finished = false;
    }

    /**
     * Appends all slots for {@code docId} from an already-decoded representation.
     *
     * <p>{@code slotLens[slotLensOff .. slotLensOff+slotCount)} gives the byte length of each slot,
     * with {@code -1} meaning a null slot. {@code values[valuesOff .. valuesOff+valuesLen)} contains
     * the concatenated raw value bytes for the non-null slots (null slots contribute nothing).
     *
     * <p>The block-flush trigger fires before appending, at the doc boundary, so no document's slots
     * are ever split across two blocks.
     *
     * @param docId      the target document ID (must be &ge; the last docId passed to this writer)
     * @param slotCount  number of slots for this document
     * @param slotLens   per-slot lengths; {@code -1} means null
     * @param slotLensOff start index in {@code slotLens}
     * @param values     raw value bytes (null slots contribute no bytes)
     * @param valuesOff  start offset in {@code values}
     * @param valuesLen  total raw value bytes for this document
     */
    void addDocSlots(int docId, int slotCount, int[] slotLens, int slotLensOff, byte[] values, int valuesOff, int valuesLen)
        throws IOException {
        if (numDocsInBlock > 0 && (numDocsInBlock >= maxDocsPerBlock || blockValuesLen >= targetBlockBytes)) {
            flushCurrentBlock();
        }

        blockDocIds = ArrayUtil.grow(blockDocIds, numDocsInBlock + 1);
        blockSlotCounts = ArrayUtil.grow(blockSlotCounts, numDocsInBlock + 1);
        blockDocIds[numDocsInBlock] = docId;
        blockSlotCounts[numDocsInBlock] = slotCount;
        numDocsInBlock++;

        blockSlotLens = ArrayUtil.grow(blockSlotLens, numSlotsInBlock + slotCount);
        System.arraycopy(slotLens, slotLensOff, blockSlotLens, numSlotsInBlock, slotCount);
        numSlotsInBlock += slotCount;

        blockValues = ArrayUtil.grow(blockValues, blockValuesLen + valuesLen);
        System.arraycopy(values, valuesOff, blockValues, blockValuesLen, valuesLen);
        blockValuesLen += valuesLen;
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
        maxUncompressedBlockLen = Math.max(maxUncompressedBlockLen, blockValuesLen);

        blockFirstDocIds = ArrayUtil.grow(blockFirstDocIds, numBlocks + 1);
        blockStartsRelative = ArrayUtil.grow(blockStartsRelative, numBlocks + 1);
        blockFirstDocIds[numBlocks] = blockDocIds[0];
        blockStartsRelative[numBlocks] = totalBlockBytes;

        final long blockStart = currentOut.getFilePointer();

        // Decide contiguous / allSingleSlot / noNulls.
        boolean contiguous = true;
        for (int i = 1; i < numDocsInBlock && contiguous; i++) {
            if (blockDocIds[i] != blockDocIds[i - 1] + 1) contiguous = false;
        }
        boolean allSingleSlot = true;
        for (int i = 0; i < numDocsInBlock && allSingleSlot; i++) {
            if (blockSlotCounts[i] != 1) allSingleSlot = false;
        }
        boolean noNulls = true;
        for (int s = 0; s < numSlotsInBlock && noNulls; s++) {
            if (blockSlotLens[s] < 0) noNulls = false;
        }

        // Encode slot counts and value lengths into the metadata buffer before writing flags,
        // because FLAG_META_COMPRESSED depends on the encoded size.

        // Step 1: compute bitsPerSlot (when !allSingleSlot).
        int bitsPerSlot = 0;
        if (allSingleSlot == false) {
            int maxSlot = 0;
            for (int i = 0; i < numDocsInBlock; i++) {
                if (blockSlotCounts[i] > maxSlot) maxSlot = blockSlotCounts[i];
            }
            bitsPerSlot = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxSlot));
        }

        // Step 2: encode lengths in-place (reuse blockSlotLens).
        // noNulls: encodedLen = valueLen (0 = empty string).
        // has nulls: encodedLen = 0 for null, valueLen+1 otherwise.
        int maxLen = 0;
        for (int s = 0; s < numSlotsInBlock; s++) {
            final int enc = noNulls ? blockSlotLens[s] : (blockSlotLens[s] < 0 ? 0 : blockSlotLens[s] + 1);
            if (enc > maxLen) maxLen = enc;
            blockSlotLens[s] = enc;
        }
        final int bitsPerLen = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxLen));

        // Step 3: pack into metaScratch. Pre-size exactly; ByteArrayDataOutput does not grow.
        int metaCap = 1 + (int) ((numSlotsInBlock * (long) bitsPerLen + 7) / 8);
        if (allSingleSlot == false) {
            metaCap += 1 + (int) ((numDocsInBlock * (long) bitsPerSlot + 7) / 8);
        }
        assert metaCap >= 2 : "metadata region must contain at least bitsPerLen byte + one packed byte";
        metaScratch = ArrayUtil.growNoCopy(metaScratch, metaCap);
        metaOut.reset(metaScratch, 0, metaScratch.length);
        if (allSingleSlot == false) {
            metaOut.writeByte((byte) bitsPerSlot);
            FlattenedDocValuesFormat.packInts(metaOut, blockSlotCounts, numDocsInBlock, bitsPerSlot);
        }
        metaOut.writeByte((byte) bitsPerLen);
        FlattenedDocValuesFormat.packInts(metaOut, blockSlotLens, numSlotsInBlock, bitsPerLen);
        final int metaLen = metaOut.getPosition();
        assert metaLen == metaCap : "computed metaCap=" + metaCap + " but wrote metaLen=" + metaLen;

        final boolean compress = blockValuesLen >= minCompressBytes;
        final boolean compressMeta = metaLen >= minCompressBytes;

        byte flags = 0;
        if (compress) flags |= FLAG_VALUES_COMPRESSED;
        if (contiguous) flags |= FLAG_DOCS_CONTIGUOUS;
        if (allSingleSlot) flags |= FLAG_ALL_SINGLE_SLOT;
        if (noNulls) flags |= FLAG_NO_NULL_VALUES;
        if (compressMeta) flags |= FLAG_META_COMPRESSED;

        currentOut.writeByte(flags);
        writeVInt(currentOut, numDocsInBlock);

        // Bit-pack docId deltas (outside any compressed region).
        if (contiguous == false) {
            int maxDelta = 0;
            for (int i = 0; i < numDocsInBlock - 1; i++) {
                final int delta = blockDocIds[i + 1] - blockDocIds[i] - 1;
                if (delta > maxDelta) maxDelta = delta;
                blockDocIds[i] = delta;
            }
            final int bitsPerDelta = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxDelta));
            currentOut.writeByte((byte) bitsPerDelta);
            FlattenedDocValuesFormat.packInts(currentOut, blockDocIds, numDocsInBlock - 1, bitsPerDelta);
        }

        // Write the metadata region (slot counts + value lengths) as its own compressed frame.
        writeVInt(currentOut, metaLen);
        if (compressMeta) {
            compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(metaScratch, 0, metaLen))), currentOut);
        } else {
            currentOut.writeBytes(metaScratch, 0, metaLen);
        }

        // Compress (or write raw) the value region.
        if (compress) {
            compressor.compress(new ByteBuffersDataInput(List.of(ByteBuffer.wrap(blockValues, 0, blockValuesLen))), currentOut);
        } else {
            currentOut.writeBytes(blockValues, 0, blockValuesLen);
        }

        totalBlockBytes += (int) (currentOut.getFilePointer() - blockStart);
        numBlocks++;

        numDocsInBlock = 0;
        numSlotsInBlock = 0;
        blockValuesLen = 0;
    }

    private static void writeVInt(IndexOutput out, int v) throws IOException {
        while ((v & ~0x7F) != 0) {
            out.writeByte((byte) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        out.writeByte((byte) v);
    }

}
