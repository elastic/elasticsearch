/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_ALL_SINGLE_SLOT;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_DOCS_CONTIGUOUS;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_META_COMPRESSED;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_NO_NULL_VALUES;
import static org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat.FLAG_VALUES_COMPRESSED;

/**
 * Forward-only, forward-repositionable sequential reader for one column of the columnar flattened format.
 *
 * <p>Unlike {@link FlattenedDocValuesProducer.ColumnCursor}, this reader is forward-only — it
 * never moves backwards — and exposes bulk slot access. It is used on the merge path
 * ({@link FlattenedDocValuesConsumer#mergeBinaryField}), where docs are always visited in
 * ascending order via {@link #nextDoc()}, and by the batch block-loading path
 * ({@link KeyColumnBatchReader}) which uses {@link #advance(int)} to seek forward to a target
 * document without decompressing any skipped blocks.
 *
 * <p>Typical use (merge path):
 * <pre>{@code
 * try (SequentialColumnReader r = new SequentialColumnReader(dataClone, ...)) {
 *     for (int doc = r.nextDoc(); doc != NO_MORE_DOCS; doc = r.nextDoc()) {
 *         writer.addDocSlots(targetId, r.slotCount(),
 *                            r.slotLens(), r.firstSlotIndex(),
 *                            r.payload(), r.docSlotsOffset(), r.docSlotsLength());
 *     }
 * }
 * }</pre>
 *
 * <p>The {@link IndexInput} passed to the constructor is owned by this reader and closed
 * via {@link #close()}.
 */
final class SequentialColumnReader implements Closeable {

    /**
     * Shared ZSTD decompressor. Column readers are used on a single merge thread;
     * the instance is not shared across threads.
     */
    private static final org.apache.lucene.codecs.compressing.Decompressor DECOMPRESSOR =
        new org.elasticsearch.index.codec.zstd.ZstdCompressionMode(1).newDecompressor();

    private final IndexInput dataIn;
    private final long columnStartOff;
    private final int numBlocks;

    // Block index (eagerly loaded: typically a handful of entries).
    private final int[] firstDocIds;
    private final int[] blockStartsRel;

    // Current block state — set by loadBlockHeader().
    private int currentBlock = -1;
    private int numDocsInBlock;
    private int numSlotsInBlock;
    private boolean contiguous;
    private boolean allSingleSlot;
    private boolean noNullValues;
    private boolean compressed;
    private boolean metaCompressed;
    /**
     * Per-doc slot ranges. {@code slotStarts[i]} is the index of the first slot for doc {@code i};
     * {@code slotStarts[numDocsInBlock]} is {@code numSlotsInBlock}. Sized {@code numDocsInBlock+1}.
     * When {@code allSingleSlot}, slot index equals doc index (no array needed; read via
     * {@code docIdx} directly).
     * Populated by {@link #ensureMetaLoaded()}.
     */
    private int[] slotStarts;   // size numDocsInBlock+1 when !allSingleSlot
    /** Resolved docIds when {@code !contiguous} (populated eagerly in loadBlockHeader). */
    private int[] docIds;
    /** Absolute file offset of the metadata region (slot counts + value lengths). */
    private long metaAbsOff;
    /** Uncompressed byte length of the metadata region. */
    private int metaLen;
    /** Absolute file offset of the value region (set after {@link #ensureMetaLoaded()}). */
    private long valueRegionAbsOff;
    /** Bit width of the packed value-length array (set in {@link #ensureMetaLoaded()}). */
    private int bitsPerLen;
    /** Offset of the packed value-length array within {@link #metaScratch}. */
    private int metaLensOff;
    /** Decompressed metadata buffer; grown on demand. */
    private byte[] metaScratch = new byte[64];
    private boolean metaLoaded;

    // Loaded per-slot state — populated lazily by ensureValuesLoaded().
    /** {@code valueOffsets[s]} = byte start of slot {@code s} in {@link #payload}. Length numSlots+1. */
    private int[] valueOffsets;
    /**
     * Resolved length per slot; {@code -1} = null slot. Shared with the merge/batch caller —
     * valid between {@link #nextDoc()}/{@link #advance(int)} and the next such call.
     */
    private int[] slotLens;
    /** Decompressed raw value bytes for the current block. */
    private byte[] payload = new byte[256];
    private boolean valuesLoaded;

    // Cursor state within the current block.
    private int docIdx = -1;        // index of current doc within block
    private int currentDocId = -1;
    private int currentSlotCount;
    private int currentFirstSlot;   // first slot index for the current doc
    private int currentDocSlotsOff; // byte offset in payload[] of this doc's first value byte
    private int currentDocSlotsLen; // byte length of this doc's value run in payload[]

    /**
     * Creates a sequential reader for one column.
     *
     * @param dataIn          cloned IndexInput positioned anywhere; ownership is transferred
     * @param columnStartOff  absolute data-file offset of the column's first block
     * @param blockIndexRelOff byte offset of the block index from {@code columnStartOff}
     * @param numBlocks       number of blocks in this column
     */
    SequentialColumnReader(IndexInput dataIn, long columnStartOff, int blockIndexRelOff, int numBlocks) throws IOException {
        this.dataIn = dataIn;
        this.columnStartOff = columnStartOff;
        this.numBlocks = numBlocks;
        this.firstDocIds = new int[numBlocks];
        this.blockStartsRel = new int[numBlocks];
        this.docIds = new int[8];
        this.slotStarts = new int[9]; // +1 for sentinel
        this.valueOffsets = new int[9];
        this.slotLens = new int[8];

        // Eagerly load the block index (8 bytes per block, typically very small).
        if (numBlocks > 0) {
            dataIn.seek(columnStartOff + blockIndexRelOff);
            for (int b = 0; b < numBlocks; b++) {
                firstDocIds[b] = dataIn.readInt();
                blockStartsRel[b] = dataIn.readInt();
            }
        }
    }

    /**
     * Advances to the next document in this column and returns its source docID, or
     * {@link org.apache.lucene.search.DocIdSetIterator#NO_MORE_DOCS} when exhausted.
     *
     * <p>After a successful return the caller may read {@link #slotCount()},
     * {@link #payload()}, {@link #docSlotsOffset()}, and {@link #docSlotsLength()}.
     */
    int nextDoc() throws IOException {
        if (currentBlock >= 0 && docIdx + 1 < numDocsInBlock) {
            // More docs in the current block.
            docIdx++;
        } else {
            // Move to the next block.
            currentBlock++;
            if (currentBlock >= numBlocks) {
                currentDocId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }
            loadBlockHeader(currentBlock);
            docIdx = 0;
        }
        return positionAt(docIdx);
    }

    /**
     * Advances to the first document in this column with {@code docId >= target}.
     * Idempotent: if the cursor is already at or past {@code target} it returns the current doc
     * immediately (no I/O, no cursor movement), so duplicate or non-decreasing doc ids are safe.
     *
     * <p>Whole blocks whose last doc id is less than {@code target} are skipped entirely without
     * decompressing their payload — only the block index (already loaded) is consulted.
     *
     * @return the landed doc id, or {@link org.apache.lucene.search.DocIdSetIterator#NO_MORE_DOCS}
     */
    int advance(int target) throws IOException {
        // Fast path: already there.
        if (currentDocId >= target) {
            return currentDocId;
        }

        final boolean currentBlockHasMore = currentBlock >= 0 && (docIdx + 1) < numDocsInBlock;
        final int firstCandidateBlock = currentBlockHasMore ? currentBlock : currentBlock + 1;

        // Binary-search the block index for the last block whose firstDocId <= target.
        int lo = Math.max(0, firstCandidateBlock);
        int hi = numBlocks - 1;
        if (hi < lo) {
            currentDocId = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        if (firstDocIds[lo] > target) {
            currentDocId = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        while (lo < hi) {
            final int mid = (lo + hi + 1) >>> 1;
            if (firstDocIds[mid] <= target) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }

        if (lo != currentBlock) {
            currentBlock = lo;
            loadBlockHeader(currentBlock);
            docIdx = -1;
        }

        final int nextUnconsumed = docIdx + 1; // 0 for a freshly loaded block

        if (contiguous) {
            final int candidateIdx = target - firstDocIds[currentBlock];
            if (candidateIdx >= numDocsInBlock) {
                currentDocId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }
            docIdx = Math.max(nextUnconsumed, candidateIdx);
        } else {
            // Binary-search docIds[nextUnconsumed .. numDocsInBlock-1] for first index >= target.
            int iLo = nextUnconsumed, iHi = numDocsInBlock - 1;
            while (iLo < iHi) {
                final int mid = (iLo + iHi) >>> 1;
                if (docIds[mid] < target) {
                    iLo = mid + 1;
                } else {
                    iHi = mid;
                }
            }
            if (docIds[iLo] < target) {
                currentDocId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }
            docIdx = iLo;
        }
        return positionAt(docIdx);
    }

    /**
     * Completes cursor positioning once {@link #docIdx} is set.
     * Ensures value offsets are loaded (decompresses once per block), then reads the doc's slot range.
     */
    private int positionAt(int idx) throws IOException {
        ensureValuesLoaded();
        currentFirstSlot = allSingleSlot ? idx : slotStarts[idx];
        currentSlotCount = allSingleSlot ? 1 : (slotStarts[idx + 1] - currentFirstSlot);
        currentDocSlotsOff = valueOffsets[currentFirstSlot];
        currentDocSlotsLen = valueOffsets[currentFirstSlot + currentSlotCount] - currentDocSlotsOff;
        currentDocId = contiguous ? (firstDocIds[currentBlock] + idx) : docIds[idx];
        return currentDocId;
    }

    /** Source docID of the current document (valid only after a successful {@link #nextDoc()}). */
    int docId() {
        return currentDocId;
    }

    /** Number of slots the current document has in this column. */
    int slotCount() {
        return currentSlotCount;
    }

    /**
     * Index of the first slot for the current document within the block's
     * {@link #slotLens()} and {@link #valueOffsets()} arrays.
     */
    int firstSlotIndex() {
        return currentFirstSlot;
    }

    /**
     * Decompressed block payload containing raw value bytes, one slot's bytes concatenated after
     * another. The current doc's value bytes occupy
     * {@code payload()[docSlotsOffset() .. docSlotsOffset() + docSlotsLength() - 1]}.
     */
    byte[] payload() {
        return payload;
    }

    /** Start of the current doc's value run within {@link #payload()}. */
    int docSlotsOffset() {
        return currentDocSlotsOff;
    }

    /** Byte length of the current doc's value run. */
    int docSlotsLength() {
        return currentDocSlotsLen;
    }

    /**
     * Resolved per-slot lengths for the entire current block. Length {@code -1} means null.
     * Array is valid from index 0 through {@code numSlotsInBlock-1}.
     * Do not modify; valid until the next {@link #nextDoc()}/{@link #advance(int)} call.
     */
    int[] slotLens() {
        return slotLens;
    }

    /**
     * Prefix-sum offset table for the current block. {@code valueOffsets()[s]} is the byte
     * start of slot {@code s} in {@link #payload()}; {@code valueOffsets()[numSlotsInBlock]}
     * is the total payload length. Array is valid until the next block is loaded.
     */
    int[] valueOffsets() {
        return valueOffsets;
    }

    /** True when every document in the current block has exactly one slot. */
    boolean blockAllSingleSlot() {
        return allSingleSlot;
    }

    /** True when at least one slot in the current block is null. */
    boolean blockHasNulls() {
        return noNullValues == false;
    }

    /** DocId of the last document in the current block, or -1 if no block is loaded. */
    int blockLastDocId() {
        if (currentBlock < 0) return -1;
        if (contiguous) {
            return firstDocIds[currentBlock] + numDocsInBlock - 1;
        }
        return docIds[numDocsInBlock - 1];
    }

    private void loadBlockHeader(int blockIdx) throws IOException {
        metaLoaded = false;
        valuesLoaded = false;

        dataIn.seek(columnStartOff + blockStartsRel[blockIdx]);

        final byte flags = dataIn.readByte();
        contiguous = (flags & FLAG_DOCS_CONTIGUOUS) != 0;
        allSingleSlot = (flags & FLAG_ALL_SINGLE_SLOT) != 0;
        noNullValues = (flags & FLAG_NO_NULL_VALUES) != 0;
        compressed = (flags & FLAG_VALUES_COMPRESSED) != 0;
        metaCompressed = (flags & FLAG_META_COMPRESSED) != 0;

        numDocsInBlock = dataIn.readVInt();

        // Eagerly read bit-packed docId deltas (outside any compressed region).
        if (contiguous == false) {
            if (docIds.length < numDocsInBlock) docIds = new int[numDocsInBlock];
            docIds[0] = firstDocIds[blockIdx];
            final int bitsPerDelta = dataIn.readByte() & 0xFF;
            FlattenedDocValuesFormat.unpackInts(dataIn, docIds, 1, numDocsInBlock - 1, bitsPerDelta);
            for (int i = 1; i < numDocsInBlock; i++) {
                docIds[i] = docIds[i - 1] + docIds[i] + 1;
            }
        }

        // Record the start of the metadata region for lazy decoding.
        metaLen = dataIn.readVInt();
        metaAbsOff = dataIn.getFilePointer();
    }

    /**
     * Decompresses (or reads) the metadata region for the current block, if not already done.
     * Builds {@link #slotStarts} (prefix sums of slot counts) and sets {@link #numSlotsInBlock},
     * {@link #bitsPerLen}, {@link #metaLensOff}, and {@link #valueRegionAbsOff}.
     */
    private void ensureMetaLoaded() throws IOException {
        if (metaLoaded) return;

        dataIn.seek(metaAbsOff);
        metaScratch = FlattenedDocValuesFormat.readMaybeCompressed(DECOMPRESSOR, dataIn, metaLen, metaCompressed, metaScratch);
        valueRegionAbsOff = dataIn.getFilePointer();

        int off = 0;
        if (allSingleSlot) {
            numSlotsInBlock = numDocsInBlock;
        } else {
            if (slotStarts.length < numDocsInBlock + 1) slotStarts = new int[numDocsInBlock + 1];
            final int bitsPerSlot = metaScratch[off++] & 0xFF;
            // Unpack slot counts temporarily into slotStarts[0..numDocs); then prefix-sum in place.
            off = FlattenedDocValuesFormat.unpackInts(metaScratch, off, slotStarts, 0, numDocsInBlock, bitsPerSlot);
            int acc = 0;
            for (int i = 0; i < numDocsInBlock; i++) {
                final int cnt = slotStarts[i];
                slotStarts[i] = acc;
                acc += cnt;
            }
            slotStarts[numDocsInBlock] = acc;
            numSlotsInBlock = acc;
        }

        bitsPerLen = metaScratch[off++] & 0xFF;
        metaLensOff = off;

        metaLoaded = true;
    }

    private void ensureValuesLoaded() throws IOException {
        if (valuesLoaded) return;

        // Metadata must be decoded first: it provides numSlotsInBlock, bitsPerLen, metaLensOff,
        // and valueRegionAbsOff, all of which this method needs.
        ensureMetaLoaded();

        if (slotLens.length < numSlotsInBlock) slotLens = new int[numSlotsInBlock];
        if (valueOffsets.length < numSlotsInBlock + 1) valueOffsets = new int[numSlotsInBlock + 1];

        // Unpack the value-length array from the already-decoded metadata buffer (no seek).
        FlattenedDocValuesFormat.unpackInts(metaScratch, metaLensOff, slotLens, 0, numSlotsInBlock, bitsPerLen);

        // Decode lengths and build prefix-sum offset table.
        int totalValueBytes = 0;
        if (noNullValues) {
            for (int s = 0; s < numSlotsInBlock; s++) {
                valueOffsets[s] = totalValueBytes;
                totalValueBytes += slotLens[s]; // raw length; never -1
            }
        } else {
            for (int s = 0; s < numSlotsInBlock; s++) {
                valueOffsets[s] = totalValueBytes;
                final int enc = slotLens[s];
                if (enc == 0) {
                    slotLens[s] = -1; // null slot
                } else {
                    final int len = enc - 1;
                    slotLens[s] = len;
                    totalValueBytes += len;
                }
            }
        }
        valueOffsets[numSlotsInBlock] = totalValueBytes;

        // Seek to the value region (ensureMetaLoaded may have been a no-op on this call, so the
        // file pointer may already have moved past the metadata area).
        dataIn.seek(valueRegionAbsOff);

        // Decompress (or read) the raw value bytes.
        if (payload.length < totalValueBytes) payload = new byte[totalValueBytes];
        if (compressed) {
            final BytesRef decompRef = new BytesRef(payload, 0, totalValueBytes);
            DECOMPRESSOR.decompress(dataIn, totalValueBytes, 0, totalValueBytes, decompRef);
            payload = decompRef.bytes;
        } else {
            dataIn.readBytes(payload, 0, totalValueBytes);
        }

        valuesLoaded = true;
    }

    @Override
    public void close() throws IOException {
        dataIn.close();
    }
}
