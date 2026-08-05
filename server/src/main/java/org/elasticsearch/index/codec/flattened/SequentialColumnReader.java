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

    // Current block state.
    private int currentBlock = -1;
    private int numDocsInBlock;
    private boolean contiguous;
    private boolean allSingleSlot;
    private boolean compressed;
    private int[] docIds;      // resolved docIds when !contiguous (populated eagerly by loadBlockHeader)
    private int[] slotCounts;  // slot counts per doc when !allSingleSlot (populated by ensurePayloadLoaded)
    private int uncompPayloadLen;
    private long payloadAbsOff;

    // Decompressed payload.
    private byte[] payload = new byte[256];
    private boolean payloadLoaded;

    // Cursor state within current block.
    private int docIdx = -1;       // index of current doc within block
    private int payloadCursor = 0; // byte offset in payload[]

    // Exported per-doc state (populated by nextDoc()).
    private int currentDocId = -1;
    private int currentSlotCount;
    private int currentDocSlotsOff;
    private int currentDocSlotsLen;

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
        this.slotCounts = new int[8];

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

        // Determine the lowest block index we still need to consider. If the current block is not
        // yet exhausted (there are unconsumed docs in it), target might still be in it. Otherwise
        // the current block is spent and we must start from the next one.
        final boolean currentBlockHasMore = currentBlock >= 0 && (docIdx + 1) < numDocsInBlock;
        final int firstCandidateBlock = currentBlockHasMore ? currentBlock : currentBlock + 1;

        // Binary-search firstDocIds[firstCandidateBlock .. numBlocks-1] for the last block whose
        // first doc id is <= target. That block is the only one that can contain target.
        int lo = Math.max(0, firstCandidateBlock);
        int hi = numBlocks - 1;
        if (hi < lo) {
            // No candidate blocks remain.
            currentDocId = NO_MORE_DOCS;
            return NO_MORE_DOCS;
        }
        if (firstDocIds[lo] > target) {
            // Even the first candidate block starts after target — target is not in the column.
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
        // lo is now the last block with firstDocIds[lo] <= target.

        // Load the block header if we moved to a different block.
        final boolean newBlock = (lo != currentBlock);
        if (newBlock) {
            currentBlock = lo;
            loadBlockHeader(currentBlock);
            docIdx = -1; // block header loaded; payload cursor not yet valid
        }

        // payloadCursor is positioned just after docIdx's slots (or at the start of the payload
        // when docIdx == -1, i.e. we just entered a new block). The first doc in the block we
        // have NOT yet consumed is therefore (docIdx + 1), which is 0 when docIdx == -1.
        final int nextUnconsumed = docIdx + 1; // 0 for a freshly loaded block

        // Find the first doc index within the block that is >= target.
        if (contiguous) {
            final int candidateIdx = target - firstDocIds[currentBlock];
            if (candidateIdx >= numDocsInBlock) {
                // target is past this block's last doc.
                currentDocId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }
            final int newIdx = Math.max(nextUnconsumed, candidateIdx);
            // Skip slots for docs nextUnconsumed .. newIdx-1 (the ones between the already-consumed
            // position and the landing doc, exclusive of newIdx itself).
            ensurePayloadLoaded();
            final int slotsToSkip = slotsBeforeIndex(nextUnconsumed, newIdx);
            if (slotsToSkip > 0) {
                skipNSlots(slotsToSkip);
            }
            docIdx = newIdx;
        } else {
            // Binary-search docIds[nextUnconsumed .. numDocsInBlock-1] for the first index >= target.
            // docIds[] is populated by loadBlockHeader (eagerly, outside the compressed region).
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
                // target is past this block's last doc.
                currentDocId = NO_MORE_DOCS;
                return NO_MORE_DOCS;
            }
            ensurePayloadLoaded();
            final int slotsToSkip = slotsBeforeIndex(nextUnconsumed, iLo);
            if (slotsToSkip > 0) {
                skipNSlots(slotsToSkip);
            }
            docIdx = iLo;
        }
        return positionAt(docIdx);
    }

    /**
     * Returns the number of slots that precede {@code targetIdx} but are at or after {@code fromIdx}.
     * Used by {@link #advance} to fast-forward {@link #payloadCursor} within the current block without
     * re-reading already-consumed docs.
     */
    private int slotsBeforeIndex(int fromIdx, int targetIdx) {
        if (allSingleSlot) {
            return targetIdx - fromIdx;
        }
        int total = 0;
        for (int i = fromIdx; i < targetIdx; i++) {
            total += slotCounts[i];
        }
        return total;
    }

    /**
     * Completes cursor positioning after {@link #docIdx} and {@link #payloadCursor} have been set
     * to the start of the target doc. Reads and stores the slot count, advances
     * {@link #payloadCursor} past the doc's slots, and records
     * {@link #currentDocSlotsOff}/{@link #currentDocSlotsLen}/{@link #currentDocId}.
     */
    private int positionAt(int idx) throws IOException {
        ensurePayloadLoaded();
        currentDocSlotsOff = payloadCursor;
        currentSlotCount = allSingleSlot ? 1 : slotCounts[idx];
        skipNSlots(currentSlotCount);
        currentDocSlotsLen = payloadCursor - currentDocSlotsOff;
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
     * Decompressed block payload. The current doc's slot bytes occupy
     * {@code payload()[docSlotsOffset() .. docSlotsOffset() + docSlotsLength() - 1]}.
     */
    byte[] payload() {
        return payload;
    }

    /** Start of the current doc's slot run within {@link #payload()}. */
    int docSlotsOffset() {
        return currentDocSlotsOff;
    }

    /** Byte length of the current doc's slot run. */
    int docSlotsLength() {
        return currentDocSlotsLen;
    }

    private void loadBlockHeader(int blockIdx) throws IOException {
        payloadLoaded = false;
        payloadCursor = 0;

        dataIn.seek(columnStartOff + blockStartsRel[blockIdx]);

        final byte flags = dataIn.readByte();
        contiguous = (flags & FLAG_DOCS_CONTIGUOUS) != 0;
        allSingleSlot = (flags & FLAG_ALL_SINGLE_SLOT) != 0;
        compressed = (flags & FLAG_VALUES_COMPRESSED) != 0;

        numDocsInBlock = dataIn.readVInt();

        // Eagerly read bit-packed docId deltas (outside the compressed region).
        if (contiguous == false) {
            if (docIds.length < numDocsInBlock) docIds = new int[numDocsInBlock];
            docIds[0] = firstDocIds[blockIdx];
            final int bitsPerDelta = dataIn.readByte() & 0xFF;
            FlattenedDocValuesFormat.unpackInts(dataIn, docIds, 1, numDocsInBlock - 1, bitsPerDelta);
            for (int i = 1; i < numDocsInBlock; i++) {
                docIds[i] = docIds[i - 1] + docIds[i] + 1;
            }
        }

        uncompPayloadLen = dataIn.readVInt();
        payloadAbsOff = dataIn.getFilePointer();
    }

    private void ensurePayloadLoaded() throws IOException {
        if (payloadLoaded) return;
        if (payload.length < uncompPayloadLen) payload = new byte[uncompPayloadLen];
        dataIn.seek(payloadAbsOff);
        if (compressed) {
            final BytesRef decompRef = new BytesRef(payload, 0, uncompPayloadLen);
            DECOMPRESSOR.decompress(dataIn, uncompPayloadLen, 0, uncompPayloadLen, decompRef);
            payload = decompRef.bytes;
        } else {
            dataIn.readBytes(payload, 0, uncompPayloadLen);
        }
        // Parse the slot-count prefix (absent when allSingleSlot).
        int cursor = 0;
        if (allSingleSlot == false) {
            if (slotCounts.length < numDocsInBlock) slotCounts = new int[numDocsInBlock];
            final int bitsPerSlot = payload[cursor++] & 0xFF;
            cursor = FlattenedDocValuesFormat.unpackInts(payload, cursor, slotCounts, 0, numDocsInBlock, bitsPerSlot);
        }
        payloadCursor = cursor;
        payloadLoaded = true;
    }

    /**
     * Advances {@link #payloadCursor} past {@code n} encoded slots in {@link #payload}.
     * Each slot is {@code [vint prefix][prefix-1 bytes]}, prefix 0 = null (no bytes follow).
     */
    private void skipNSlots(int n) {
        for (int i = 0; i < n; i++) {
            int prefix = 0, shift = 0;
            while (true) {
                final int b = payload[payloadCursor++] & 0xFF;
                prefix |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (prefix > 0) payloadCursor += prefix - 1;
        }
    }

    @Override
    public void close() throws IOException {
        dataIn.close();
    }
}
