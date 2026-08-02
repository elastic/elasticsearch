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
 * Forward-only sequential reader for one column of the columnar flattened format.
 *
 * <p>Unlike {@link FlattenedDocValuesProducer.ColumnCursor}, this reader is strictly
 * forward-only and supports bulk slot access. It is used exclusively on the merge path
 * ({@link FlattenedDocValuesConsumer#mergeBinaryField}) where docs are always visited in
 * ascending order. Not re-positionable.
 *
 * <p>Typical use:
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
    private int[] docIds;      // resolved docIds when !contiguous
    private int[] slotCounts;  // slot counts per doc when !allSingleSlot
    private int uncompPayloadLen;
    private long payloadAbsOff;

    // Decompressed payload.
    private byte[] payload = new byte[256];
    private boolean payloadLoaded;

    // Cursor state within current block.
    private int docIdx = -1;       // index of current doc within block
    private int payloadCursor = 0; // byte offset in payload[] after the current doc's slots

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
        ensurePayloadLoaded();
        currentDocSlotsOff = payloadCursor;
        currentSlotCount = allSingleSlot ? 1 : slotCounts[docIdx];
        skipNSlots(currentSlotCount);
        currentDocSlotsLen = payloadCursor - currentDocSlotsOff;
        currentDocId = contiguous ? (firstDocIds[currentBlock] + docIdx) : docIds[docIdx];
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

        if (contiguous == false) {
            if (docIds.length < numDocsInBlock) docIds = new int[numDocsInBlock];
            docIds[0] = firstDocIds[blockIdx];
            for (int i = 1; i < numDocsInBlock; i++) {
                docIds[i] = docIds[i - 1] + dataIn.readVInt() + 1;
            }
        }
        if (allSingleSlot == false) {
            if (slotCounts.length < numDocsInBlock) slotCounts = new int[numDocsInBlock];
            for (int i = 0; i < numDocsInBlock; i++) {
                slotCounts[i] = dataIn.readVInt();
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
