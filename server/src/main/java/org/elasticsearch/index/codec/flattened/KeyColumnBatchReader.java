/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoader.Block;
import org.elasticsearch.index.mapper.BlockLoader.BlockFactory;
import org.elasticsearch.index.mapper.BlockLoader.BytesRefBuilder;
import org.elasticsearch.index.mapper.BlockLoader.Docs;
import org.elasticsearch.index.mapper.BlockLoader.SingletonBytesRefBuilder;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.util.Arrays;

/**
 * Batch reader for a single column of the columnar flattened doc-values format.
 *
 * <p>Implements {@link BlockLoader.OptionalColumnAtATimeReader} so that ES|QL's
 * {@code ValuesSourceReaderOperator} can load a whole page of documents with one forward
 * scan per block, rather than one binary-search-and-seek per document.
 *
 * <p>The output is semantically identical to the per-doc path:
 * <ul>
 *   <li>null slots are dropped;</li>
 *   <li>surviving values are sorted (ascending {@link BytesRef#compareTo}) and deduplicated;</li>
 *   <li>a document with exactly one surviving value emits a plain {@code appendBytesRef} (no
 *       position entry), matching the output of
 *       {@link org.elasticsearch.index.mapper.flattened.KeyedFlattenedDocValuesBlockLoader.BinaryKeyedBlockDocValuesReader}.</li>
 * </ul>
 *
 * <h2>Bulk-copy fast path</h2>
 *
 * <p>When every document in a page has exactly one non-null value (the overwhelmingly common case
 * for ES|QL queries over single-valued fields), and all those documents fall within a single block
 * (common when processing a dense page), the reader copies the entire value run for the page with a
 * single {@link System#arraycopy} and fills the offset table arithmetically — no per-doc I/O or
 * vint decoding at all.
 *
 * <p>The {@link SequentialColumnReader} cursor held by this reader is not closed by this class;
 * the owning producer closes the underlying {@link org.apache.lucene.store.IndexInput} clone.
 */
final class KeyColumnBatchReader implements BlockLoader.OptionalColumnAtATimeReader {

    private final SequentialColumnReader cursor;

    /**
     * Scratch arrays for slot (offset, length) pairs within the current decompressed payload.
     * Grown as needed; re-used across calls to avoid per-call allocation.
     */
    private int[] slotOffsets = new int[8];
    private int[] slotLengths = new int[8];

    /**
     * Scratch arrays for the singleton fast path. {@code singletonBytes} holds packed value
     * bytes; {@code singletonOffsets[i]} is the start byte of position {@code i}'s value, with
     * {@code singletonOffsets[count]} the total packed length. Both arrays are grown lazily and
     * reused across pages, so content is only valid up to the positions set by {@link #tryRead}.
     */
    private byte[] singletonBytes = new byte[256];
    private int[] singletonOffsets = new int[33];

    KeyColumnBatchReader(SequentialColumnReader cursor) {
        this.cursor = cursor;
    }

    @Override
    public Block tryRead(
        BlockFactory factory,
        Docs docs,
        int offset,
        boolean nullsFiltered,
        BlockDocValuesReader.ToDouble toDouble,
        boolean toInt,
        boolean binaryMultiValuedFormat
    ) throws IOException {
        final int count = docs.count() - offset;
        if (singletonOffsets.length < count + 1) {
            singletonOffsets = new int[count + 1];
        }
        singletonOffsets[0] = 0;
        int bytePos = 0;

        int i = offset;
        while (i < docs.count()) {
            final int doc = docs.get(i);
            final int idx = i - offset;

            if (cursor.advance(doc) != doc) {
                // Doc missing from this column — bail to general path.
                return finishWithBytesRefs(factory, docs, i, offset, count, idx, true);
            }
            if (cursor.blockAllSingleSlot() == false || cursor.blockHasNulls()) {
                // Block has multi-slot or null docs — bail to general path for this doc.
                return finishWithBytesRefs(factory, docs, i, offset, count, idx, false);
            }

            // Try to coalesce a maximal run of docs within this block (and in consecutive slot order).
            final int blockLast = cursor.blockLastDocId();
            final int[] vo = cursor.valueOffsets();
            final byte[] payload = cursor.payload();
            int runStart = cursor.firstSlotIndex(); // == docIdx for allSingleSlot blocks
            int runLen = 1;

            while (i + runLen < docs.count()) {
                final int nextDoc = docs.get(i + runLen);
                if (nextDoc > blockLast) break;
                if (cursor.advance(nextDoc) != nextDoc) break;
                if (cursor.blockAllSingleSlot() == false || cursor.blockHasNulls()) break;
                // Check that slot indices are consecutive (no gaps in the doc array within the block).
                if (cursor.firstSlotIndex() != runStart + runLen) break;
                runLen++;
            }

            // Copy the run in one shot.
            final int runValueStart = vo[runStart];
            final int runValueLen = vo[runStart + runLen] - runValueStart;
            if (bytePos + runValueLen > singletonBytes.length) {
                singletonBytes = Arrays.copyOf(singletonBytes, Math.max(singletonBytes.length * 2, bytePos + runValueLen));
            }
            System.arraycopy(payload, runValueStart, singletonBytes, bytePos, runValueLen);
            // Fill per-doc offsets: each doc's value length = vo[runStart+j+1] - vo[runStart+j].
            for (int j = 0; j < runLen; j++) {
                bytePos += vo[runStart + j + 1] - vo[runStart + j];
                singletonOffsets[idx + j + 1] = bytePos;
            }

            i += runLen;
        }

        // Every doc had exactly one non-null value: produce a dense null-free BytesRefVector.
        final byte[] bytes = Arrays.copyOf(singletonBytes, bytePos);
        final int[] offsets = Arrays.copyOf(singletonOffsets, count + 1);
        try (SingletonBytesRefBuilder builder = factory.singletonBytesRefs(count)) {
            return builder.appendBytesRefs(bytes, offsets).build();
        }
    }

    /**
     * Finishes loading after the singleton fast path hit a bail-out condition at doc index {@code i}.
     * Replays the {@code singletonCount} values already collected in {@code singletonBytes} /
     * {@code singletonOffsets} into a general {@link BytesRefBuilder}, handles the current
     * document, then continues with {@link #emitDoc} for the remaining documents.
     *
     * @param cursorMissed {@code true} when {@code cursor.advance(docs.get(i))} skipped past the
     *                     current doc (it has no column entry and the cursor is already beyond it).
     *                     {@code false} when the cursor is still positioned at {@code docs.get(i)},
     *                     so {@link #emitDoc} can read it directly.
     */
    private Block finishWithBytesRefs(
        BlockFactory factory,
        Docs docs,
        int i,
        int offset,
        int count,
        int singletonCount,
        boolean cursorMissed
    ) throws IOException {
        try (BytesRefBuilder builder = factory.bytesRefs(count)) {
            for (int j = 0; j < singletonCount; j++) {
                builder.appendBytesRef(new BytesRef(singletonBytes, singletonOffsets[j], singletonOffsets[j + 1] - singletonOffsets[j]));
            }
            if (cursorMissed) {
                builder.appendNull();
            } else {
                emitDoc(builder);
            }
            for (int k = i + 1; k < docs.count(); k++) {
                final int doc = docs.get(k);
                if (cursor.advance(doc) != doc) {
                    builder.appendNull();
                } else {
                    emitDoc(builder);
                }
            }
            return builder.build();
        }
    }

    /**
     * Emits the current document's slots into {@code builder}.
     *
     * <p>Reads slot lengths and value offsets from the current block's pre-built tables (set up by
     * {@link SequentialColumnReader#advance(int)}), collects non-null values as zero-copy
     * {@link BytesRef} views into the payload, sorts and deduplicates them, then writes:
     * <ul>
     *   <li>0 non-null → {@code appendNull()}</li>
     *   <li>1 non-null → {@code appendBytesRef(value)} (no position entry)</li>
     *   <li>n &gt; 1 non-null → {@code beginPositionEntry()} + n × {@code appendBytesRef} +
     *       {@code endPositionEntry()}</li>
     * </ul>
     */
    private void emitDoc(BytesRefBuilder builder) {
        final byte[] payload = cursor.payload();
        final int[] slotLens = cursor.slotLens();
        final int[] vo = cursor.valueOffsets();
        final int firstSlot = cursor.firstSlotIndex();
        final int slotCount = cursor.slotCount();

        // Collect (offset, length) pairs for all non-null slots.
        int nonNull = 0;
        for (int s = firstSlot; s < firstSlot + slotCount; s++) {
            final int len = slotLens[s];
            if (len < 0) continue; // null slot
            if (nonNull >= slotOffsets.length) {
                slotOffsets = grow(slotOffsets);
                slotLengths = grow(slotLengths);
            }
            slotOffsets[nonNull] = vo[s];
            slotLengths[nonNull] = len;
            nonNull++;
        }

        if (nonNull == 0) {
            builder.appendNull();
            return;
        }

        if (nonNull == 1) {
            builder.appendBytesRef(new BytesRef(payload, slotOffsets[0], slotLengths[0]));
            return;
        }

        // Sort by BytesRef natural order (unsigned byte comparison) using the payload as backing array.
        new InPlaceMergeSorter() {
            @Override
            protected int compare(int a, int b) {
                return compareSlots(payload, slotOffsets[a], slotLengths[a], slotOffsets[b], slotLengths[b]);
            }

            @Override
            protected void swap(int a, int b) {
                int tmp = slotOffsets[a];
                slotOffsets[a] = slotOffsets[b];
                slotOffsets[b] = tmp;
                tmp = slotLengths[a];
                slotLengths[a] = slotLengths[b];
                slotLengths[b] = tmp;
            }
        }.sort(0, nonNull);

        // Deduplicate adjacent equal entries.
        int dedupCount = 1;
        for (int i = 1; i < nonNull; i++) {
            if (compareSlots(payload, slotOffsets[i - 1], slotLengths[i - 1], slotOffsets[i], slotLengths[i]) != 0) {
                slotOffsets[dedupCount] = slotOffsets[i];
                slotLengths[dedupCount] = slotLengths[i];
                dedupCount++;
            }
        }

        if (dedupCount == 1) {
            builder.appendBytesRef(new BytesRef(payload, slotOffsets[0], slotLengths[0]));
            return;
        }

        builder.beginPositionEntry();
        for (int i = 0; i < dedupCount; i++) {
            builder.appendBytesRef(new BytesRef(payload, slotOffsets[i], slotLengths[i]));
        }
        builder.endPositionEntry();
    }

    private static int compareSlots(byte[] bytes, int offA, int lenA, int offB, int lenB) {
        final int minLen = Math.min(lenA, lenB);
        for (int i = 0; i < minLen; i++) {
            final int diff = (bytes[offA + i] & 0xFF) - (bytes[offB + i] & 0xFF);
            if (diff != 0) return diff;
        }
        return lenA - lenB;
    }

    private static int[] grow(int[] arr) {
        final int[] grown = new int[arr.length * 2];
        System.arraycopy(arr, 0, grown, 0, arr.length);
        return grown;
    }
}
