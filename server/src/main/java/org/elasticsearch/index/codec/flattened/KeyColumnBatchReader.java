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
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;

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
        try (BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
            for (int i = offset; i < docs.count(); i++) {
                final int doc = docs.get(i);
                if (cursor.advance(doc) != doc) {
                    // This document has no entry for this key in the column.
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
     * <p>Reads slot-encoded bytes from the decompressed payload (already loaded by
     * {@link SequentialColumnReader#advance(int)}), collects non-null values as zero-copy
     * {@link BytesRef} views into the payload, sorts and deduplicates them, then writes:
     * <ul>
     *   <li>0 non-null → {@code appendNull()}</li>
     *   <li>1 non-null → {@code appendBytesRef(value)} (no position entry)</li>
     *   <li>n > 1 non-null → {@code beginPositionEntry()} + n × {@code appendBytesRef} +
     *       {@code endPositionEntry()}</li>
     * </ul>
     */
    private void emitDoc(BytesRefBuilder builder) {
        final byte[] payload = cursor.payload();
        int pos = cursor.docSlotsOffset();
        final int slotCount = cursor.slotCount();

        // Collect (offset, length) pairs for all non-null slots.
        int nonNull = 0;
        for (int s = 0; s < slotCount; s++) {
            // Each slot: [vint prefix][prefix-1 value bytes]; prefix == 0 means null.
            // The payload is always well-formed so we read unconditionally (no bounds guard).
            int prefix = 0, shift = 0;
            while (true) {
                final int b = payload[pos++] & 0xFF;
                prefix |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            if (prefix == 0) {
                // null slot: no value bytes follow
                continue;
            }
            final int valLen = prefix - 1;
            if (nonNull >= slotOffsets.length) {
                slotOffsets = grow(slotOffsets);
                slotLengths = grow(slotLengths);
            }
            slotOffsets[nonNull] = pos;
            slotLengths[nonNull] = valLen;
            nonNull++;
            pos += valLen;
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
            protected int compare(int i, int j) {
                return compareSlots(payload, slotOffsets[i], slotLengths[i], slotOffsets[j], slotLengths[j]);
            }

            @Override
            protected void swap(int i, int j) {
                int tmp = slotOffsets[i];
                slotOffsets[i] = slotOffsets[j];
                slotOffsets[j] = tmp;
                tmp = slotLengths[i];
                slotLengths[i] = slotLengths[j];
                slotLengths[j] = tmp;
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
