/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.util.BitUtil;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FieldArrayContext {

    private final Map<String, Offsets> offsetsPerField = new HashMap<>();

    void recordOffset(String field, String value) {
        Offsets arrayOffsets = offsetsPerField.computeIfAbsent(field, k -> new Offsets());
        int nextOffset = arrayOffsets.currentOffset++;
        var offsets = arrayOffsets.valueToOffsets.computeIfAbsent(value, s -> new ArrayList<>(2));
        offsets.add(nextOffset);
    }

    void recordNull(String field) {
        Offsets arrayOffsets = offsetsPerField.computeIfAbsent(field, k -> new Offsets());
        int nextOffset = arrayOffsets.currentOffset++;
        arrayOffsets.nullValueOffsets.add(nextOffset);
    }

    void maybeRecordEmptyArray(String field) {
        offsetsPerField.computeIfAbsent(field, k -> new Offsets());
    }

    void addToLuceneDocument(DocumentParserContext context) throws IOException {
        for (var entry : offsetsPerField.entrySet()) {
            var fieldName = entry.getKey();
            var offset = entry.getValue();

            int currentOrd = 0;
            // This array allows to retain the original ordering of elements in leaf arrays and retain duplicates.
            int[] offsetToOrd = new int[offset.currentOffset];
            for (var offsetEntry : offset.valueToOffsets.entrySet()) {
                for (var offsetAndLevel : offsetEntry.getValue()) {
                    offsetToOrd[offsetAndLevel] = currentOrd;
                }
                currentOrd++;
            }
            for (var nullOffset : offset.nullValueOffsets) {
                offsetToOrd[nullOffset] = -1;
            }

            try (var streamOutput = new BytesStreamOutput()) {
                // Could just use vint for array length, but this allows for decoding my_field: null as -1
                streamOutput.writeVInt(BitUtil.zigZagEncode(offsetToOrd.length));
                for (int ord : offsetToOrd) {
                    streamOutput.writeVInt(BitUtil.zigZagEncode(ord));
                }
                context.doc().add(new SortedDocValuesField(fieldName, streamOutput.bytes().toBytesRef()));
            }
        }
    }

    static int[] parseOffsetArray(StreamInput in) throws IOException {
        int[] offsetToOrd = new int[BitUtil.zigZagDecode(in.readVInt())];
        for (int i = 0; i < offsetToOrd.length; i++) {
            offsetToOrd[i] = BitUtil.zigZagDecode(in.readVInt());
        }
        return offsetToOrd;
    }

    private static class Offsets {

        int currentOffset;
        // Need to use TreeMap here, so that we maintain the order in which each value (with offset) stored inserted,
        // (which is in the same order the document gets parsed) so we store offsets in right order. This is the same
        // order in what the values get stored in SortedSetDocValues.
        final Map<String, List<Integer>> valueToOffsets = new TreeMap<>();
        final List<Integer> nullValueOffsets = new ArrayList<>(2);

    }

}
