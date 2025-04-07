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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FieldArrayContext {

    private static final String OFFSETS_FIELD_NAME_SUFFIX = ".offsets";
    private final Map<String, Offsets> offsetsPerField = new HashMap<>();

    public void recordOffset(String field, Comparable<?> value) {
        Offsets arrayOffsets = offsetsPerField.computeIfAbsent(field, k -> new Offsets());
        int nextOffset = arrayOffsets.currentOffset++;
        var offsets = arrayOffsets.valueToOffsets.computeIfAbsent(value, s -> new ArrayList<>(2));
        offsets.add(nextOffset);
    }

    public void recordNull(String field) {
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

            int expectedSize = offsetToOrd.length + 1; // Initialize buffer to avoid unnecessary resizing, assume 1 byte per offset + size.
            try (var streamOutput = new BytesStreamOutput(expectedSize)) {
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

    public static String getOffsetsFieldName(
        MapperBuilderContext context,
        Mapper.SourceKeepMode indexSourceKeepMode,
        boolean hasDocValues,
        boolean isStored,
        FieldMapper.Builder fieldMapperBuilder,
        IndexVersion indexCreatedVersion,
        IndexVersion minSupportedVersionMain
    ) {
        var sourceKeepMode = fieldMapperBuilder.sourceKeepMode.orElse(indexSourceKeepMode);
        if (context.isSourceSynthetic()
            && sourceKeepMode == Mapper.SourceKeepMode.ARRAYS
            && hasDocValues
            && isStored == false
            && context.isInNestedContext() == false
            && fieldMapperBuilder.copyTo.copyToFields().isEmpty()
            && fieldMapperBuilder.multiFieldsBuilder.hasMultiFields() == false
            && indexVersionSupportStoringArraysNatively(indexCreatedVersion, minSupportedVersionMain)) {
            // Skip stored, we will be synthesizing from stored fields, no point to keep track of the offsets
            // Skip copy_to and multi fields, supporting that requires more work. However, copy_to usage is rare in metrics and
            // logging use cases

            // keep track of value offsets so that we can reconstruct arrays from doc values in order as was specified during indexing
            // (if field is stored then there is no point of doing this)
            return context.buildFullName(fieldMapperBuilder.leafName() + FieldArrayContext.OFFSETS_FIELD_NAME_SUFFIX);
        } else {
            return null;
        }
    }

    private static boolean indexVersionSupportStoringArraysNatively(
        IndexVersion indexCreatedVersion,
        IndexVersion minSupportedVersionMain
    ) {
        return indexCreatedVersion.onOrAfter(minSupportedVersionMain)
            || indexCreatedVersion.between(
                IndexVersions.SYNTHETIC_SOURCE_STORE_ARRAYS_NATIVELY_BACKPORT_8_X,
                IndexVersions.UPGRADE_TO_LUCENE_10_0_0
            );
    }

    private static class Offsets {

        int currentOffset;
        // Need to use TreeMap here, so that we maintain the order in which each value (with offset) stored inserted,
        // (which is in the same order the document gets parsed) so we store offsets in right order. This is the same
        // order in what the values get stored in SortedSetDocValues.
        final Map<Comparable<?>, List<Integer>> valueToOffsets = new TreeMap<>();
        final List<Integer> nullValueOffsets = new ArrayList<>(2);

    }

}
