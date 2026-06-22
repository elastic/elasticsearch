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
import org.apache.lucene.util.BytesRef;
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

    protected static final String OFFSETS_FIELD_NAME_SUFFIX = ".offsets";

    // Sentinel ord, representing a slot whose source value was {@code null}.
    public static final int NULL_ORD = -1;

    public static String offsetsFieldName(String fieldName) {
        return fieldName + OFFSETS_FIELD_NAME_SUFFIX;
    }

    public static boolean shouldRecordOffsets(DocumentParserContext context, String offsetsFieldName, boolean multiValue) {
        if (offsetsFieldName == null) {
            return false;
        }

        // Columnar mode relies solely on offsets for array order reconstruction. Record an offset for every indexed value so the offset
        // slots remain aligned with the sorted-set ords during decode, regardless of whether the value's immediate parent was an array.
        if (multiValue && context.indexSettings().getMode().isStrictColumnar()) {
            return true;
        }

        // synthetic_source_keep=arrays path: only record when the value's immediate parent is an array, since out-of-array values are
        // captured by _ignored_source.
        return context.isImmediateParentAnArray() && context.canAddIgnoredField();
    }

    protected final Map<String, Offsets> offsetsPerField = new HashMap<>();

    public void recordOffset(String field, Comparable<?> value) {
        offsetsPerField.computeIfAbsent(field, k -> new Offsets()).recordOffset(value);
    }

    public void recordNull(String field) {
        offsetsPerField.computeIfAbsent(field, k -> new Offsets()).recordNull();
    }

    void maybeRecordEmptyArray(String field) {
        offsetsPerField.computeIfAbsent(field, k -> new Offsets()).markEmptyArray();
    }

    public void addToLuceneDocument(DocumentParserContext context) throws IOException {
        boolean strictlyColumnar = context.indexSettings().getMode().isStrictColumnar();
        for (var entry : offsetsPerField.entrySet()) {
            var offsets = entry.getValue();
            // In strict columnar a single non-null value carries no ordering or shape information beyond what the sorted-set doc
            // values already encode, so the offsets entry is redundant. This means that single-valued arrays are rebuilt as
            // single valued; ex. ["a"] -> "a".
            if (strictlyColumnar && offsets.currentOffset() <= 1 && offsets.hasNulls() == false) {
                continue;
            }
            context.doc().add(new SortedDocValuesField(entry.getKey(), encodeOffsetArray(offsets)));
        }
    }

    /**
     * Encodes per-array offsets (in document parsing order) recording which distinct value occupies each slot in a recorded array.
     * <p>
     * The array takes the form {@code [slot count][ordinal 0][ordinal 1]...}, where {@code slot count} is the number of leaf positions
     * (one per indexed array element, including {@code null}s) and each following ordinal is either a non-negative index into the
     * per-document sorted set of array values, or {@code -1} when that slot is {@code null}. Decoding uses {@link #parseOffsetArray}.
     */
    protected static BytesRef encodeOffsetArray(Offsets offsets) throws IOException {
        // Force inline single-value state to materialize so the table below has a populated valueToOffsets to iterate.
        offsets.startRecording();
        int currentOrd = 0;
        // This array allows to retain the original ordering of elements in leaf arrays and retain duplicates.
        int[] offsetToOrd = new int[offsets.currentOffset()];
        for (var offsetEntry : offsets.valueToOffsets.entrySet()) {
            for (var offsetAndLevel : offsetEntry.getValue()) {
                offsetToOrd[offsetAndLevel] = currentOrd;
            }
            currentOrd++;
        }
        for (var nullOffset : offsets.nullValueOffsets) {
            offsetToOrd[nullOffset] = NULL_ORD;
        }

        int expectedSize = offsetToOrd.length + 1; // Initialize buffer to avoid unnecessary resizing, assume 1 byte per offset + size.
        try (var streamOutput = new BytesStreamOutput(expectedSize)) {
            // Could just use vint for array length, but this allows for decoding my_field: null as -1
            streamOutput.writeVInt(BitUtil.zigZagEncode(offsetToOrd.length));
            for (int ord : offsetToOrd) {
                streamOutput.writeVInt(BitUtil.zigZagEncode(ord));
            }
            return streamOutput.bytes().toBytesRef();
        }
    }

    public static int[] parseOffsetArray(StreamInput in) throws IOException {
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
        return getOffsetsFieldName(
            context,
            indexSourceKeepMode,
            hasDocValues,
            isStored,
            fieldMapperBuilder,
            indexCreatedVersion,
            minSupportedVersionMain,
            false,
            false
        );
    }

    public static String getOffsetsFieldName(
        MapperBuilderContext context,
        Mapper.SourceKeepMode indexSourceKeepMode,
        boolean hasDocValues,
        boolean isStored,
        FieldMapper.Builder fieldMapperBuilder,
        IndexVersion indexCreatedVersion,
        IndexVersion minSupportedVersionMain,
        boolean isStrictColumnar,
        boolean multiValue
    ) {
        var sourceKeepMode = fieldMapperBuilder.sourceKeepMode.orElse(indexSourceKeepMode);

        // source_keep_mode = arrays path
        if (sourceKeepMode == Mapper.SourceKeepMode.ARRAYS && context.isSourceSynthetic() && hasDocValues
        // Skip stored, we will be synthesizing from stored fields, no point to keep track of the offsets
            && isStored == false
            // Skip nested docs - we don't have per-nested-doc offset tracking
            && context.isInNestedContext() == false
            // Skip copy_to and multi fields, supporting that requires more work. copy_to usage is rare in metrics and logging use cases.
            && fieldMapperBuilder.copyTo.copyToFields().isEmpty()
            && fieldMapperBuilder.multiFieldsBuilder.hasMultiFields() == false
            && indexVersionSupportStoringArraysNatively(indexCreatedVersion, minSupportedVersionMain)) {

            return context.buildFullName(offsetsFieldName(fieldMapperBuilder.leafName()));
        }

        // multi_value = true + columnar mode path
        var columnarOffsetsFieldName = getOffsetsFieldName(context, isStrictColumnar, hasDocValues, multiValue, fieldMapperBuilder);
        if (columnarOffsetsFieldName != null) {
            return columnarOffsetsFieldName;
        }

        // Otherwise, offsets won't be recorded
        return null;
    }

    /**
     * Columnar variant of {@link #getOffsetsFieldName}.
     */
    public static String getOffsetsFieldName(
        MapperBuilderContext context,
        boolean isStrictColumnar,
        boolean hasDocValues,
        boolean multiValue,
        FieldMapper.Builder fieldMapperBuilder
    ) {
        // Note, stored fields and nested docs will not be allowed in columnar mode - no need to check them explicitly
        // The offsets sidecar reconstructs array order from the field's own doc values, so it is only meaningful when those are present;
        // a columnar text field that dedups against a plain keyword delegate disables its own doc values and records no offsets here.
        // TODO: copy_to is disabled since copy_to forces _ignored_source to be used for synthetic source, recording offsets in addition
        // to that is a big storage overhead. This will be addressed in a follow up
        if (multiValue
            && hasDocValues
            && isStrictColumnar
            && context.isSourceSynthetic()
            && fieldMapperBuilder.copyTo.copyToFields().isEmpty()) {
            return context.buildFullName(offsetsFieldName(fieldMapperBuilder.leafName()));
        }
        return null;
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

    /**
     * Per-field offsets accumulator. Lazily allocates its backing collections: the first non-null value is stashed in
     * {@link #pendingValue}, and the collections are populated only once a second value, a null, or an empty-array marker arrives.
     * This avoids the {@link TreeMap}/{@link ArrayList} allocation cost for the common case of a field with a single scalar value
     * per document.
     */
    protected static class Offsets {

        // Inline single-value state — non-null while no second value/null/empty-array marker has been recorded yet
        private Comparable<?> pendingValue;

        // Recording state — populated by startRecording()
        private Map<Comparable<?>, List<Integer>> valueToOffsets;

        private List<Integer> nullValueOffsets;
        private int currentOffset;

        public void recordOffset(Comparable<?> value) {
            if (valueToOffsets != null) {
                // Already recording — append at the next slot
                appendValue(value);
            } else if (pendingValue == null) {
                // First value - lazily record it, defer collection allocation until a second arrives
                pendingValue = value;
            } else {
                // Second value - startRecording backfills the inline value at offset 0, then we append at offset 1
                startRecording();
                appendValue(value);
            }
        }

        public void recordNull() {
            startRecording();
            nullValueOffsets.add(currentOffset++);
        }

        public void markEmptyArray() {
            startRecording();
        }

        public int currentOffset() {
            if (valueToOffsets == null) {
                return pendingValue == null ? 0 : 1;
            }
            return currentOffset;
        }

        public boolean hasNulls() {
            return nullValueOffsets != null && nullValueOffsets.isEmpty() == false;
        }

        private void startRecording() {
            if (valueToOffsets != null) {
                // Offsets are already being recorded
                return;
            }

            // TreeMap so iteration order matches the sorted-set doc-values ordering used during reconstruction
            valueToOffsets = new TreeMap<>();
            nullValueOffsets = new ArrayList<>(2);

            // Backfill the previously stored value
            if (pendingValue != null) {
                appendValue(pendingValue);
                pendingValue = null;
            }
        }

        private void appendValue(Comparable<?> value) {
            valueToOffsets.computeIfAbsent(value, k -> new ArrayList<>(2)).add(currentOffset++);
        }
    }

}
