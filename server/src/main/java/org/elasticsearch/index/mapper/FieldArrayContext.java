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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FieldArrayContext {

    public static final String OFFSETS_FIELD_NAME_SUFFIX = ".offsets";

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

    // Offsets are tracked per target document so array order is recorded on the document the values were parsed into. In
    // particular, a multi-valued leaf inside a nested object records its offsets on that nested child document, not on the
    // root, so the order is reconstructed when the child document is read back. The current document is set on every
    // getOffSetContext() call (see DocumentParserContext), so it always reflects the document being parsed.
    protected final Map<LuceneDocument, Map<String, Offsets>> offsetsPerDoc = new HashMap<>();
    private LuceneDocument currentDoc;

    public void setCurrentDoc(LuceneDocument doc) {
        this.currentDoc = doc;
    }

    private Offsets offsetsFor(String field) {
        return offsetsPerDoc.computeIfAbsent(currentDoc, d -> new HashMap<>()).computeIfAbsent(field, k -> new Offsets());
    }

    public void recordOffset(String field, Comparable<?> value) {
        offsetsFor(field).recordOffset(value);
    }

    /**
     * Like {@link #recordOffset(String, Comparable)}, but also registers a writer that converts a recorded array-slot value
     * back to a form suitable for {@code _source}, so that, when the encoded offsets turn out too large for a sorted doc
     * value, the array can be rebuilt and stored in {@code _ignored_source} instead of failing the document. Since mappers
     * record their doc-values representation (e.g. sortable longs for numbers), the writer must produce the same
     * representation that the field's offsets-aware synthetic source loader produces, so that the {@code _ignored_source}
     * fallback is indistinguishable from the native array reconstruction.
     */
    public void recordOffset(
        String field,
        Comparable<?> value,
        CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter
    ) {
        var offsets = offsetsFor(field);
        assert offsets.fallbackWriter == null || offsets.fallbackWriter == fallbackWriter
            : "a field must always record offsets with the same fallback writer";
        offsets.fallbackWriter = fallbackWriter;
        offsets.recordOffset(value);
    }

    public void recordNull(String field) {
        offsetsFor(field).recordNull();
    }

    /**
     * Like {@link #recordNull(String)}, but also registers a fallback writer; see
     * {@link #recordOffset(String, Comparable, CheckedBiConsumer)}.
     */
    public void recordNull(String field, CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter) {
        var offsets = offsetsFor(field);
        assert offsets.fallbackWriter == null || offsets.fallbackWriter == fallbackWriter
            : "a field must always record offsets with the same fallback writer";
        offsets.fallbackWriter = fallbackWriter;
        offsets.recordNull();
    }

    /**
     * Records a value of {@code field} that was not indexed, because of {@code ignore_above} or {@code ignore_malformed}, and so
     * occupies no offsets slot. Such values live in field-specific fallback storage that the field's synthetic source loader
     * reads back, but an {@code _ignored_source} entry for the field suppresses that loader at load time. So when the encoded
     * offsets turn out too large for a sorted doc value and the array is moved to {@code _ignored_source} (see
     * {@link #recordOffset(String, Comparable, CheckedBiConsumer)}), the recorded ignored values are written into that entry at
     * their original position in the array. They are not part of the encoded offsets.
     *
     * @param encodedValue the ignored value, encoded like {@link XContentDataHelper#encodeToken} does
     */
    public void recordIgnored(String field, BytesRef encodedValue) {
        offsetsFor(field).recordIgnored(encodedValue);
    }

    void maybeRecordEmptyArray(String field) {
        offsetsFor(field).markEmptyArray();
    }

    public void addToLuceneDocument(DocumentParserContext context) throws IOException {
        boolean strictlyColumnar = context.indexSettings().getMode().isStrictColumnar();
        for (var docEntry : offsetsPerDoc.entrySet()) {
            // A null key means no document was set while recording (e.g. direct unit-test usage); fall back to the document
            // being flushed. During real parsing the current document is always set, so each entry targets its own document.
            LuceneDocument doc = docEntry.getKey() != null ? docEntry.getKey() : context.doc();
            for (var entry : docEntry.getValue().entrySet()) {
                var offsets = entry.getValue();
                // An array whose values were all ignored (ignore_above, ignore_malformed) recorded ignored values but no slot.
                // Writing offsets for it would read back as an empty-array marker; the ignored values are synthesized from the
                // field's own storage of ignored values instead, like before ignored values were recorded here.
                if (offsets.hasRecordedSlots() == false) {
                    continue;
                }
                // In strict columnar a single non-null value carries no ordering or shape information beyond what the sorted-set doc
                // values already encode, so the offsets entry is redundant. This means that single-valued arrays are rebuilt as
                // single valued; ex. ["a"] -> "a".
                if (strictlyColumnar && offsets.currentOffset() <= 1 && offsets.hasNulls() == false) {
                    continue;
                }
                BytesRef encoded = encodeOffsetArray(offsets);
                if (encoded.length <= IndexWriter.MAX_TERM_LENGTH) {
                    doc.add(new SortedDocValuesField(entry.getKey(), encoded));
                } else {
                    // Lucene rejects sorted doc values larger than MAX_TERM_LENGTH, and the encoding takes 1-5 bytes per array
                    // slot, so a very large leaf array can exceed the limit. Rather than failing the whole document, keep the
                    // array in _ignored_source when possible; otherwise reject the document with a clearer and earlier error
                    // than the one Lucene would produce at indexing time.
                    storeArrayInIgnoredSource(context, doc, entry.getKey(), offsets, encoded.length);
                }
            }
        }
    }

    /**
     * Fallback for a leaf array whose encoded offsets exceed {@link IndexWriter#MAX_TERM_LENGTH}: rebuilds the array from the
     * recorded offsets, in original element order, and stores it in {@code _ignored_source}. The {@code _ignored_source} entry
     * takes precedence over the field's own synthetic source loader at load time (see {@code ObjectMapper}'s synthetic source
     * field loader), so the values are not synthesized twice. Since that also suppresses the field-specific storage of values
     * that were ignored (e.g. {@code ignore_above} or {@code ignore_malformed}), the ignored values recorded via
     * {@link #recordIgnored(String, BytesRef)} are written into the entry too, at their original position. When the array cannot
     * be preserved in {@code _ignored_source} the document is rejected, like it was before this fallback existed, but with an
     * earlier and clearer error than the one Lucene produces for an oversized sorted doc value.
     */
    private static void storeArrayInIgnoredSource(
        DocumentParserContext context,
        LuceneDocument doc,
        String offsetsFieldName,
        Offsets offsets,
        int encodedSize
    ) throws IOException {
        CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter = offsets.fallbackWriter;
        if (fallbackWriter == null || context.canAddIgnoredField() == false) {
            // Either the recording mapper registered no way to rebuild the recorded values, or _ignored_source is unavailable
            // (strict columnar mode or ignored source writes disabled).
            throw new IllegalArgumentException(
                oversizedOffsetsMessage(offsetsFieldName, encodedSize) + " and the array cannot be preserved in [_ignored_source]"
            );
        }
        assert offsetsFieldName.endsWith(OFFSETS_FIELD_NAME_SUFFIX) : offsetsFieldName;
        String fieldName = offsetsFieldName.substring(0, offsetsFieldName.length() - OFFSETS_FIELD_NAME_SUFFIX.length());
        List<Offsets.IgnoredValue> ignoredValues = offsets.ignoredValues();
        if (ignoredValues.isEmpty() && context.isFieldIgnored(fieldName)) {
            // Some values of this field were not indexed (e.g. ignore_above or ignore_malformed), but the mapper did not record
            // them here. They live in field-specific fallback storage that the _ignored_source entry would suppress at load
            // time, so the array cannot be preserved in _ignored_source without losing those values.
            throw new IllegalArgumentException(
                oversizedOffsetsMessage(offsetsFieldName, encodedSize)
                    + " and the array cannot be preserved in [_ignored_source] because some values of ["
                    + fieldName
                    + "] were ignored"
            );
        }
        // Force inline single-value state to materialize, mirroring encodeOffsetArray. In practice an array large enough to get
        // here is always recording already.
        offsets.startRecording();
        Comparable<?>[] slotValues = new Comparable<?>[offsets.currentOffset()];
        for (var valueEntry : offsets.valueToOffsets.entrySet()) {
            for (int slot : valueEntry.getValue()) {
                slotValues[slot] = valueEntry.getKey();
            }
        }
        String leafName = leafName(context, fieldName);
        // Slots recorded via recordNull keep their null and are written as explicit null values below. Ignored values have no
        // slot; each one is written before the slot it was recorded ahead of, which restores its original position.
        try (XContentBuilder builder = XContentBuilder.builder(context.parser().contentType().xContent())) {
            builder.startArray();
            int nextIgnored = 0;
            for (int slot = 0; slot < slotValues.length; slot++) {
                while (nextIgnored < ignoredValues.size() && ignoredValues.get(nextIgnored).slot() == slot) {
                    XContentDataHelper.decodeAndWrite(builder, ignoredValues.get(nextIgnored++).encodedValue());
                }
                Comparable<?> slotValue = slotValues[slot];
                if (slotValue == null) {
                    builder.nullValue();
                } else {
                    fallbackWriter.accept(builder, slotValue);
                }
            }
            // Ignored values that came after the last slot.
            for (; nextIgnored < ignoredValues.size(); nextIgnored++) {
                XContentDataHelper.decodeAndWrite(builder, ignoredValues.get(nextIgnored).encodedValue());
            }
            builder.endArray();
            int parentOffset = fieldName.lastIndexOf(leafName);
            context.addIgnoredField(
                new IgnoredSourceFieldMapper.NameValue(fieldName, parentOffset, XContentDataHelper.encodeXContentBuilder(builder), doc)
            );
        }
    }

    /**
     * Resolves the leaf name of the field that recorded offsets, needed to compute the parent offset of the
     * {@link IgnoredSourceFieldMapper.NameValue} entry. The leaf name cannot be parsed out of the full path, since under
     * {@code subobjects: false} the leaf name itself can contain dots. The mapping lookup does not contain fields that were
     * mapped dynamically while parsing the current document, so those are resolved through the context's dynamic mappers.
     */
    private static String leafName(DocumentParserContext context, String fieldName) {
        var fieldMapper = context.mappingLookup().getMapper(fieldName);
        if (fieldMapper != null) {
            return fieldMapper.leafName();
        }
        var dynamicMappers = context.getDynamicMappers(fieldName);
        assert dynamicMappers != null && dynamicMappers.isEmpty() == false : "no mapper for field [" + fieldName + "]";
        return dynamicMappers.getFirst().leafName();
    }

    /**
     * The error for a leaf array whose encoded offsets exceed {@link IndexWriter#MAX_TERM_LENGTH}, shared with the columnar
     * batch path ({@code ColumnarOffsetsBuilder}) so both paths report the condition the same way.
     */
    public static String oversizedOffsetsMessage(String offsetsFieldName, int encodedSize) {
        return "encoded offsets of field ["
            + offsetsFieldName
            + "] are too large to store (["
            + encodedSize
            + "] bytes, limit ["
            + IndexWriter.MAX_TERM_LENGTH
            + "])";
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
        return getOffsetsFieldName(
            context,
            indexSourceKeepMode,
            hasDocValues,
            isStored,
            fieldMapperBuilder,
            indexVersionSupportStoringArraysNatively(indexCreatedVersion, minSupportedVersionMain),
            isStrictColumnar,
            multiValue
        );
    }

    /**
     * Variant of {@link #getOffsetsFieldName(MapperBuilderContext, Mapper.SourceKeepMode, boolean, boolean, FieldMapper.Builder,
     * IndexVersion, IndexVersion, boolean, boolean)} for field types whose index version gate does not follow the shared
     * {@link #indexVersionSupportStoringArraysNatively} rule, for example because the field type never recorded offsets in the 8.x
     * backport range or has its own backport versions. The caller evaluates the gate and passes the result as
     * {@code indexVersionSupportsNativeArrays}.
     */
    public static String getOffsetsFieldName(
        MapperBuilderContext context,
        Mapper.SourceKeepMode indexSourceKeepMode,
        boolean hasDocValues,
        boolean isStored,
        FieldMapper.Builder fieldMapperBuilder,
        boolean indexVersionSupportsNativeArrays,
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
            && indexVersionSupportsNativeArrays) {

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
        // Note, stored fields will not be allowed in columnar mode - no need to check them explicitly.
        // The offsets sidecar reconstructs array order from the field's own doc values, so it is only meaningful when those are present;
        // a columnar text field that dedups against a plain keyword delegate disables its own doc values and records no offsets here.
        // Offsets are recorded per document (see FieldArrayContext), so a leaf inside a nested object records them on its child
        // document and array order is preserved there too.
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

        // Registered via the fallback-aware record overloads. Used to rebuild the array for _ignored_source when the
        // encoded offsets are too large for a sorted doc value.
        private CheckedBiConsumer<XContentBuilder, Comparable<?>, IOException> fallbackWriter;

        // Values that were not indexed (ignore_above, ignore_malformed) and so occupy no slot. Only used to rebuild the array
        // for _ignored_source when the encoded offsets are too large for a sorted doc value; not part of the encoded offsets.
        private List<IgnoredValue> ignoredValues;

        /**
         * An ignored value and the slot it was recorded ahead of, i.e. the number of slots recorded before it. Equals the total
         * slot count when the ignored value came after the last slot.
         */
        record IgnoredValue(int slot, BytesRef encodedValue) {}

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

        public void recordIgnored(BytesRef encodedValue) {
            if (ignoredValues == null) {
                ignoredValues = new ArrayList<>(2);
            }
            // currentOffset() also counts the inline pending value, which startRecording() later backfills at slot 0.
            ignoredValues.add(new IgnoredValue(currentOffset(), encodedValue));
        }

        /** The ignored values in recording order, so by non-decreasing {@link IgnoredValue#slot()}. */
        List<IgnoredValue> ignoredValues() {
            return ignoredValues == null ? List.of() : ignoredValues;
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

        /**
         * Whether a value, a null or an empty-array marker was recorded. False when only ignored values were recorded via
         * {@link #recordIgnored(BytesRef)}, in which case there is nothing to encode.
         */
        public boolean hasRecordedSlots() {
            return valueToOffsets != null || pendingValue != null;
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
