/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

class FlattenedDocValuesSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private final String fieldFullPath;
    private final String keyedFieldFullPath;
    private final String keyedIgnoredValuesFieldFullPath;
    private final String leafName;
    private final boolean usesBinaryDocValues;
    private final boolean usesArrayOrderBinaryDocValues;
    private final List<SourceLoader.SyntheticFieldLoader> mappedSubFieldLoaders;
    private final boolean storeIgnoredFieldsInBinaryDocValues;

    protected FlattenedDocValues docValues = NO_VALUES;

    // ignored values are either stored in doc values or stored fields
    protected FlattenedDocValues ignoredDocValues = NO_VALUES;
    protected List<Object> ignoredStoredValues = List.of();

    private FlattenedFieldMapper.PreserveLeafArrays preserveLeafArrays;
    private final String offsetsFieldName;
    private FlattenedDocValues offsetsDocValues;

    /**
     * Build a loader for flattened fields from either binary or sorted set doc values.
     *
     * @param fieldFullPath                        full path to the original field
     * @param keyedFieldFullPath                   full path to the keyed field to load doc values from
     * @param keyedIgnoredValuesFieldFullPath      full path to the keyed field that stores values that are not present in doc values
     *                                             due to ignore_above
     * @param leafName                             the name of the leaf field to use in the rendered {@code _source}
     * @param usesBinaryDocValues                  whether the values are stored using binary or sorted set doc values
     * @param mappedSubFieldLoaders                synthetic field loaders for mapped sub-fields; their values are
     *                                             written inside the flattened field's object alongside unmapped keys
     * @param storeIgnoredFieldsInBinaryDocValues  whether ignored values are stored in binary doc values instead of stored fields
     * @param preserveLeafArrays                   whether leaf arrays preserve order, duplicates, and nulls
     */
    FlattenedDocValuesSyntheticFieldLoader(
        String fieldFullPath,
        String keyedFieldFullPath,
        @Nullable String keyedIgnoredValuesFieldFullPath,
        String leafName,
        boolean usesBinaryDocValues,
        List<SourceLoader.SyntheticFieldLoader> mappedSubFieldLoaders,
        boolean storeIgnoredFieldsInBinaryDocValues,
        FlattenedFieldMapper.PreserveLeafArrays preserveLeafArrays,
        boolean usesArrayOrderBinaryDocValues
    ) {
        assert storeIgnoredFieldsInBinaryDocValues == false || usesBinaryDocValues
            : "storeIgnoredFieldsInBinaryDocValues requires usesBinaryDocValues";
        assert usesArrayOrderBinaryDocValues == false || usesBinaryDocValues : "usesArrayOrderBinaryDocValues requires usesBinaryDocValues";
        this.fieldFullPath = fieldFullPath;
        this.keyedFieldFullPath = keyedFieldFullPath;
        this.keyedIgnoredValuesFieldFullPath = keyedIgnoredValuesFieldFullPath;
        this.leafName = leafName;
        this.usesBinaryDocValues = usesBinaryDocValues;
        this.usesArrayOrderBinaryDocValues = usesArrayOrderBinaryDocValues;
        this.mappedSubFieldLoaders = mappedSubFieldLoaders;
        this.storeIgnoredFieldsInBinaryDocValues = storeIgnoredFieldsInBinaryDocValues;
        this.preserveLeafArrays = preserveLeafArrays;
        this.offsetsFieldName = FlattenedFieldArrayContext.getFlattenedOffsetsFieldName(fieldFullPath);
    }

    @Override
    public String fieldName() {
        return fieldFullPath;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        if (keyedIgnoredValuesFieldFullPath == null || storeIgnoredFieldsInBinaryDocValues) {
            return Stream.empty();
        }

        Stream<Map.Entry<String, StoredFieldLoader>> flattenedLoaders = Stream.of(Map.entry(keyedIgnoredValuesFieldFullPath, (values) -> {
            ignoredStoredValues = new ArrayList<>();
            ignoredStoredValues.addAll(values);
        }));
        Stream<Map.Entry<String, StoredFieldLoader>> subFieldLoaders = mappedSubFieldLoaders.stream()
            .flatMap(SourceLoader.SyntheticFieldLoader::storedFieldLoaders);
        return Stream.concat(flattenedLoaders, subFieldLoaders);
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        List<DocValuesLoader> allLoaders = new ArrayList<>();

        // Load regular values for this field, if any
        if (usesArrayOrderBinaryDocValues) {
            // Document-order path: read raw binary DV + counts; decode the KeyedArrayOrderInlineNull blob per document.
            BinaryDocValues binary = reader.getBinaryDocValues(keyedFieldFullPath);
            if (binary != null) {
                NumericDocValues counts = reader.getNumericDocValues(
                    keyedFieldFullPath + MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX
                );
                docValues = new DocumentOrderKeyedFlattenedDocValues(binary, counts);
                allLoaders.add(docValues);
            } else {
                docValues = NO_VALUES;
            }
            offsetsDocValues = NO_VALUES;  // no .offsets sidecar in document-order mode
        } else if (usesBinaryDocValues) {
            var binaryDv = reader.getBinaryDocValues(keyedFieldFullPath);
            if (binaryDv != null) {
                SortedBinaryDocValues dv = MultiValuedSortedBinaryDocValues.fromMultiValued(reader, keyedFieldFullPath, binaryDv);
                docValues = new MultiValuedBinaryFlattenedDocValues(dv);
                allLoaders.add(docValues);
            } else {
                docValues = NO_VALUES;
            }

            {
                var offsetsBinaryDv = reader.getBinaryDocValues(offsetsFieldName);
                if (offsetsBinaryDv != null) {
                    SortedBinaryDocValues offsetsDv = MultiValuedSortedBinaryDocValues.from(reader, offsetsFieldName);
                    offsetsDocValues = new MultiValuedBinaryFlattenedDocValues(offsetsDv);
                    allLoaders.add(offsetsDocValues);
                } else {
                    offsetsDocValues = NO_VALUES;
                }
            }
        } else {
            final SortedSetDocValues dv = DocValues.getSortedSet(reader, keyedFieldFullPath);
            if (dv.getValueCount() > 0) {
                docValues = new SortedSetFlattenedDocValues(dv);
                allLoaders.add(docValues);
            } else {
                docValues = NO_VALUES;
            }
            {
                var binaryDv = reader.getBinaryDocValues(offsetsFieldName);
                if (binaryDv != null) {
                    SortedBinaryDocValues offsetsDv = MultiValuedSortedBinaryDocValues.from(reader, offsetsFieldName);
                    offsetsDocValues = new MultiValuedBinaryFlattenedDocValues(offsetsDv);
                    allLoaders.add(offsetsDocValues);
                } else {
                    offsetsDocValues = NO_VALUES;
                }
            }
        }

        for (SourceLoader.SyntheticFieldLoader subFieldLoader : mappedSubFieldLoaders) {
            DocValuesLoader subFieldDvLoader = subFieldLoader.docValuesLoader(reader, docIdsInLeaf);
            if (subFieldDvLoader != null) {
                allLoaders.add(subFieldDvLoader);
            }
        }

        // Load ignored values for this field, if any
        if (storeIgnoredFieldsInBinaryDocValues && keyedIgnoredValuesFieldFullPath != null) {
            var binaryDv = reader.getBinaryDocValues(keyedIgnoredValuesFieldFullPath);
            if (binaryDv != null) {
                SortedBinaryDocValues dv = MultiValuedSortedBinaryDocValues.fromMultiValued(
                    reader,
                    keyedIgnoredValuesFieldFullPath,
                    binaryDv
                );
                ignoredDocValues = new MultiValuedBinaryFlattenedDocValues(dv);
                allLoaders.add(ignoredDocValues);
            } else {
                ignoredDocValues = NO_VALUES;
            }
        }

        // There are no doc values associated with this field
        if (allLoaders.isEmpty()) {
            return null;
        }

        // There is exactly one loader
        if (allLoaders.size() == 1) {
            return allLoaders.getFirst();
        }

        // Combine all loaders
        return docId -> {
            boolean any = false;
            for (DocValuesLoader loader : allLoaders) {
                any |= loader.advanceToDoc(docId);
            }
            return any;
        };
    }

    @Override
    public boolean hasValue() {
        return hasFlattenedValues() || hasPropertyValues();
    }

    protected boolean hasFlattenedValues() {
        boolean hasIgnoredValues = ignoredStoredValues.isEmpty() == false || ignoredDocValues.count() > 0;
        return docValues.count() > 0 || offsetsDocValues.count() > 0 || hasIgnoredValues;
    }

    private boolean hasPropertyValues() {
        for (SourceLoader.SyntheticFieldLoader loader : mappedSubFieldLoaders) {
            if (loader.hasValue()) {
                return true;
            }
        }
        return false;
    }

    protected FlattenedFieldSyntheticWriterHelper getWriter(List<SourceLoader.SyntheticFieldLoader> subFieldLoaders) throws IOException {
        String parentPrefix = fieldFullPath + ".";
        List<Map.Entry<String, SourceLoader.SyntheticFieldLoader>> sortedSubFieldEntries = new ArrayList<>();
        for (SourceLoader.SyntheticFieldLoader loader : subFieldLoaders) {
            if (loader.hasValue()) {
                sortedSubFieldEntries.add(Map.entry(loader.fieldName().substring(parentPrefix.length()), loader));
            }
        }
        return new FlattenedFieldSyntheticWriterHelper(getKeyedValueProducer(), sortedSubFieldEntries);
    }

    private FlattenedFieldSyntheticWriterHelper.KeyedValueProducer getKeyedValueProducer() throws IOException {
        List<BytesRef> rawIgnored = collectIgnoredValues();

        if (usesArrayOrderBinaryDocValues) {
            TreeMap<String, List<String>> slotsByKey = (docValues instanceof DocumentOrderKeyedFlattenedDocValues dv)
                ? dv.slotsByKey
                : new TreeMap<>();
            TreeMap<String, List<String>> ignoredByKey = new TreeMap<>();
            if (rawIgnored != null) {
                for (BytesRef kv : rawIgnored) {
                    String key = FlattenedFieldParser.extractKey(kv).utf8ToString();
                    ignoredByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(FlattenedFieldParser.extractValue(kv).utf8ToString());
                }
            }
            return new FlattenedFieldSyntheticWriterHelper.ArrayOrderKeyedValueProducer(slotsByKey, ignoredByKey);
        }

        // Legacy path: wrap in a TreeSet to restore sorted-unique order for merge with doc values.
        TreeSet<BytesRef> ignoredKeyedValues = rawIgnored != null ? new TreeSet<>(rawIgnored) : null;

        FlattenedFieldSyntheticWriterHelper.SortedKeyedValues sortedKeyedValues = ((SortedKeyedFlattenedDocValues) docValues).getValues();
        if (ignoredKeyedValues != null) {
            sortedKeyedValues = new DocValuesWithIgnoredSortedKeyedValues(sortedKeyedValues, ignoredKeyedValues);
        }
        final var offsetsValues = ((SortedKeyedFlattenedDocValues) offsetsDocValues).getValues();
        FlattenedFieldSyntheticWriterHelper.SortedOffsetValues keyedOffsetFieldSupplier = () -> {
            var value = offsetsValues.next();
            return value != null ? FlattenedFieldArrayContext.parseOffsetField(value) : null;
        };
        return new FlattenedFieldSyntheticWriterHelper.OffsetKeyedValueProducer(sortedKeyedValues, keyedOffsetFieldSupplier);
    }

    private List<BytesRef> collectIgnoredValues() throws IOException {
        if (storeIgnoredFieldsInBinaryDocValues) {
            if (ignoredDocValues.count() > 0) {
                var result = new ArrayList<BytesRef>();
                var values = ((SortedKeyedFlattenedDocValues) ignoredDocValues).getValues();
                for (int i = 0; i < ignoredDocValues.count(); i++) {
                    result.add(BytesRef.deepCopyOf(values.next()));
                }
                return result;
            }
        } else {
            if (ignoredStoredValues.isEmpty() == false) {
                var result = new ArrayList<BytesRef>();
                for (Object value : ignoredStoredValues) {
                    result.add((BytesRef) value);
                }
                ignoredStoredValues = List.of();
                return result;
            }
        }
        return null;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        boolean hasFlattenedValues = hasFlattenedValues();
        if (hasFlattenedValues == false && hasPropertyValues() == false) {
            return;
        }

        b.startObject(leafName);
        getWriter(mappedSubFieldLoaders).writeNested(b);
        b.endObject();
    }

    @Override
    public void reset() {
        ignoredStoredValues = List.of();
        for (SourceLoader.SyntheticFieldLoader loader : mappedSubFieldLoaders) {
            loader.reset();
        }
    }

    protected List<SourceLoader.SyntheticFieldLoader> getMappedSubFieldLoaders() {
        return mappedSubFieldLoaders;
    }

    /**
     * An abstraction over different Lucene doc values formats ({@link SortedSetDocValues} and {@link SortedBinaryDocValues}) that provides
     * a uniform way to position on a document and read its keyed values. This allows the rest of the loader to work with keyed doc values
     * without caring about the underlying storage format.
     */
    interface FlattenedDocValues extends DocValuesLoader {
        boolean advanceToDoc(int docId) throws IOException;

        int count();
    }

    /**
     * A {@link FlattenedDocValues} that additionally exposes its values as a sorted keyed stream. Implemented by the
     * legacy SORTED_UNIQUE decoders; not implemented by {@link DocumentOrderKeyedFlattenedDocValues}, which exposes
     * its slots via {@link DocumentOrderKeyedFlattenedDocValues#slotsByKey} instead.
     */
    interface SortedKeyedFlattenedDocValues extends FlattenedDocValues {
        FlattenedFieldSyntheticWriterHelper.SortedKeyedValues getValues();
    }

    private static final SortedKeyedFlattenedDocValues NO_VALUES = new SortedKeyedFlattenedDocValues() {
        @Override
        public boolean advanceToDoc(int docId) {
            return false;
        }

        @Override
        public int count() {
            return 0;
        }

        @Override
        public FlattenedFieldSyntheticWriterHelper.SortedKeyedValues getValues() {
            return () -> null;
        }
    };

    private static final class SortedSetFlattenedDocValues implements SortedKeyedFlattenedDocValues {
        private final SortedSetDocValues docValues;
        private boolean hasValue;

        SortedSetFlattenedDocValues(SortedSetDocValues docValues) {
            this.docValues = docValues;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            return hasValue = docValues.advanceExact(docId);
        }

        @Override
        public int count() {
            return hasValue ? docValues.docValueCount() : 0;
        }

        @Override
        public FlattenedFieldSyntheticWriterHelper.SortedKeyedValues getValues() {
            return new FlattenedFieldSyntheticWriterHelper.SortedKeyedValues() {
                private int seen = 0;

                @Override
                public BytesRef next() throws IOException {
                    if (seen < count()) {
                        seen += 1;
                        return docValues.lookupOrd(docValues.nextOrd());
                    }
                    return null;
                }
            };
        }
    }

    private static final class MultiValuedBinaryFlattenedDocValues implements SortedKeyedFlattenedDocValues {
        private final SortedBinaryDocValues docValues;
        private boolean hasValue = false;

        MultiValuedBinaryFlattenedDocValues(SortedBinaryDocValues docValues) {
            this.docValues = docValues;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            return hasValue = docValues.advanceExact(docId);
        }

        @Override
        public int count() {
            return hasValue ? docValues.docValueCount() : 0;
        }

        @Override
        public FlattenedFieldSyntheticWriterHelper.SortedKeyedValues getValues() {
            return new FlattenedFieldSyntheticWriterHelper.SortedKeyedValues() {
                private int seen = 0;

                @Override
                public BytesRef next() throws IOException {
                    if (seen < count()) {
                        seen += 1;
                        return docValues.nextValue();
                    }
                    return null;
                }
            };
        }
    }

    /**
     * Decodes a {@link MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull} binary doc-values blob per document,
     * grouping each slot by its key into a {@link TreeMap} in document order. Null slots are represented as a
     * {@code null} element in the per-key list. Every slot uses the uniform {@code [valueLen+1]key\0value} encoding;
     * a prefix of {@code 0} marks a null slot ({@code [0]key\0}).
     */
    private static final class DocumentOrderKeyedFlattenedDocValues implements FlattenedDocValues {
        // Note: does not implement SortedKeyedFlattenedDocValues — slots are accessed directly via slotsByKey.

        private final BinaryDocValues binary;
        private final NumericDocValues counts;
        private final ByteArrayStreamInput in = new ByteArrayStreamInput();

        /**
         * Slots grouped by key in document order; a {@code null} element represents a null slot.
         * Populated on each call to {@link #advanceToDoc}.
         */
        private final TreeMap<String, List<String>> slotsByKey = new TreeMap<>();
        private int totalSlotCount;

        DocumentOrderKeyedFlattenedDocValues(BinaryDocValues binary, NumericDocValues counts) {
            this.binary = binary;
            this.counts = counts;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            slotsByKey.clear();
            totalSlotCount = 0;

            if (counts == null || counts.advanceExact(docId) == false) {
                return false;
            }
            int slotCount = Math.toIntExact(counts.longValue());

            if (binary.advanceExact(docId) == false) {
                return false;
            }
            BytesRef bytes = binary.binaryValue();

            decodeSlots(bytes, slotCount);
            totalSlotCount = slotCount;
            return slotCount > 0;
        }

        private void decodeSlots(BytesRef bytes, int slotCount) throws IOException {
            in.reset(bytes.bytes, bytes.offset, bytes.length);
            for (int i = 0; i < slotCount; i++) {
                int prefix = in.readVInt();
                int slotKeyStart = in.getPosition();
                int remaining = bytes.length - (slotKeyStart - bytes.offset);
                int keyLen = ESVectorUtil.indexOf(bytes.bytes, slotKeyStart, remaining, (byte) 0);
                String key = new String(bytes.bytes, slotKeyStart, keyLen, StandardCharsets.UTF_8);
                if (prefix == 0) {
                    // Null slot: [0]key\0
                    in.setPosition(slotKeyStart + keyLen + 1);
                    slotsByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(null);
                } else {
                    // Non-null slot: [valueLen+1]key\0value
                    int valueLen = prefix - 1;
                    int valueStart = slotKeyStart + keyLen + 1;
                    in.setPosition(valueStart + valueLen);
                    slotsByKey.computeIfAbsent(key, k -> new ArrayList<>())
                        .add(new BytesRef(bytes.bytes, valueStart, valueLen).utf8ToString());
                }
            }
        }

        @Override
        public int count() {
            return totalSlotCount;
        }
    }

    private static class DocValuesWithIgnoredSortedKeyedValues implements FlattenedFieldSyntheticWriterHelper.SortedKeyedValues {
        private final FlattenedFieldSyntheticWriterHelper.SortedKeyedValues docValues;
        private final TreeSet<BytesRef> ignoredStoredValues;

        private BytesRef currentFromDocValues;

        private DocValuesWithIgnoredSortedKeyedValues(
            FlattenedFieldSyntheticWriterHelper.SortedKeyedValues docValues,
            TreeSet<BytesRef> ignoredStoredValues
        ) {
            this.docValues = docValues;
            this.ignoredStoredValues = ignoredStoredValues;
        }

        /**
         * Returns next keyed field value to be included in synthetic source.
         * This function merges keyed values from doc values and ignored values (due to ignore_above)
         * that are loaded from stored fields and provided as input.
         * Sort order of keyed values is preserved during merge so the output is the same as if
         * it was using only doc values.
         * @return
         * @throws IOException
         */
        @Override
        public BytesRef next() throws IOException {
            if (currentFromDocValues == null) {
                currentFromDocValues = docValues.next();
            }

            if (ignoredStoredValues.isEmpty() == false) {
                BytesRef ignoredCandidate = ignoredStoredValues.first();
                if (currentFromDocValues == null || ignoredCandidate.compareTo(currentFromDocValues) <= 0) {
                    ignoredStoredValues.pollFirst();
                    return ignoredCandidate;
                }
            }
            if (currentFromDocValues == null) {
                return null;
            }

            var toReturn = currentFromDocValues;
            currentFromDocValues = null;
            return toReturn;
        }
    }

}
