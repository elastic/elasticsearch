/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;

class FlattenedDocValuesSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private final String fieldFullPath;
    private final String keyedFieldFullPath;
    private final String keyedIgnoredValuesFieldFullPath;
    private final String leafName;
    private final boolean usesBinaryDocValues;
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
        FlattenedFieldMapper.PreserveLeafArrays preserveLeafArrays
    ) {
        assert storeIgnoredFieldsInBinaryDocValues == false || usesBinaryDocValues
            : "storeIgnoredFieldsInBinaryDocValues requires usesBinaryDocValues";
        this.fieldFullPath = fieldFullPath;
        this.keyedFieldFullPath = keyedFieldFullPath;
        this.keyedIgnoredValuesFieldFullPath = keyedIgnoredValuesFieldFullPath;
        this.leafName = leafName;
        this.usesBinaryDocValues = usesBinaryDocValues;
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
        if (usesBinaryDocValues) {
            var binaryDv = reader.getBinaryDocValues(keyedFieldFullPath);
            if (binaryDv != null) {
                SortedBinaryDocValues dv = MultiValuedSortedBinaryDocValues.fromMultiValued(reader, keyedFieldFullPath, binaryDv);
                docValues = new MultiValuedBinaryFlattenedDocValues(dv);
                allLoaders.add(docValues);
            } else {
                docValues = NO_VALUES;
            }
        } else {
            final SortedSetDocValues dv = DocValues.getSortedSet(reader, keyedFieldFullPath);
            if (dv.getValueCount() > 0) {
                docValues = new SortedSetFlattenedDocValues(dv);
                allLoaders.add(docValues);
            } else {
                docValues = NO_VALUES;
            }
        }

        {
            var binaryDv = reader.getBinaryDocValues(offsetsFieldName);
            if (binaryDv != null) {
                SortedBinaryDocValues dv = MultiValuedSortedBinaryDocValues.from(reader, offsetsFieldName);
                offsetsDocValues = new MultiValuedBinaryFlattenedDocValues(dv);
                allLoaders.add(offsetsDocValues);
            } else {
                offsetsDocValues = NO_VALUES;
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

    protected FlattenedFieldSyntheticWriterHelper getWriter() throws IOException {
        FlattenedFieldSyntheticWriterHelper.SortedKeyedValues sortedKeyedValues = docValues.getValues();
        TreeSet<BytesRef> ignoredValuesSet = collectIgnoredValues();
        if (ignoredValuesSet != null) {
            sortedKeyedValues = new DocValuesWithIgnoredSortedKeyedValues(sortedKeyedValues, ignoredValuesSet);
        }
        final var offsetsValues = offsetsDocValues.getValues();
        FlattenedFieldSyntheticWriterHelper.SortedOffsetValues keyedOffsetFieldSupplier = () -> {
            var value = offsetsValues.next();
            return value != null ? FlattenedFieldArrayContext.parseOffsetField(value) : null;
        };

        return new FlattenedFieldSyntheticWriterHelper(sortedKeyedValues, keyedOffsetFieldSupplier);
    }

    private TreeSet<BytesRef> collectIgnoredValues() throws IOException {
        if (storeIgnoredFieldsInBinaryDocValues) {
            // Ignored values were stored in binary doc values
            if (ignoredDocValues.count() > 0) {
                var result = new TreeSet<BytesRef>();
                var values = ignoredDocValues.getValues();
                for (int i = 0; i < ignoredDocValues.count(); i++) {
                    result.add(BytesRef.deepCopyOf(values.next()));
                }
                return result;
            }
        } else {
            // Otherwise, ignored values were stored in StoredFields
            if (ignoredStoredValues.isEmpty() == false) {
                var result = new TreeSet<BytesRef>();
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
        if (hasFlattenedValues) {
            var writer = getWriter();
            writer.write(b);
        }
        for (SourceLoader.SyntheticFieldLoader loader : mappedSubFieldLoaders) {
            if (loader.hasValue()) {
                loader.write(b);
            }
        }
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
     * a uniform way to position on a document and read its keyed values. This allows the rest of the loaded to work with keyed doc values
     * without caring about the underlying storage format.
     */
    interface FlattenedDocValues extends DocValuesLoader {
        boolean advanceToDoc(int docId) throws IOException;

        int count();

        FlattenedFieldSyntheticWriterHelper.SortedKeyedValues getValues();
    }

    private static final FlattenedDocValues NO_VALUES = new FlattenedDocValues() {
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

    private static final class SortedSetFlattenedDocValues implements FlattenedDocValues {
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

    private static final class MultiValuedBinaryFlattenedDocValues implements FlattenedDocValues {
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
