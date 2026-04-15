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

    protected DocValuesFieldValues docValues = NO_VALUES;
    protected List<Object> ignoredValues = List.of();

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
     */
    FlattenedDocValuesSyntheticFieldLoader(
        String fieldFullPath,
        String keyedFieldFullPath,
        @Nullable String keyedIgnoredValuesFieldFullPath,
        String leafName,
        boolean usesBinaryDocValues,
        List<SourceLoader.SyntheticFieldLoader> mappedSubFieldLoaders
    ) {
        this.fieldFullPath = fieldFullPath;
        this.keyedFieldFullPath = keyedFieldFullPath;
        this.keyedIgnoredValuesFieldFullPath = keyedIgnoredValuesFieldFullPath;
        this.leafName = leafName;
        this.usesBinaryDocValues = usesBinaryDocValues;
        this.mappedSubFieldLoaders = mappedSubFieldLoaders;
    }

    @Override
    public String fieldName() {
        return fieldFullPath;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        Stream<Map.Entry<String, StoredFieldLoader>> flattenedLoaders = keyedIgnoredValuesFieldFullPath == null
            ? Stream.empty()
            : Stream.of(Map.entry(keyedIgnoredValuesFieldFullPath, (values) -> {
                ignoredValues = new ArrayList<>();
                ignoredValues.addAll(values);
            }));
        Stream<Map.Entry<String, StoredFieldLoader>> subFieldLoaders = mappedSubFieldLoaders.stream()
            .flatMap(SourceLoader.SyntheticFieldLoader::storedFieldLoaders);
        return Stream.concat(flattenedLoaders, subFieldLoaders);
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        List<DocValuesLoader> allLoaders = new ArrayList<>();

        if (usesBinaryDocValues) {
            var binaryDv = reader.getBinaryDocValues(keyedFieldFullPath);
            if (binaryDv != null) {
                SortedBinaryDocValues dv = MultiValuedSortedBinaryDocValues.from(reader, keyedFieldFullPath, binaryDv);
                MultiValuedBinaryFieldValues loader = new MultiValuedBinaryFieldValues(dv);
                docValues = loader;
                allLoaders.add(loader);
            } else {
                docValues = NO_VALUES;
            }
        } else {
            final SortedSetDocValues dv = DocValues.getSortedSet(reader, keyedFieldFullPath);
            if (dv.getValueCount() > 0) {
                SortedSetFieldValues loader = new SortedSetFieldValues(dv);
                docValues = loader;
                allLoaders.add(loader);
            } else {
                docValues = NO_VALUES;
            }
        }

        for (SourceLoader.SyntheticFieldLoader subFieldLoader : mappedSubFieldLoaders) {
            DocValuesLoader subFieldDvLoader = subFieldLoader.docValuesLoader(reader, docIdsInLeaf);
            if (subFieldDvLoader != null) {
                allLoaders.add(subFieldDvLoader);
            }
        }

        if (allLoaders.isEmpty()) {
            return null;
        }
        if (allLoaders.size() == 1) {
            return allLoaders.getFirst();
        }
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
        return docValues.count() > 0 || ignoredValues.isEmpty() == false;
    }

    private boolean hasPropertyValues() {
        for (SourceLoader.SyntheticFieldLoader loader : mappedSubFieldLoaders) {
            if (loader.hasValue()) {
                return true;
            }
        }
        return false;
    }

    protected FlattenedFieldSyntheticWriterHelper getWriter() {
        FlattenedFieldSyntheticWriterHelper.SortedKeyedValues sortedKeyedValues = docValues.getValues();
        if (ignoredValues.isEmpty() == false) {
            var ignoredValuesSet = new TreeSet<BytesRef>();
            for (Object value : ignoredValues) {
                ignoredValuesSet.add((BytesRef) value);
            }
            ignoredValues = List.of();
            sortedKeyedValues = new DocValuesWithIgnoredSortedKeyedValues(sortedKeyedValues, ignoredValuesSet);
        }
        return new FlattenedFieldSyntheticWriterHelper(sortedKeyedValues);
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
        ignoredValues = List.of();
        for (SourceLoader.SyntheticFieldLoader loader : mappedSubFieldLoaders) {
            loader.reset();
        }
    }

    protected List<SourceLoader.SyntheticFieldLoader> getMappedSubFieldLoaders() {
        return mappedSubFieldLoaders;
    }

    protected interface DocValuesFieldValues {
        int count();

        FlattenedFieldSyntheticWriterHelper.SortedKeyedValues getValues();
    }

    private static final DocValuesFieldValues NO_VALUES = new DocValuesFieldValues() {
        @Override
        public int count() {
            return 0;
        }

        @Override
        public FlattenedFieldSyntheticWriterHelper.SortedKeyedValues getValues() {
            return () -> null;
        }
    };

    private static final class SortedSetFieldValues implements DocValuesFieldValues, DocValuesLoader {
        private final SortedSetDocValues docValues;
        private boolean hasValue;

        SortedSetFieldValues(SortedSetDocValues docValues) {
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

    private static final class MultiValuedBinaryFieldValues implements DocValuesFieldValues, DocValuesLoader {
        private final SortedBinaryDocValues docValues;
        private boolean hasValue = false;

        MultiValuedBinaryFieldValues(SortedBinaryDocValues docValues) {
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
        private final TreeSet<BytesRef> ignoredValues;

        private BytesRef currentFromDocValues;

        private DocValuesWithIgnoredSortedKeyedValues(
            FlattenedFieldSyntheticWriterHelper.SortedKeyedValues docValues,
            TreeSet<BytesRef> ignoredValues
        ) {
            this.docValues = docValues;
            this.ignoredValues = ignoredValues;
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

            if (ignoredValues.isEmpty() == false) {
                BytesRef ignoredCandidate = ignoredValues.first();
                if (currentFromDocValues == null || ignoredCandidate.compareTo(currentFromDocValues) <= 0) {
                    ignoredValues.pollFirst();
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
