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
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;

class FlattenedSortedSetDocValuesSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private final String fieldFullPath;
    private final String keyedFieldFullPath;
    private final String keyedIgnoredValuesFieldFullPath;
    private final String leafName;

    private DocValuesFieldValues docValues = NO_VALUES;
    private List<Object> ignoredValues = List.of();

    /**
     * Build a loader for flattened fields from doc values.
     *
     * @param fieldFullPath                        full path to the original field
     * @param keyedFieldFullPath                   full path to the keyed field to load doc values from
     * @param keyedIgnoredValuesFieldFullPath      full path to the keyed field that stores values that are not present in doc values
     *                                             due to ignore_above
     * @param leafName                             the name of the leaf field to use in the rendered {@code _source}
     */
    FlattenedSortedSetDocValuesSyntheticFieldLoader(
        String fieldFullPath,
        String keyedFieldFullPath,
        @Nullable String keyedIgnoredValuesFieldFullPath,
        String leafName
    ) {
        this.fieldFullPath = fieldFullPath;
        this.keyedFieldFullPath = keyedFieldFullPath;
        this.keyedIgnoredValuesFieldFullPath = keyedIgnoredValuesFieldFullPath;
        this.leafName = leafName;
    }

    @Override
    public String fieldName() {
        return fieldFullPath;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        if (keyedIgnoredValuesFieldFullPath == null) {
            return Stream.empty();
        }

        return Stream.of(Map.entry(keyedIgnoredValuesFieldFullPath, (values) -> {
            ignoredValues = new ArrayList<>();
            ignoredValues.addAll(values);
        }));
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader reader, int[] docIdsInLeaf) throws IOException {
        final SortedSetDocValues dv = DocValues.getSortedSet(reader, keyedFieldFullPath);
        if (dv.getValueCount() == 0) {
            docValues = NO_VALUES;
            return null;
        }
        final FlattenedFieldDocValuesLoader loader = new FlattenedFieldDocValuesLoader(dv);
        docValues = loader;
        return loader;
    }

    @Override
    public boolean hasValue() {
        return docValues.count() > 0 || ignoredValues.isEmpty() == false;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (docValues.count() == 0 && ignoredValues.isEmpty()) {
            return;
        }

        FlattenedFieldSyntheticWriterHelper.SortedKeyedValues sortedKeyedValues = new DocValuesSortedKeyedValues(docValues);
        if (ignoredValues.isEmpty() == false) {
            var ignoredValuesSet = new TreeSet<BytesRef>();
            for (Object value : ignoredValues) {
                ignoredValuesSet.add((BytesRef) value);
            }
            ignoredValues = List.of();
            sortedKeyedValues = new DocValuesWithIgnoredSortedKeyedValues(sortedKeyedValues, ignoredValuesSet);
        }
        var writer = new FlattenedFieldSyntheticWriterHelper(sortedKeyedValues);

        b.startObject(leafName);
        writer.write(b);
        b.endObject();
    }

    @Override
    public void reset() {
        ignoredValues = List.of();
    }

    private interface DocValuesFieldValues {
        int count();

        SortedSetDocValues getValues();
    }

    private static final DocValuesFieldValues NO_VALUES = new DocValuesFieldValues() {
        @Override
        public int count() {
            return 0;
        }

        @Override
        public SortedSetDocValues getValues() {
            return null;
        }
    };

    /**
     * Load ordinals in line with populating the doc and immediately
     * convert from ordinals into {@link BytesRef}s.
     */
    private static class FlattenedFieldDocValuesLoader implements DocValuesLoader, DocValuesFieldValues {
        private final SortedSetDocValues dv;
        private boolean hasValue;

        FlattenedFieldDocValuesLoader(final SortedSetDocValues dv) {
            this.dv = dv;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            return hasValue = dv.advanceExact(docId);
        }

        @Override
        public int count() {
            return hasValue ? dv.docValueCount() : 0;
        }

        @Override
        public SortedSetDocValues getValues() {
            return dv;
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

    private static class DocValuesSortedKeyedValues implements FlattenedFieldSyntheticWriterHelper.SortedKeyedValues {
        private final DocValuesFieldValues docValues;
        private int seen = 0;

        private DocValuesSortedKeyedValues(DocValuesFieldValues docValues) {
            this.docValues = docValues;
        }

        @Override
        public BytesRef next() throws IOException {
            if (seen < docValues.count()) {
                seen += 1;
                var sortedSetDocValues = docValues.getValues();
                return sortedSetDocValues.lookupOrd(sortedSetDocValues.nextOrd());
            }

            return null;
        }
    }
}
