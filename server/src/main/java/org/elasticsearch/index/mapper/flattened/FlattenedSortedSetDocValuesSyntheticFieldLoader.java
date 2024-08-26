/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

public class FlattenedSortedSetDocValuesSyntheticFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private DocValuesFieldValues docValues = NO_VALUES;
    private final String fieldFullPath;
    private final String keyedFieldFullPath;
    private final String leafName;

    /**
     * Build a loader for flattened fields from doc values.
     *
     * @param fieldFullPath           full path to the original field
     * @param keyedFieldFullPath      full path to the keyed field to load doc values from
     * @param leafName                the name of the leaf field to use in the rendered {@code _source}
     */
    public FlattenedSortedSetDocValuesSyntheticFieldLoader(String fieldFullPath, String keyedFieldFullPath, String leafName) {
        this.fieldFullPath = fieldFullPath;
        this.keyedFieldFullPath = keyedFieldFullPath;
        this.leafName = leafName;
    }

    @Override
    public String fieldName() {
        return fieldFullPath;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        return Stream.empty();
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
        return docValues.count() > 0;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (docValues.count() == 0) {
            return;
        }
        b.startObject(leafName);
        docValues.write(b);
        b.endObject();
    }

    private interface DocValuesFieldValues {
        int count();

        void write(XContentBuilder b) throws IOException;
    }

    private static final DocValuesFieldValues NO_VALUES = new DocValuesFieldValues() {
        @Override
        public int count() {
            return 0;
        }

        @Override
        public void write(XContentBuilder b) {}
    };

    /**
     * Load ordinals in line with populating the doc and immediately
     * convert from ordinals into {@link BytesRef}s.
     */
    private static class FlattenedFieldDocValuesLoader implements DocValuesLoader, DocValuesFieldValues {
        private final SortedSetDocValues dv;
        private boolean hasValue;
        private final FlattenedFieldSyntheticWriterHelper writer;

        FlattenedFieldDocValuesLoader(final SortedSetDocValues dv) {
            this.dv = dv;
            this.writer = new FlattenedFieldSyntheticWriterHelper(dv);
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
        public void write(XContentBuilder b) throws IOException {
            this.writer.write(b);
        }
    }
}
