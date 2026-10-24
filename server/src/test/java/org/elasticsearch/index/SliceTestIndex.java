/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.tests.util.LuceneTestCase.random;

/**
 * Builds an index laid out the way a slice-enabled index is: the slice held as sorted doc values with a skip index,
 * and the index sorted on it. The configuration is not randomized because the tests assert on how the skip index
 * groups documents.
 */
final class SliceTestIndex {

    static final String FIELD = "slice";
    static final String ID_FIELD = "id";

    private SliceTestIndex() {}

    static IndexWriterConfig config() {
        SortField sortField = new SortField(FIELD, SortField.Type.STRING);
        sortField.setMissingValue(SortField.STRING_LAST);
        return new IndexWriterConfig().setIndexSort(new Sort(sortField));
    }

    /** As {@link #config()}, for tests that need deletes to stay visible rather than be merged away. */
    static IndexWriterConfig configWithoutMerges() {
        return config().setMergePolicy(NoMergePolicy.INSTANCE);
    }

    /** The id of the {@code index}th document of {@code slice}, to delete it by. */
    static String id(String slice, int index) {
        return slice + "#" + index;
    }

    /**
     * Writes {@code docsPerSlice} documents of each slice, interleaved, so that only the index sort groups a slice's
     * documents together.
     */
    static void write(Directory dir, Map<String, Integer> docsPerSlice, boolean forceMerge) throws IOException {
        write(dir, docsPerSlice, forceMerge, config());
    }

    static void write(Directory dir, Map<String, Integer> docsPerSlice, boolean forceMerge, IndexWriterConfig config) throws IOException {
        List<String> slicePerDoc = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : docsPerSlice.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                slicePerDoc.add(entry.getKey());
            }
        }
        Collections.shuffle(slicePerDoc, random());
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            Map<String, Integer> seen = new HashMap<>();
            for (String slice : slicePerDoc) {
                int index = seen.merge(slice, 1, Integer::sum) - 1;
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(FIELD, new BytesRef(slice)));
                doc.add(new StringField(ID_FIELD, id(slice, index), Field.Store.NO));
                writer.addDocument(doc);
            }
            if (forceMerge) {
                writer.forceMerge(1);
            }
        }
    }
}
