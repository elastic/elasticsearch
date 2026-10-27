/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.slotsPayload;

final class ColumnarStringQueryTestUtils {

    private ColumnarStringQueryTestUtils() {}

    static List<Integer> found(final IndexSearcher searcher, final Query query) throws IOException {
        final TopDocs hits = searcher.search(query, Integer.MAX_VALUE);
        final List<Integer> docs = new ArrayList<>();
        for (final ScoreDoc hit : hits.scoreDocs) {
            docs.add(hit.doc);
        }
        docs.sort(Integer::compareTo);
        return docs;
    }

    /**
     * Writes {@code docs} to {@code dir} as one segment of {@code field}. A {@code null} document has no value for
     * the field, an empty array is a document with no slots, a {@code null} element is a null slot, and any other
     * element is a value.
     */
    static void indexOneSegment(final Directory dir, final Codec codec, final String field, final BytesRef[]... docs) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec).setMergePolicy(new LogDocMergePolicy());
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (final BytesRef[] slots : docs) {
                final Document doc = new Document();
                if (slots != null) {
                    doc.add(new Field(field, slotsPayload(slots), columnarBinaryFieldType()));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    /** One document per value, each holding that value as its only slot. A {@code null} value is a document without the field. */
    static BytesRef[][] oneSlotEach(final List<String> values) {
        final BytesRef[][] docs = new BytesRef[values.size()][];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = values.get(d) == null ? null : new BytesRef[] { new BytesRef(values.get(d)) };
        }
        return docs;
    }
}
