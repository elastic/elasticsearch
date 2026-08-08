/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.flattened.ColumnarKeyedBinaryDocValues;
import org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class KeyedFlattenedExistsQueryTests extends ESTestCase {

    private static final String FIELD = "field._keyed";

    private static RandomIndexWriter newColumnarWriter(Directory dir) throws IOException {
        FlattenedDocValuesFormat fmt = new FlattenedDocValuesFormat();
        IndexWriterConfig iwc = newIndexWriterConfig().setCodec(new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return FIELD.equals(field) ? fmt : super.getDocValuesFormatForField(field);
            }
        });
        return new RandomIndexWriter(random(), dir, iwc);
    }

    private static RandomIndexWriter newRowWriter(Directory dir) throws IOException {
        // NoMergePolicy prevents the row writer from auto-merging the pre-existing columnar segment
        // into the new row-format segment, which would collapse two leaves into one and break the
        // mixed-segment assertion.
        IndexWriterConfig iwc = newIndexWriterConfig().setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()))
            .setMergePolicy(NoMergePolicy.INSTANCE);
        return new RandomIndexWriter(random(), dir, iwc);
    }

    private static void addColumnarDoc(RandomIndexWriter writer, String key, String... values) throws IOException {
        LuceneDocument doc = new LuceneDocument();
        for (String v : values) {
            KeyedArrayOrderInlineNull.recordValue(doc, FIELD, new BytesRef(key + "\0" + v));
        }
        writer.addDocument(doc);
    }

    private static void addColumnarDocNullOnly(RandomIndexWriter writer, String key) throws IOException {
        LuceneDocument doc = new LuceneDocument();
        KeyedArrayOrderInlineNull.recordNull(doc, FIELD, new BytesRef(key + "\0"));
        writer.addDocument(doc);
    }

    private static void addEmptyDoc(RandomIndexWriter writer) throws IOException {
        writer.addDocument(new LuceneDocument());
    }

    private static KeyedFlattenedExistsQuery existsQuery(String key) {
        return new KeyedFlattenedExistsQuery(FIELD, key);
    }

    /** A non-null slot makes exists() return true. */
    public void testBasicColumnarExists() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDoc(writer, "k", "v");
            addEmptyDoc(writer);
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                assertTrue(leaf.reader().getBinaryDocValues(FIELD) instanceof ColumnarKeyedBinaryDocValues);
                assertEquals(1, newSearcher(reader).count(existsQuery("k")));
                assertEquals(0, newSearcher(reader).count(existsQuery("missing")));
            }
        }
    }

    /**
     * A document whose only slot for the key is a null slot must NOT match exists.
     * Consistent with the row-format behaviour in {@link AbstractBinaryDocValuesQuery#keyedInlineNullIterator}.
     */
    public void testNullOnlySlotDoesNotMatch() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDocNullOnly(writer, "k");           // null only
            addColumnarDoc(writer, "k", "v");              // non-null
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                assertEquals(1, newSearcher(reader).count(existsQuery("k")));
            }
        }
    }

    /** A prefix collision: "a" must not match exists for key "ab". */
    public void testNoPrefixCollision() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDoc(writer, "ab", "x");
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                assertEquals(0, newSearcher(reader).count(existsQuery("a")));
                assertEquals(1, newSearcher(reader).count(existsQuery("ab")));
            }
        }
    }

    /** Key absent from segment: whole leaf skipped. */
    public void testKeyAbsentFromSegment() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarWriter(dir)) {
            addColumnarDoc(writer, "other", "x");
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer.w)) {
                assertEquals(0, newSearcher(reader).count(existsQuery("missing")));
            }
        }
    }

    /** Mixed columnar and row segments must both contribute hits. */
    public void testMixedColumnarAndRowSegments() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newColumnarWriter(dir)) {
                addColumnarDoc(writer, "k", "v1");
                writer.commit();
            }
            try (RandomIndexWriter writer = newRowWriter(dir)) {
                LuceneDocument doc = new LuceneDocument();
                KeyedArrayOrderInlineNull.recordValue(doc, FIELD, new BytesRef("k\0v2"));
                writer.addDocument(doc);
                writer.commit();
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertEquals(2, reader.leaves().size());
                assertEquals(2, newSearcher(reader).count(existsQuery("k")));
                assertEquals(0, newSearcher(reader).count(existsQuery("absent")));
            }
        }
    }

    /** equals and hashCode: different keys produce different queries. */
    public void testEqualsAndHashCode() {
        KeyedFlattenedExistsQuery q1 = existsQuery("k");
        KeyedFlattenedExistsQuery q2 = existsQuery("k");
        KeyedFlattenedExistsQuery qDiffKey = existsQuery("other");
        KeyedFlattenedExistsQuery qDiffField = new KeyedFlattenedExistsQuery("other._keyed", "k");

        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertNotEquals(q1, qDiffKey);
        assertNotEquals(q1, qDiffField);
    }
}
