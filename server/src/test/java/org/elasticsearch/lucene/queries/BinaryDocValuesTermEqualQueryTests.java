/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.lucene.queries;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Tests for {@link BinaryDocValuesTermEqualQuery}: the optimized single-valued exact-match path.
 *
 * <p>This query is only ever constructed by {@link SlowCustomBinaryDocValuesTermQuery#rewrite}
 * after it has verified that every leaf is single-valued and the underlying codec supports
 * {@link org.elasticsearch.index.mapper.BlockLoader.OptionalColumnAtATimeReader#tryTermEqualIterator}.
 * Tests here therefore use single-valued documents with the ES819 compressed format.
 *
 * <p>Multi-valued semantics and the rewrite decision are tested in
 * {@link SlowCustomBinaryDocValuesTermQueryTests}.
 */
public class BinaryDocValuesTermEqualQueryTests extends ESTestCase {

    public void testSingleValued() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                // doc 0: "search" -> equals "search"
                addSingleValueDoc(writer, fieldName, "search");
                // doc 1: "elasticsearch" -> does not equal "search" (is a superset)
                addSingleValueDoc(writer, fieldName, "elasticsearch");
                // doc 2: "search" -> equals "search"
                addSingleValueDoc(writer, fieldName, "search");
                // doc 3: "sear" -> prefix of "search", does not equal
                addSingleValueDoc(writer, fieldName, "sear");
                // doc 4: "searcher" -> does not equal "search"
                addSingleValueDoc(writer, fieldName, "searcher");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(2, searcher.count(new BinaryDocValuesTermEqualQuery(fieldName, new BytesRef("search"))));
                }
            }
        }
    }

    public void testNoField() throws IOException {
        String fieldName = "field";

        // no field in index at all
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(0, searcher.count(new BinaryDocValuesTermEqualQuery(fieldName, new BytesRef("test"))));
                }
            }
        }

        // field absent from one segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "test");
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(1, searcher.count(new BinaryDocValuesTermEqualQuery(fieldName, new BytesRef("test"))));
                }
            }
        }
    }

    public void testNoMatch() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "world");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(0, searcher.count(new BinaryDocValuesTermEqualQuery(fieldName, new BytesRef("xyz"))));
                }
            }
        }
    }

    /**
     * When the index is all-single-valued with the ES819 compressed format,
     * {@link SlowCustomBinaryDocValuesTermQuery#rewrite} produces a
     * {@link BinaryDocValuesTermEqualQuery}. Verify that both queries return identical counts.
     */
    public void testParityWithSlowQuery() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "world");
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "foo");

                BytesRef term = new BytesRef("hello");
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    int slow = searcher.count(new SlowCustomBinaryDocValuesTermQuery(fieldName, term));
                    int fast = searcher.count(new BinaryDocValuesTermEqualQuery(fieldName, term));
                    assertEquals("optimized query must produce the same count as the slow query", slow, fast);
                }
            }
        }
    }

    /**
     * With single-valued docs and the ES819 format, {@link SlowCustomBinaryDocValuesTermQuery#rewrite}
     * must return a {@link BinaryDocValuesTermEqualQuery} so the TwoPhaseIterator optimized path
     * is taken.
     */
    public void testRewritesToOptimizedQuery() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "world");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query rewritten = new SlowCustomBinaryDocValuesTermQuery(fieldName, new BytesRef("hello")).rewrite(searcher);
                    assertThat(rewritten, org.hamcrest.Matchers.instanceOf(BinaryDocValuesTermEqualQuery.class));
                }
            }
        }
    }

    public void testEqualsAndHashCode() {
        BinaryDocValuesTermEqualQuery q1 = new BinaryDocValuesTermEqualQuery("field", new BytesRef("term"));
        BinaryDocValuesTermEqualQuery q2 = new BinaryDocValuesTermEqualQuery("field", new BytesRef("term"));
        BinaryDocValuesTermEqualQuery q3 = new BinaryDocValuesTermEqualQuery("other", new BytesRef("term"));
        BinaryDocValuesTermEqualQuery q4 = new BinaryDocValuesTermEqualQuery("field", new BytesRef("other"));

        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertNotEquals(q1, q3);
        assertNotEquals(q1, q4);
    }

    static void addSingleValueDoc(RandomIndexWriter writer, String fieldName, String value) throws IOException {
        Document document = new Document();
        var field = new MultiValuedBinaryDocValuesField.SeparateCount(
            fieldName,
            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
        );
        field.add(new BytesRef(value.getBytes(StandardCharsets.UTF_8)));
        var countField = NumericDocValuesField.indexedField(fieldName + ".counts", 1);
        document.add(field);
        document.add(countField);
        writer.addDocument(document);
    }

    static void addMultiValueDoc(RandomIndexWriter writer, String fieldName, String... values) throws IOException {
        Document document = new Document();
        var field = new MultiValuedBinaryDocValuesField.SeparateCount(
            fieldName,
            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
        );
        for (String value : values) {
            field.add(new BytesRef(value.getBytes(StandardCharsets.UTF_8)));
        }
        var countField = NumericDocValuesField.indexedField(fieldName + ".counts", field.count());
        document.add(field);
        document.add(countField);
        writer.addDocument(document);
    }

    static RandomIndexWriter newRandomIndexWriter(Directory dir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        return new RandomIndexWriter(random(), dir, iwc);
    }
}
