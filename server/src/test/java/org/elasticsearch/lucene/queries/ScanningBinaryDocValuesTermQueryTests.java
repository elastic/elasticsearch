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
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ScanningBinaryDocValuesTermQueryTests extends ESTestCase {

    public void testArrayOrderInlineNull() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = ArrayOrderInlineNullTestUtils.newWriter(dir)) {
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "alpha", null, "beta"); // multi-value with an inline null slot
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, (String) null);          // all-null, immediately before a match
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "beta");                  // single value stored raw
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName);                          // empty array
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "gamma", "delta");        // multi-value, no beta
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // "beta" is carried by the multi-value doc and the single-value doc; the all-null doc that immediately precedes the
                    // single-value "beta" doc must not be matched (guards advanceExact vs advance on the binary cursor).
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("beta"), true)));
                    assertEquals(1, searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("alpha"), true)));
                    assertEquals(0, searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("zeta"), true)));
                }
            }
        }
    }

    public void testBasics() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            Map<String, Long> expectedCounts = new HashMap<>();
            expectedCounts.put("a", 2L);
            expectedCounts.put("b", 5L);
            expectedCounts.put("c", 1L);
            expectedCounts.put("d", 3L);
            expectedCounts.put("e", 10L);
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (var entry : expectedCounts.entrySet()) {
                    for (int i = 0; i < entry.getValue(); i++) {
                        Document document = new Document();

                        var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                            "field",
                            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                        );
                        field.add(new BytesRef(entry.getKey().getBytes(StandardCharsets.UTF_8)));
                        var countField = new NumericDocValuesField("field.counts", 1);

                        if (randomBoolean()) {
                            field.add(new BytesRef("z".getBytes(StandardCharsets.UTF_8)));
                            countField.setLongValue(field.count());
                        }
                        document.add(field);
                        document.add(countField);
                        writer.addDocument(document);
                    }
                }

                // search
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    for (var entry : expectedCounts.entrySet()) {
                        long count = searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef(entry.getKey()), false));
                        assertEquals(entry.getValue().longValue(), count);
                    }
                }
            }
        }
    }

    public void testNoField() throws IOException {
        String fieldName = "field";

        // no field in index
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("a"), false);
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document document = new Document();

                var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                    "field",
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
                field.add(new BytesRef("a".getBytes(StandardCharsets.UTF_8)));
                var countField = new NumericDocValuesField("field.counts", 1);
                document.add(field);
                document.add(countField);

                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("a"), false);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    /**
     * Multi-valued docs use ANY-value (OR) semantics: a doc matches if any one of its values
     * exactly equals the term.
     */
    public void testMultiValued() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                // doc 0: ["hello", "world"] -> "hello" equals term
                addMultiValueDoc(writer, fieldName, "hello", "world");
                // doc 1: ["foo", "bar"] -> neither equals term
                addMultiValueDoc(writer, fieldName, "foo", "bar");
                // doc 2: ["world", "hello"] -> "hello" equals term
                addMultiValueDoc(writer, fieldName, "world", "hello");
                // doc 3: single-valued "hello" -> equals term
                addSingleValueDoc(writer, fieldName, "hello");

                BytesRef term = new BytesRef("hello");
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(3, searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, term, false)));
                }
            }
        }
    }

    /**
     * With the ES819 compressed format and single-valued docs, the optimized
     * column-at-a-time iterator path is taken per leaf. Verify count matches
     * the generic slow path.
     */
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
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("search"), false)));
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
                    assertEquals(0, searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("xyz"), false)));
                }
            }
        }
    }

    /**
     * With single-valued docs and the ES819 format, the optimized path produces the same count as
     * a generic (non-ES819) index that uses the scanning fallback.
     */
    public void testParityBetweenOptimizedAndSlowPath() throws IOException {
        String fieldName = "field";
        BytesRef term = new BytesRef("hello");

        // slow path: plain RandomIndexWriter (no ES819 codec)
        int slowCount;
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "world");
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "foo");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    slowCount = searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, term, false));
                }
            }
        }

        // fast path: ES819 codec, same documents
        int fastCount;
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "world");
                addSingleValueDoc(writer, fieldName, "hello");
                addSingleValueDoc(writer, fieldName, "foo");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    fastCount = searcher.count(new ScanningBinaryDocValuesTermQuery(fieldName, term, false));
                }
            }
        }

        assertEquals("optimized path must produce the same count as the slow path", slowCount, fastCount);
    }

    /**
     * rewrite() returns a {@link BinaryDocValuesLengthQuery} for an empty term.
     */
    public void testRewritesEmptyTermToLengthQuery() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "hello");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query rewritten = new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef(""), false).rewrite(searcher);
                    assertThat(rewritten, Matchers.instanceOf(BinaryDocValuesLengthQuery.class));
                }
            }
        }
    }

    /**
     * rewrite() returns {@code this} for a non-empty term regardless of the codec — the per-leaf
     * optimized-vs-scanning decision is now made inside {@code getDocIdSetIterator}.
     */
    public void testNonEmptyTermDoesNotRewrite() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addSingleValueDoc(writer, fieldName, "hello");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    ScanningBinaryDocValuesTermQuery q = new ScanningBinaryDocValuesTermQuery(fieldName, new BytesRef("hello"), false);
                    assertSame("rewrite must return this for a non-empty term", q, q.rewrite(searcher));
                }
            }
        }
    }

    public void testEqualsAndHashCode() {
        ScanningBinaryDocValuesTermQuery q1 = new ScanningBinaryDocValuesTermQuery("field", new BytesRef("term"), false);
        ScanningBinaryDocValuesTermQuery q2 = new ScanningBinaryDocValuesTermQuery("field", new BytesRef("term"), false);
        ScanningBinaryDocValuesTermQuery q3 = new ScanningBinaryDocValuesTermQuery("other", new BytesRef("term"), false);
        ScanningBinaryDocValuesTermQuery q4 = new ScanningBinaryDocValuesTermQuery("field", new BytesRef("other"), false);

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
