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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ScanningBinaryDocValuesPrefixQueryTests extends ESTestCase {

    public void testArrayOrderInlineNull() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = ArrayOrderInlineNullTestUtils.newWriter(dir)) {
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "alpha", null, "beta"); // multi-value with an inline null slot
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, (String) null);          // all-null, immediately before a match
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "best");                  // single value stored raw
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName);                          // empty array
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "gamma", "delta");        // multi-value
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // prefix "be" matches "beta" (multi-value doc) and "best" (single-value doc); the all-null doc preceding "best" must
                    // not be matched.
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "be", false, true)));
                    // prefix "a" matches only "alpha".
                    assertEquals(1, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "a", false, true)));
                    assertEquals(0, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "zz", false, true)));
                }
            }
        }
    }

    public void testBasics() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            Map<String, Long> expectedCounts = new HashMap<>();
            expectedCounts.put("apple", 2L);
            expectedCounts.put("apricot", 5L);
            expectedCounts.put("banana", 1L);
            expectedCounts.put("blueberry", 3L);
            expectedCounts.put("avocado", 10L);
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                for (var entry : expectedCounts.entrySet()) {
                    for (int i = 0; i < entry.getValue(); i++) {
                        Document document = new Document();

                        var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                            fieldName,
                            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                        );
                        field.add(new BytesRef(entry.getKey().getBytes(StandardCharsets.UTF_8)));
                        var countField = NumericDocValuesField.indexedField(fieldName + ".counts", 1);

                        if (randomBoolean()) {
                            field.add(new BytesRef("z".getBytes(StandardCharsets.UTF_8)));
                            countField.setLongValue(field.count());
                        }
                        document.add(field);
                        document.add(countField);
                        writer.addDocument(document);
                    }
                }

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // prefix "a" matches apple (2), apricot (5), avocado (10)
                    assertEquals(17, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "a", false, false)));
                    // prefix "ap" matches apple (2), apricot (5)
                    assertEquals(7, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "ap", false, false)));
                    // prefix "b" matches banana (1), blueberry (3)
                    assertEquals(4, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "b", false, false)));
                    // exact prefix matches all docs with that value
                    for (var entry : expectedCounts.entrySet()) {
                        assertEquals(
                            entry.getValue().longValue(),
                            searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, entry.getKey(), false, false))
                        );
                    }
                }
            }
        }
    }

    public void testNoField() throws IOException {
        String fieldName = "field";

        // no field in index
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(0, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "foo", false, false)));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                Document document = new Document();
                var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                    fieldName,
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
                field.add(new BytesRef("foobar".getBytes(StandardCharsets.UTF_8)));
                var countField = NumericDocValuesField.indexedField(fieldName + ".counts", 1);
                document.add(field);
                document.add(countField);

                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertEquals(1, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "foo", false, false)));
                }
            }
        }
    }

    public void testAgainstPrefixQuery() throws IOException {
        List<String> randomValues = randomList(8, 32, () -> randomAlphaOfLength(8));
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                for (String randomValue : randomValues) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("baseline_field", new BytesRef(randomValue)));

                    var binaryDVField = new MultiValuedBinaryDocValuesField.SeparateCount(
                        "contender_field",
                        MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                    );
                    binaryDVField.add(new BytesRef(randomValue.getBytes(StandardCharsets.UTF_8)));
                    var countField = NumericDocValuesField.indexedField("contender_field.counts", 1);
                    document.add(binaryDVField);
                    document.add(countField);

                    if (randomBoolean()) {
                        String extraRandomValue = randomFrom(randomValues);
                        binaryDVField.add(new BytesRef(extraRandomValue.getBytes(StandardCharsets.UTF_8)));
                        countField.setLongValue(binaryDVField.count());
                        document.add(new SortedSetDocValuesField("baseline_field", new BytesRef(extraRandomValue)));
                    }
                    writer.addDocument(document);
                }

                try (IndexReader reader = writer.getReader()) {
                    String prefix = randomFrom(randomValues).substring(0, 2);
                    IndexSearcher searcher = newSearcher(reader);

                    Query baselineQuery = new PrefixQuery(new Term("baseline_field", prefix), MultiTermQuery.DOC_VALUES_REWRITE);
                    TopDocs baselineResults = searcher.search(baselineQuery, 32);

                    Query contenderQuery = new ScanningBinaryDocValuesPrefixQuery("contender_field", prefix, false, false);
                    TopDocs contenderResults = searcher.search(contenderQuery, 32);

                    assertThat(contenderResults.totalHits, equalTo(baselineResults.totalHits));
                    assertThat(baselineResults.scoreDocs.length, greaterThanOrEqualTo(1));
                    assertThat(baselineResults.scoreDocs.length, equalTo(contenderResults.scoreDocs.length));
                    for (int i = 0; i < baselineResults.scoreDocs.length; i++) {
                        assertThat(baselineResults.scoreDocs[i].doc, equalTo(contenderResults.scoreDocs[i].doc));
                        assertThat(baselineResults.scoreDocs[i].score, equalTo(contenderResults.scoreDocs[i].score));
                    }
                }
            }
        }
    }

    public void testCaseInsensitive() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "Elasticsearch");
                addDoc(writer, fieldName, "ELASTIC");
                addDoc(writer, fieldName, "kibana");
                addDoc(writer, fieldName, "Logstash");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // "elastic" case-insensitive prefix matches "Elasticsearch" and "ELASTIC"
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "elastic", true, false)));
                    // case-sensitive matches only the exact-cased docs
                    assertEquals(0, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "elastic", false, false)));
                    assertEquals(1, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "Elastic", false, false)));
                    // "log" case-insensitive matches "Logstash"
                    assertEquals(1, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "log", true, false)));
                    assertEquals(1, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "LOG", true, false)));
                    assertEquals(0, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "log", false, false)));
                }
            }
        }
    }

    public void testEmptyPrefix() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "foo");
                addDoc(writer, fieldName, "bar");
                addDoc(writer, fieldName, "baz");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // empty prefix matches everything
                    assertEquals(3, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "", false, false)));
                    assertEquals(3, searcher.count(new ScanningBinaryDocValuesPrefixQuery(fieldName, "", true, false)));
                }
            }
        }
    }

    private static void addDoc(RandomIndexWriter writer, String fieldName, String... values) throws IOException {
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

    private static RandomIndexWriter newRandomIndexWriter(Directory dir) throws IOException {
        IndexWriterConfig iwc = newIndexWriterConfig();
        if (randomBoolean()) {
            iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819Version3TSDBDocValuesFormat()));
        }
        return new RandomIndexWriter(random(), dir, iwc);
    }
}
