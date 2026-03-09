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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.lucene.queries.SlowCustomBinaryDocValuesWildcardQuery.getContainsPattern;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class SlowCustomBinaryDocValuesWildcardQueryTests extends ESTestCase {

    public void testBasics() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            Map<String, Long> expectedCounts = new HashMap<>();
            expectedCounts.put("a", 2L);
            expectedCounts.put("b", 5L);
            expectedCounts.put("c", 1L);
            expectedCounts.put("d", 3L);
            expectedCounts.put("e", 10L);
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                for (var entry : expectedCounts.entrySet()) {
                    for (int i = 0; i < entry.getValue(); i++) {
                        Document document = new Document();

                        var field = new MultiValuedBinaryDocValuesField.SeparateCount("field", false);
                        field.add(new BytesRef(entry.getKey().getBytes(StandardCharsets.UTF_8)));
                        var countField = NumericDocValuesField.indexedField("field.counts", 1);

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
                        long count = searcher.count(new SlowCustomBinaryDocValuesWildcardQuery(fieldName, entry.getKey() + "*", false));
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
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "a*", false);
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                Document document = new Document();

                var field = new MultiValuedBinaryDocValuesField.SeparateCount("field", false);
                field.add(new BytesRef("a".getBytes(StandardCharsets.UTF_8)));
                var countField = NumericDocValuesField.indexedField("field.counts", 1);
                document.add(field);
                document.add(countField);

                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "a*", false);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testAgainstWildcardQuery() throws IOException {
        List<String> randomValues = randomList(8, 32, () -> randomAlphaOfLength(8));
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                for (String randomValue : randomValues) {
                    Document document = new Document();
                    document.add(new SortedSetDocValuesField("baseline_field", new BytesRef(randomValue)));

                    var binaryDVField = new MultiValuedBinaryDocValuesField.SeparateCount("contender_field", false);
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
                    String randomWildcard = randomFrom(randomValues).substring(0, 2) + "*";
                    IndexSearcher searcher = newSearcher(reader);

                    Query baselineQuery = new WildcardQuery(
                        new Term("baseline_field", randomWildcard),
                        Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                        MultiTermQuery.DOC_VALUES_REWRITE
                    );
                    TopDocs baselineResults = searcher.search(baselineQuery, 32);

                    Query contenderQuery = new SlowCustomBinaryDocValuesWildcardQuery("contender_field", randomWildcard, false);
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

    public void testGetContainsPattern() {
        assertThat(getContainsPattern("*foo*"), equalTo("foo"));
        assertThat(getContainsPattern("*hello world*"), equalTo("hello world"));
        assertThat(getContainsPattern("*a*"), equalTo("a"));

        assertThat(getContainsPattern("**"), nullValue());
        assertThat(getContainsPattern("*"), nullValue());
        assertThat(getContainsPattern(""), nullValue());
        assertThat(getContainsPattern("foo*"), nullValue());
        assertThat(getContainsPattern("*foo"), nullValue());
        assertThat(getContainsPattern("foo"), nullValue());
        assertThat(getContainsPattern("*foo*bar*"), nullValue());
        assertThat(getContainsPattern("*fo?*"), nullValue());
        assertThat(getContainsPattern("*fo\\**"), nullValue());
    }

    public void testRewriteToContainsQuery() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "elasticsearch");
                addDoc(writer, fieldName, "kibana");
                addDoc(writer, fieldName, "research");
                addDoc(writer, fieldName, "logstash");
                addDoc(writer, fieldName, "searching");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "*search*", false);
                    Query rewritten = query.rewrite(searcher);
                    assertThat(rewritten, instanceOf(BinaryDocValuesContainsTermQuery.class));
                    assertEquals(3, searcher.count(rewritten));
                }
            }
        }
    }

    public void testRewriteToContainsQueryMultiValued() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "hello", "world");
                addDoc(writer, fieldName, "foo", "bar");
                addDoc(writer, fieldName, "wellcome", "to");
                addDoc(writer, fieldName, "abc");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "*ell*", false);
                    Query rewritten = query.rewrite(searcher);
                    assertThat(rewritten, instanceOf(BinaryDocValuesContainsTermQuery.class));
                    assertEquals(2, searcher.count(rewritten));
                }
            }
        }
    }

    public void testCaseInsensitiveNotRewrittenToContains() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "Elasticsearch");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "*search*", true);
                    Query rewritten = query.rewrite(searcher);
                    assertThat(rewritten, instanceOf(SlowCustomBinaryDocValuesWildcardQuery.class));
                    assertEquals(1, searcher.count(rewritten));
                }
            }
        }
    }

    public void testNonContainsPatternNotRewritten() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "foobar");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    Query prefixQuery = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "foo*", false);
                    assertThat(prefixQuery.rewrite(searcher), instanceOf(SlowCustomBinaryDocValuesWildcardQuery.class));

                    Query multiWildcard = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "*foo*bar*", false);
                    assertThat(multiWildcard.rewrite(searcher), instanceOf(SlowCustomBinaryDocValuesWildcardQuery.class));

                    Query singleCharWildcard = new SlowCustomBinaryDocValuesWildcardQuery(fieldName, "*fo?*", false);
                    assertThat(singleCharWildcard.rewrite(searcher), instanceOf(SlowCustomBinaryDocValuesWildcardQuery.class));
                }
            }
        }
    }

    private static void addDoc(RandomIndexWriter writer, String fieldName, String... values) throws IOException {
        Document document = new Document();
        var field = new MultiValuedBinaryDocValuesField.SeparateCount(fieldName, false);
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
