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
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ScanningBinaryDocValuesRegexpQueryTests extends ESTestCase {

    public void testArrayOrderInlineNull() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = ArrayOrderInlineNullTestUtils.newWriter(dir)) {
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "alpha", null, "beta"); // multi-value with an inline null slot
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, (String) null);          // all-null, immediately before a match
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "beta");                  // single value stored raw
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName);                          // empty array
                ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "gamma", "delta");        // multi-value
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // "be.*" matches "beta" in the multi-value doc and the single-value doc; the all-null doc preceding the latter must
                    // not be matched.
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, "be.*", 0, 0, 1000, true)));
                    // "(alpha|delta)" matches the first and last docs.
                    assertEquals(2, searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, "(alpha|delta)", 0, 0, 1000, true)));
                    assertEquals(0, searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, "zeta", 0, 0, 1000, true)));
                }
            }
        }
    }

    public void testBasics() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            Map<String, Long> expectedCounts = new HashMap<>();
            expectedCounts.put("apple", 2L);
            expectedCounts.put("banana", 5L);
            expectedCounts.put("apricot", 1L);
            expectedCounts.put("cherry", 3L);
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
                            field.add(new BytesRef("zzz".getBytes(StandardCharsets.UTF_8)));
                            countField.setLongValue(field.count());
                        }
                        document.add(field);
                        document.add(countField);
                        writer.addDocument(document);
                    }
                }

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    // "a.*" matches apple, apricot, avocado
                    long aCount = expectedCounts.get("apple") + expectedCounts.get("apricot") + expectedCounts.get("avocado");
                    assertEquals(aCount, searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, "a.*", 0, 0, 1000, false)));

                    // "b.*" matches banana
                    assertEquals(
                        expectedCounts.get("banana").longValue(),
                        searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, "b.*", 0, 0, 1000, false))
                    );

                    // "cherry" exact matches cherry
                    assertEquals(
                        expectedCounts.get("cherry").longValue(),
                        searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, "cherry", 0, 0, 1000, false))
                    );
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
                    Query query = new ScanningBinaryDocValuesRegexpQuery(fieldName, "a.*", 0, 0, 1000, false);
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // field present in one segment but not another
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "apple");
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = new ScanningBinaryDocValuesRegexpQuery(fieldName, "a.*", 0, 0, 1000, false);
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testAgainstRegexpQuery() throws IOException {
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
                    // Use the first two characters as a prefix pattern so we get some matches
                    String prefix = randomFrom(randomValues).substring(0, 2);
                    String regexpPattern = prefix + ".*";
                    IndexSearcher searcher = newSearcher(reader);

                    Query baselineQuery = new RegexpQuery(
                        new Term("baseline_field", regexpPattern),
                        RegExp.ALL,
                        0,
                        RegexpQuery.DEFAULT_PROVIDER,
                        1000,
                        MultiTermQuery.DOC_VALUES_REWRITE
                    );
                    TopDocs baselineResults = searcher.search(baselineQuery, 64);

                    Query contenderQuery = new ScanningBinaryDocValuesRegexpQuery(
                        "contender_field",
                        regexpPattern,
                        RegExp.ALL,
                        0,
                        1000,
                        false
                    );
                    TopDocs contenderResults = searcher.search(contenderQuery, 64);

                    assumeTrue("no baseline matches for pattern " + regexpPattern, baselineResults.scoreDocs.length >= 1);
                    assertThat(contenderResults.totalHits, equalTo(baselineResults.totalHits));
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
                addDoc(writer, fieldName, "Hello");
                addDoc(writer, fieldName, "WORLD");
                addDoc(writer, fieldName, "foo");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    // Case-sensitive: only lowercase "foo" matches "foo"
                    assertEquals(1, searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, "foo", 0, 0, 1000, false)));

                    // Case-insensitive via RegExp.ASCII_CASE_INSENSITIVE matchFlag: matches "foo" and "FOO" (none here, so still 1)
                    assertEquals(
                        1,
                        searcher.count(
                            new ScanningBinaryDocValuesRegexpQuery(fieldName, "foo", 0, RegExp.ASCII_CASE_INSENSITIVE, 1000, false)
                        )
                    );

                    // Case-insensitive "hello" matches "Hello"
                    assertEquals(
                        1,
                        searcher.count(
                            new ScanningBinaryDocValuesRegexpQuery(fieldName, "hello", 0, RegExp.ASCII_CASE_INSENSITIVE, 1000, false)
                        )
                    );

                    // Case-insensitive "world" matches "WORLD"
                    assertEquals(
                        1,
                        searcher.count(
                            new ScanningBinaryDocValuesRegexpQuery(fieldName, "world", 0, RegExp.ASCII_CASE_INSENSITIVE, 1000, false)
                        )
                    );

                    // Case-sensitive "hello" does NOT match "Hello"
                    assertEquals(0, searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, "hello", 0, 0, 1000, false)));
                }
            }
        }
    }

    public void testMatchesAll() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "anything");
                addDoc(writer, fieldName, "something");
                addDoc(writer, fieldName, "");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // ".*" matches all docs
                    assertEquals(3, searcher.count(new ScanningBinaryDocValuesRegexpQuery(fieldName, ".*", 0, 0, 1000, false)));
                }
            }
        }
    }

    public void testToString() {
        ScanningBinaryDocValuesRegexpQuery query = new ScanningBinaryDocValuesRegexpQuery("my_field", "foo.*", 0, 0, 1000, false);
        // toString must always use the stored field name, not the Lucene context parameter
        assertThat(query.toString("other_field"), containsString("my_field"));
        assertThat(query.toString(""), containsString("my_field"));
    }

    public void testVisitor() {
        String fieldName = "my_field";
        ScanningBinaryDocValuesRegexpQuery query = new ScanningBinaryDocValuesRegexpQuery(fieldName, "hel.*", 0, 0, 1000, false);

        // consumeTermsMatching must be called with the correct field and a working automaton
        boolean[] called = { false };
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTermsMatching(Query q, String field, Supplier<ByteRunAutomaton> automaton) {
                called[0] = true;
                assertSame(query, q);
                assertEquals(fieldName, field);
                ByteRunAutomaton a = automaton.get();
                assertNotNull(a);
                byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
                byte[] world = "world".getBytes(StandardCharsets.UTF_8);
                assertTrue(a.run(hello, 0, hello.length));
                assertFalse(a.run(world, 0, world.length));
            }
        });
        assertTrue("consumeTermsMatching was not called", called[0]);

        // acceptField returning false must suppress the call
        boolean[] calledForRejectedField = { false };
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                return false;
            }

            @Override
            public void consumeTermsMatching(Query q, String field, Supplier<ByteRunAutomaton> automaton) {
                calledForRejectedField[0] = true;
            }
        });
        assertFalse(calledForRejectedField[0]);
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
