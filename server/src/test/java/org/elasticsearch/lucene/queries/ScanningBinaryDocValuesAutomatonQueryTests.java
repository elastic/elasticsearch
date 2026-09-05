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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.ARRAY_ORDER_INLINE_NULL;
import static org.elasticsearch.index.mapper.BinaryDocValuesFormat.SEPARATE_COUNT;
import static org.elasticsearch.lucene.queries.ScanningBinaryDocValuesAutomatonQuery.getContainsPattern;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ScanningBinaryDocValuesAutomatonQueryTests extends ESTestCase {

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
                    // Automaton wildcard "be*" matches "beta" and "best"; the all-null doc preceding "best" must not be matched.
                    assertEquals(
                        2,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "be*", false, ARRAY_ORDER_INLINE_NULL))
                    );
                    // "*et*" short-circuits to a contains query: with a multi-valued doc present in the segment the contains fast path is
                    // gated off and the per-value decode fallback runs, so only "beta" (which contains "et") matches — not "best".
                    assertEquals(
                        1,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "*et*", false, ARRAY_ORDER_INLINE_NULL))
                    );
                    // "*ph*" contains-matches "alpha" only.
                    assertEquals(
                        1,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "*ph*", false, ARRAY_ORDER_INLINE_NULL))
                    );
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
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                for (var entry : expectedCounts.entrySet()) {
                    for (int i = 0; i < entry.getValue(); i++) {
                        Document document = new Document();

                        var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                            "field",
                            MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                        );
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
                        long count = searcher.count(
                            ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, entry.getKey() + "*", false, SEPARATE_COUNT)
                        );
                        assertEquals(entry.getValue().longValue(), count);
                    }
                }
            }
        }
    }

    /** Exercises the primary constructor with a union automaton — the shape ESQL's LIKE-list pushdown produces. */
    public void testBasicsUnionAutomaton() throws Exception {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "alpha");
                addDoc(writer, fieldName, "beta");
                addDoc(writer, fieldName, "gamma");
                addDoc(writer, fieldName, "delta");
                addDoc(writer, fieldName, "alpha", "gamma"); // multi-valued: both arms match, count as 1 doc

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    // union of "alpha" and "beta" — matches first, second, and fifth docs
                    Automaton automaton = Operations.determinize(
                        Operations.union(Arrays.asList(Automata.makeString("alpha"), Automata.makeString("beta"))),
                        Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
                    );
                    Query query = new ScanningBinaryDocValuesAutomatonQuery(
                        fieldName,
                        automaton,
                        SEPARATE_COUNT,
                        "LIKE(\"alpha\", \"beta\"), caseInsensitive=false"
                    );
                    assertEquals(3, searcher.count(query));
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
                    Query query = ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "a*", false, SEPARATE_COUNT);
                    assertEquals(0, searcher.count(query));
                }
            }
        }

        // no field in segment
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                Document document = new Document();

                var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                    "field",
                    MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
                );
                field.add(new BytesRef("a".getBytes(StandardCharsets.UTF_8)));
                var countField = NumericDocValuesField.indexedField("field.counts", 1);
                document.add(field);
                document.add(countField);

                writer.addDocument(document);
                writer.commit();
                writer.addDocument(new Document());
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "a*", false, SEPARATE_COUNT);
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
                    String randomWildcard = randomFrom(randomValues).substring(0, 2) + "*";
                    IndexSearcher searcher = newSearcher(reader);

                    Query baselineQuery = new WildcardQuery(
                        new Term("baseline_field", randomWildcard),
                        Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                        MultiTermQuery.DOC_VALUES_REWRITE
                    );
                    TopDocs baselineResults = searcher.search(baselineQuery, 32);

                    Query contenderQuery = ScanningBinaryDocValuesAutomatonQuery.forWildcard(
                        "contender_field",
                        randomWildcard,
                        false,
                        SEPARATE_COUNT
                    );
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

    public void testForWildcardReturnsContainsQueryForStarLiteralStar() throws IOException {
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
                    Query query = ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "*search*", false, SEPARATE_COUNT);
                    assertThat(query, instanceOf(BinaryDocValuesContainsTermQuery.class));
                    assertEquals(3, searcher.count(query));
                }
            }
        }
    }

    public void testForWildcardReturnsContainsQueryMultiValued() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "hello", "world");
                addDoc(writer, fieldName, "foo", "bar");
                addDoc(writer, fieldName, "wellcome", "to");
                addDoc(writer, fieldName, "abc");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "*ell*", false, SEPARATE_COUNT);
                    assertThat(query, instanceOf(BinaryDocValuesContainsTermQuery.class));
                    assertEquals(2, searcher.count(query));
                }
            }
        }
    }

    public void testCaseInsensitiveNotOptimizedToContains() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "Elasticsearch");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    Query query = ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "*search*", true, SEPARATE_COUNT);
                    assertThat(query, instanceOf(ScanningBinaryDocValuesAutomatonQuery.class));
                    assertEquals(1, searcher.count(query));
                }
            }
        }
    }

    public void testNonContainsPatternNotOptimized() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "foobar");

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    Query prefixQuery = ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "foo*", false, SEPARATE_COUNT);
                    assertThat(prefixQuery, instanceOf(ScanningBinaryDocValuesAutomatonQuery.class));
                    assertThat(prefixQuery, sameInstance(prefixQuery.rewrite(searcher)));

                    Query multiWildcard = ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "*foo*bar*", false, SEPARATE_COUNT);
                    assertThat(multiWildcard, instanceOf(ScanningBinaryDocValuesAutomatonQuery.class));

                    Query singleCharWildcard = ScanningBinaryDocValuesAutomatonQuery.forWildcard(fieldName, "*fo?*", false, SEPARATE_COUNT);
                    assertThat(singleCharWildcard, instanceOf(ScanningBinaryDocValuesAutomatonQuery.class));
                }
            }
        }
    }

    public void testToString() {
        // wildcard factory — description contains the pattern and caseInsensitive flag
        Query q1 = ScanningBinaryDocValuesAutomatonQuery.forWildcard("my_field", "foo*", false, SEPARATE_COUNT);
        String str1 = q1.toString("other_field");
        assertThat(str1, containsString("my_field")); // stored fieldName, not the Lucene context param
        assertThat(str1, containsString("foo*"));
        assertThat(str1, not(containsString("other_field")));

        // primary ctor — description is caller-supplied
        Automaton automaton = Automata.makeString("hello");
        Query q2 = new ScanningBinaryDocValuesAutomatonQuery("my_field", automaton, SEPARATE_COUNT, "custom desc");
        assertThat(q2.toString("other_field"), containsString("my_field"));
        assertThat(q2.toString("other_field"), containsString("custom desc"));
    }

    public void testVisitor() {
        Automaton automaton = Automata.makeString("hello");
        ScanningBinaryDocValuesAutomatonQuery query = new ScanningBinaryDocValuesAutomatonQuery(
            "my_field",
            automaton,
            SEPARATE_COUNT,
            "desc"
        );

        AtomicBoolean called = new AtomicBoolean(false);
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                return "my_field".equals(field);
            }

            @Override
            public void consumeTermsMatching(Query query, String field, java.util.function.Supplier<ByteRunAutomaton> automaton) {
                called.set(true);
                assertEquals("my_field", field);
                // the automaton supplier returns a functional ByteRunAutomaton
                ByteRunAutomaton bra = automaton.get();
                BytesRef hello = new BytesRef("hello");
                assertTrue(bra.run(hello.bytes, hello.offset, hello.length));
                BytesRef world = new BytesRef("world");
                assertFalse(bra.run(world.bytes, world.offset, world.length));
            }
        });
        assertTrue("consumeTermsMatching should have been called", called.get());

        // acceptField == false → consumeTermsMatching not called
        AtomicBoolean notCalled = new AtomicBoolean(false);
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                return false;
            }

            @Override
            public void consumeTermsMatching(Query q, String field, java.util.function.Supplier<ByteRunAutomaton> automaton) {
                notCalled.set(true);
            }
        });
        assertFalse("consumeTermsMatching should NOT have been called when acceptField returns false", notCalled.get());
    }

    public void testEqualsAndHashCode() {
        Automaton automaton1 = Automata.makeString("hello");
        Automaton automaton2 = Automata.makeString("hello"); // same language, built independently
        Automaton automaton3 = Automata.makeString("world"); // different language

        // Instances with structurally equal automatons are equal regardless of description.
        // Description is display-only and does not participate in equals/hashCode.
        ScanningBinaryDocValuesAutomatonQuery q1 = new ScanningBinaryDocValuesAutomatonQuery("field", automaton1, SEPARATE_COUNT, "desc-a");
        ScanningBinaryDocValuesAutomatonQuery q2 = new ScanningBinaryDocValuesAutomatonQuery(
            "field",
            automaton2,
            SEPARATE_COUNT,
            "desc-b" // different description, same automaton
        );
        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());

        // Different automaton → not equal.
        ScanningBinaryDocValuesAutomatonQuery q3 = new ScanningBinaryDocValuesAutomatonQuery("field", automaton3, SEPARATE_COUNT, "desc-a");
        assertNotEquals(q1, q3);

        // Different field → not equal.
        ScanningBinaryDocValuesAutomatonQuery q4 = new ScanningBinaryDocValuesAutomatonQuery(
            "other_field",
            automaton1,
            SEPARATE_COUNT,
            "desc-a"
        );
        assertNotEquals(q1, q4);

        // Different binaryFormat → not equal.
        ScanningBinaryDocValuesAutomatonQuery q5 = new ScanningBinaryDocValuesAutomatonQuery(
            "field",
            automaton1,
            ARRAY_ORDER_INLINE_NULL,
            "desc-a"
        );
        assertNotEquals(q1, q5);

        // forWildcard with the same inputs produces equal instances.
        Query fw1 = ScanningBinaryDocValuesAutomatonQuery.forWildcard("field", "foo*", false, SEPARATE_COUNT);
        Query fw2 = ScanningBinaryDocValuesAutomatonQuery.forWildcard("field", "foo*", false, SEPARATE_COUNT);
        assertEquals(fw1, fw2);
        assertEquals(fw1.hashCode(), fw2.hashCode());

        // forWildcard differs with different caseInsensitive (different automaton).
        Query fw3 = ScanningBinaryDocValuesAutomatonQuery.forWildcard("field", "foo*", true, SEPARATE_COUNT);
        assertNotEquals(fw1, fw3);

        // Instances of different types are never equal (sameClassAs check).
        assertNotEquals(q1, new ScanningBinaryDocValuesRegexpQuery("field", "hello", 0, 0, 10, SEPARATE_COUNT, null));
    }

    public void testUnionMatchesAllPatterns() throws IOException {
        String fieldName = "field";
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = newRandomIndexWriter(dir)) {
                addDoc(writer, fieldName, "apple");
                addDoc(writer, fieldName, "banana");
                addDoc(writer, fieldName, "cherry");
                addDoc(writer, fieldName, "date"); // should not match

                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);

                    // Three-pattern union: apple | banana | cherry
                    Automaton automaton = Operations.determinize(
                        Operations.union(
                            Arrays.asList(Automata.makeString("apple"), Automata.makeString("banana"), Automata.makeString("cherry"))
                        ),
                        Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
                    );
                    Query query = new ScanningBinaryDocValuesAutomatonQuery(
                        fieldName,
                        automaton,
                        SEPARATE_COUNT,
                        "LIKE(\"apple\", \"banana\", \"cherry\"), caseInsensitive=false"
                    );
                    assertEquals(3, searcher.count(query));
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

    /**
     * Verifies that {@link ScanningBinaryDocValuesAutomatonQuery#forCaseInsensitiveTerm} matches documents whose value equals the
     * target string under Unicode case-folding, regardless of the original case written to the index.
     */
    public void testForCaseInsensitiveTerm() throws Exception {
        String fieldName = "field";
        BinaryDocValuesFormat format = randomFormat();
        try (org.apache.lucene.store.Directory dir = newDirectory()) {
            try (
                RandomIndexWriter writer = format == ARRAY_ORDER_INLINE_NULL
                    ? ArrayOrderInlineNullTestUtils.newWriter(dir)
                    : newRandomIndexWriter(dir)
            ) {
                if (format == ARRAY_ORDER_INLINE_NULL) {
                    ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "Elasticsearch");
                    ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "KIBANA", null, "logstash");
                    ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "nosuchterm");
                } else {
                    addDoc(writer, fieldName, "Elasticsearch");
                    addDoc(writer, fieldName, "KIBANA", "logstash");
                    addDoc(writer, fieldName, "nosuchterm");
                }
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // "elasticsearch" in any case matches the first doc
                    assertEquals(
                        1,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forCaseInsensitiveTerm(fieldName, "elasticsearch", format))
                    );
                    assertEquals(
                        1,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forCaseInsensitiveTerm(fieldName, "ELASTICSEARCH", format))
                    );
                    // "kibana" matches the second doc (stored as "KIBANA")
                    assertEquals(
                        1,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forCaseInsensitiveTerm(fieldName, "kibana", format))
                    );
                    // "logstash" matches the second doc
                    assertEquals(
                        1,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forCaseInsensitiveTerm(fieldName, "logstash", format))
                    );
                    // unrelated value → no match
                    assertEquals(
                        0,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forCaseInsensitiveTerm(fieldName, "beats", format))
                    );
                }
            }
        }
    }

    /**
     * Verifies that {@link ScanningBinaryDocValuesAutomatonQuery#forFuzzy} matches documents within the specified edit distance
     * and does not match documents whose values are farther away.
     */
    public void testForFuzzy() throws Exception {
        String fieldName = "field";
        BinaryDocValuesFormat format = randomFormat();
        try (org.apache.lucene.store.Directory dir = newDirectory()) {
            try (
                RandomIndexWriter writer = format == ARRAY_ORDER_INLINE_NULL
                    ? ArrayOrderInlineNullTestUtils.newWriter(dir)
                    : newRandomIndexWriter(dir)
            ) {
                if (format == ARRAY_ORDER_INLINE_NULL) {
                    ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "elastic");    // exact
                    ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "elastik");    // 1 edit
                    ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "elasti");     // 1 edit (deletion)
                    ArrayOrderInlineNullTestUtils.addDoc(writer, fieldName, "something");  // too far
                } else {
                    addDoc(writer, fieldName, "elastic");
                    addDoc(writer, fieldName, "elastik");
                    addDoc(writer, fieldName, "elasti");
                    addDoc(writer, fieldName, "something");
                }
                try (IndexReader reader = writer.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    // maxEdits=1 matches "elastic", "elastik", and "elasti" (3 docs); "something" is too far
                    assertEquals(
                        3,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forFuzzy(fieldName, "elastic", 1, 0, true, format))
                    );
                    // exact match only (maxEdits=0)
                    assertEquals(
                        1,
                        searcher.count(ScanningBinaryDocValuesAutomatonQuery.forFuzzy(fieldName, "elastic", 0, 0, true, format))
                    );
                }
            }
        }
    }

    private static BinaryDocValuesFormat randomFormat() {
        return randomFrom(SEPARATE_COUNT, ARRAY_ORDER_INLINE_NULL);
    }
}
