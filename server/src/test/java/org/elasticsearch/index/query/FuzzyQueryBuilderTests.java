/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Strings;
import org.elasticsearch.lucene.search.FuzzyQueries;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FuzzyQueryBuilderTests extends AbstractQueryTestCase<FuzzyQueryBuilder> {

    @Override
    protected FuzzyQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
        FuzzyQueryBuilder query = new FuzzyQueryBuilder(fieldName, getRandomValueForFieldName(fieldName));
        if (randomBoolean()) {
            query.fuzziness(randomFuzziness(query.fieldName()));
        }
        if (randomBoolean()) {
            query.prefixLength(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            query.maxExpansions(randomIntBetween(1, 10));
        }
        if (randomBoolean()) {
            query.transpositions(randomBoolean());
        }
        if (randomBoolean()) {
            query.rewrite(getRandomRewriteMethod());
        }
        return query;
    }

    @Override
    protected Map<String, FuzzyQueryBuilder> getAlternateVersions() {
        Map<String, FuzzyQueryBuilder> alternateVersions = new HashMap<>();
        FuzzyQueryBuilder fuzzyQuery = new FuzzyQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
        String contentString = Strings.format("""
            {
                "fuzzy" : {
                    "%s" : "%s"
                }
            }""", fuzzyQuery.fieldName(), fuzzyQuery.value());
        alternateVersions.put(contentString, fuzzyQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(FuzzyQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(FuzzyQuery.class));

        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
        String actualFieldName = fuzzyQuery.getTerm().field();
        assertThat(actualFieldName, equalTo(expectedFieldName));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder(null, "text"));
        assertEquals("field name cannot be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder("", "text"));
        assertEquals("field name cannot be null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new FuzzyQueryBuilder("field", null));
        assertEquals("query value cannot be null", e.getMessage());
    }

    public void testToQueryWithStringField() throws IOException {
        String query = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        Query parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) parsedQuery;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) boostQuery.getQuery();
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "sh")));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(Fuzziness.AUTO.asDistance("sh")));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
    }

    public void testToQueryWithStringFieldDefinedFuzziness() throws IOException {
        String query = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:2,5",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        Query parsedQuery = parseQuery(query).toQuery(createSearchExecutionContext());
        assertThat(parsedQuery, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) parsedQuery;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(FuzzyQuery.class));
        FuzzyQuery fuzzyQuery = (FuzzyQuery) boostQuery.getQuery();
        assertThat(fuzzyQuery.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "sh")));
        assertThat(fuzzyQuery.getMaxEdits(), equalTo(1));
        assertThat(fuzzyQuery.getPrefixLength(), equalTo(1));
    }

    public void testToQueryWithStringFieldDefinedWrongFuzziness() throws IOException {
        String queryMissingFuzzinessUpLimit = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:2",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessUpLimit).toQuery(createSearchExecutionContext())
        );
        String msg = "failed to find low and high distance values";
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));

        String queryHavingNegativeFuzzinessLowLimit = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:-1,6",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        String msg2 = "fuzziness wrongly configured";
        ElasticsearchParseException e2 = expectThrows(
            ElasticsearchParseException.class,
            () -> parseQuery(queryHavingNegativeFuzzinessLowLimit).toQuery(createSearchExecutionContext())
        );
        assertTrue(e2.getMessage() + " didn't contain: " + msg2 + " but: " + e.getMessage(), e.getMessage().contains(msg));

        String queryMissingFuzzinessUpLimit2 = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:1,",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessUpLimit2).toQuery(createSearchExecutionContext())
        );
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));

        String queryMissingFuzzinessLowLimit = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":"sh",
                        "fuzziness": "AUTO:,5",
                        "prefix_length":1,
                        "boost":2.0
                    }
                }
            }""", TEXT_FIELD_NAME);
        e = expectThrows(
            ElasticsearchParseException.class,
            () -> parseQuery(queryMissingFuzzinessLowLimit).toQuery(createSearchExecutionContext())
        );
        msg = "failed to parse [AUTO:,5] as a \"auto:int,int\"";
        assertTrue(e.getMessage() + " didn't contain: " + msg + " but: " + e.getMessage(), e.getMessage().contains(msg));
    }

    public void testToQueryWithNumericField() throws IOException {
        String query = Strings.format("""
            {
                "fuzzy":{
                    "%s":{
                        "value":12,
                        "fuzziness":2
                    }
                }
            }
            """, INT_FIELD_NAME);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> parseQuery(query).toQuery(createSearchExecutionContext())
        );
        assertEquals(
            "Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage()
        );
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "fuzzy" : {
                "user" : {
                  "value" : "ki",
                  "fuzziness" : "2",
                  "prefix_length" : 0,
                  "max_expansions" : 100,
                  "transpositions" : false,
                  "boost" : 42.0
                }
              }
            }""";
        FuzzyQueryBuilder parsed = (FuzzyQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, 42.0, parsed.boost(), 0.00001);
        assertEquals(json, 2, parsed.fuzziness().asFloat(), 0f);
        assertEquals(json, false, parsed.transpositions());
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json1 = """
            {
              "fuzzy" : {
                "message1" : {
                  "value" : "this is a test"
                }
              }
            }""";
        parseQuery(json1); // should be all good

        String json2 = """
            {
              "fuzzy" : {
                "message1" : {
                  "value" : "this is a test"
                },
                "message2" : {
                  "value" : "this is a test"
                }
              }
            }""";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json2));
        assertEquals("[fuzzy] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = """
            {
              "fuzzy" : {
                "message1" : "this is a test",
                "message2" : "value" : "this is a test"
              }
            }""";

        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[fuzzy] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());
    }

    public void testParseFailsWithValueArray() {
        String query = """
            {
              "fuzzy" : {
                "message1" : {
                  "value" : [ "one", "two", "three"]
                }
              }
            }""";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(query));
        assertEquals("[fuzzy] unexpected token [START_ARRAY] after [value]", e.getMessage());
    }

    public void testToQueryWithTranspositions() throws Exception {
        Query query = new FuzzyQueryBuilder(TEXT_FIELD_NAME, "text").toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(FuzzyQuery.defaultTranspositions, ((FuzzyQuery) query).getTranspositions());

        query = new FuzzyQueryBuilder(TEXT_FIELD_NAME, "text").transpositions(true).toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(true, ((FuzzyQuery) query).getTranspositions());

        query = new FuzzyQueryBuilder(TEXT_FIELD_NAME, "text").transpositions(false).toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(FuzzyQuery.class));
        assertEquals(false, ((FuzzyQuery) query).getTranspositions());
    }

    public void testManyFuzzyClausesTripCircuitBreaker() {
        assertCircuitBreakerTripsOnQueryConstruction("1kb", () -> {
            BoolQueryBuilder boolQuery = new BoolQueryBuilder();
            IntStream.range(0, 500).forEach(i -> boolQuery.should(new FuzzyQueryBuilder(TEXT_FIELD_NAME, "value" + i)));
            return boolQuery;
        });
    }

    public void testSingleFuzzyClauseTripsCircuitBreakerAtConstruction() throws IOException {
        CircuitBreaker cb = createCircuitBreakerService("2kb");
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            expectThrows(CircuitBreakingException.class, () -> new FuzzyQueryBuilder(TEXT_FIELD_NAME, "value").toQuery(context));
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    public void testFuzzyCostEstimateChargedUpfront() throws IOException {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            long cbBaseline = cb.getUsed();
            FuzzyQuery query = (FuzzyQuery) new FuzzyQueryBuilder(TEXT_FIELD_NAME, "value").toQuery(context);

            long totalCharged = context.getQueryConstructionMemoryUsed();
            long fieldTypeBytes = FuzzyQueries.queryRamBytes(query);
            long costEstimate = totalCharged - fieldTypeBytes;

            assertTrue("field-type fuzzy must charge the query object's retained heap", fieldTypeBytes > 0);
            assertTrue("breaker must be charged upfront for the estimated automata cost", costEstimate > 0);
            assertEquals(
                "circuit breaker delta must equal the sum of field-type and cost-estimate charges",
                totalCharged,
                cb.getUsed() - cbBaseline
            );
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    public void testFuzzyRewriteAtSearchTimeDoesNotAddBreakerCharges() throws IOException {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            Query query = new FuzzyQueryBuilder(TEXT_FIELD_NAME, "value").toQuery(context);
            long chargedAfterConstruction = context.getQueryConstructionMemoryUsed();

            try (Directory dir = new ByteBuffersDirectory()) {
                try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
                    for (int i = 0; i < 8; i++) {
                        Document doc = new Document();
                        doc.add(new StringField(TEXT_FIELD_NAME, "value" + i, Field.Store.NO));
                        w.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    new IndexSearcher(reader).rewrite(query);
                }
            }

            assertEquals(
                "search-time rewrite must not add any further breaker charges — all charging is upfront",
                chargedAfterConstruction,
                context.getQueryConstructionMemoryUsed()
            );
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    public void testFuzzyQueryEndToEndSearchHonorsOncePerPhaseAccounting() throws IOException {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            long cbBaseline = cb.getUsed();
            Query query = new FuzzyQueryBuilder(TEXT_FIELD_NAME, "value").toQuery(context);
            long chargedAfterConstruction = context.getQueryConstructionMemoryUsed();
            assertTrue("a fuzzy clause must charge the breaker via the once-per-phase visitor walk", chargedAfterConstruction > 0);
            assertEquals(
                "breaker delta must equal the visitor's committed total after construction",
                chargedAfterConstruction,
                cb.getUsed() - cbBaseline
            );

            try (Directory dir = new ByteBuffersDirectory()) {
                try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
                    addStringDoc(w, "value");            // 0 edits — exact match
                    addStringDoc(w, "vlaue");            // 1 Damerau-Levenshtein edit (a↔l transposition)
                    addStringDoc(w, "totallydifferent"); // well beyond Fuzziness.AUTO's 1-edit budget for "value"
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    TopDocs topDocs = searcher.search(query, 10);
                    assertEquals(
                        "fuzzy search must return the exact match and the 1-edit transposition neighbour, "
                            + "and must not return the unrelated doc",
                        2L,
                        topDocs.totalHits.value()
                    );
                }
            }

            assertEquals(
                "end-to-end search execution must not add any further breaker charges — all charging is upfront",
                chargedAfterConstruction,
                context.getQueryConstructionMemoryUsed()
            );
            assertEquals(
                "end-to-end search execution must not move the breaker counter beyond the construction commit",
                chargedAfterConstruction,
                cb.getUsed() - cbBaseline
            );
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    private static void addStringDoc(IndexWriter w, String value) throws IOException {
        Document doc = new Document();
        doc.add(new StringField(TEXT_FIELD_NAME, value, Field.Store.NO));
        w.addDocument(doc);
    }

    public void testFuzzyConstructionMemoryReleasedOnRelease() throws IOException {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);

        long cbBaseline = cb.getUsed();
        new FuzzyQueryBuilder(TEXT_FIELD_NAME, "value").toQuery(context);
        assertTrue("construction-time charge should be recorded", cb.getUsed() > cbBaseline);

        context.releaseQueryConstructionMemory();
        assertEquals("breaker pool must be drained on release", 0L, context.getQueryConstructionMemoryUsed());
        assertEquals("circuit breaker bookkeeping must be fully restored", cbBaseline, cb.getUsed());
    }

    public void testAddCircuitBreakerMemoryRejectsNegativeAndIgnoresZero() {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            long cbBaseline = cb.getUsed();

            context.addCircuitBreakerMemory(0L, "fuzzy:" + TEXT_FIELD_NAME);
            assertEquals("zero-byte charges must not move the per-request pool", 0L, context.getQueryConstructionMemoryUsed());
            assertEquals("zero-byte charges must not touch breaker bookkeeping", cbBaseline, cb.getUsed());

            AssertionError ae = expectThrows(AssertionError.class, () -> context.addCircuitBreakerMemory(-1L, "fuzzy:" + TEXT_FIELD_NAME));
            assertThat(ae.getMessage(), containsString("negative breaker charge"));

            assertEquals("rejected negative charges must not move the per-request pool", 0L, context.getQueryConstructionMemoryUsed());
            assertEquals("rejected negative charges must not touch breaker bookkeeping", cbBaseline, cb.getUsed());
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    public void testFuzzyCostEstimateScalesWithParameters() throws IOException {
        long shortTermOneEdit = costEstimateFor("abc", Fuzziness.ONE, 0);
        long longerTermOneEdit = costEstimateFor("abcdefghij", Fuzziness.ONE, 0);
        long shortTermTwoEdits = costEstimateFor("abc", Fuzziness.TWO, 0);
        long longerTermLargePrefix = costEstimateFor("abcdefghij", Fuzziness.ONE, 8);

        assertTrue(
            "longer term must yield a larger cost estimate (" + shortTermOneEdit + " vs " + longerTermOneEdit + ")",
            longerTermOneEdit > shortTermOneEdit
        );
        assertTrue(
            "higher max edits must yield a larger cost estimate (" + shortTermOneEdit + " vs " + shortTermTwoEdits + ")",
            shortTermTwoEdits > shortTermOneEdit
        );
        assertTrue(
            "larger prefix must reduce the cost estimate (" + longerTermOneEdit + " vs " + longerTermLargePrefix + ")",
            longerTermLargePrefix < longerTermOneEdit
        );
    }

    public void testFieldTypeFuzzyQueryDoesNotChargeBreakerDirectly() throws IOException {
        assertFieldTypeFuzzyDoesNotChargeDirectly(null);
    }

    public void testFieldTypeFuzzyQueryWithUserRewriteDoesNotChargeBreakerDirectly() throws IOException {
        assertFieldTypeFuzzyDoesNotChargeDirectly(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);
    }

    private long costEstimateFor(String value, Fuzziness fuzziness, int prefixLength) throws IOException {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            FuzzyQuery query = (FuzzyQuery) new FuzzyQueryBuilder(TEXT_FIELD_NAME, value).fuzziness(fuzziness)
                .prefixLength(prefixLength)
                .toQuery(context);
            return context.getQueryConstructionMemoryUsed() - FuzzyQueries.queryRamBytes(query);
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }

    private void assertFieldTypeFuzzyDoesNotChargeDirectly(MultiTermQuery.RewriteMethod rewriteMethod) throws IOException {
        CircuitBreaker cb = createCircuitBreakerService();
        SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), cb);
        try {
            long cbBefore = cb.getUsed();
            long tallyBefore = context.getQueryConstructionMemoryUsed();
            FuzzyQuery query = (FuzzyQuery) context.getFieldType(TEXT_FIELD_NAME)
                .fuzzyQuery("value", Fuzziness.fromEdits(2), 1, 50, true, context, rewriteMethod);

            assertEquals(FuzzyQuery.class, query.getClass());
            if (rewriteMethod != null) {
                assertSame("user-supplied rewrite must be preserved on the query", rewriteMethod, query.getRewriteMethod());
            }
            assertTrue(
                "the produced query must still have a non-zero retained-heap estimate, even though "
                    + "fieldType.fuzzyQuery() no longer charges the breaker itself",
                FuzzyQueries.estimateBytes(query) > 0
            );
            assertEquals(
                "fieldType.fuzzyQuery() must not commit to the request circuit breaker; the visitor "
                    + "walk in AbstractQueryBuilder#toQuery charges once per phase via FuzzyQueries.estimateBytes",
                cbBefore,
                cb.getUsed()
            );
            assertEquals(
                "fieldType.fuzzyQuery() must not touch the per-request query-construction tally",
                tallyBefore,
                context.getQueryConstructionMemoryUsed()
            );
        } finally {
            context.releaseQueryConstructionMemory();
        }
    }
}
