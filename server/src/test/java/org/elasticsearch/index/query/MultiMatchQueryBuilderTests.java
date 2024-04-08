/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBooleanSubQuery;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class MultiMatchQueryBuilderTests extends AbstractQueryTestCase<MultiMatchQueryBuilder> {
    private static final String MISSING_WILDCARD_FIELD_NAME = "missing_*";
    private static final String MISSING_FIELD_NAME = "missing";

    @Override
    protected MultiMatchQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(
            TEXT_FIELD_NAME,
            INT_FIELD_NAME,
            DOUBLE_FIELD_NAME,
            BOOLEAN_FIELD_NAME,
            DATE_FIELD_NAME,
            MISSING_FIELD_NAME,
            MISSING_WILDCARD_FIELD_NAME
        );

        final Object value;
        if (fieldName.equals(TEXT_FIELD_NAME)) {
            value = getRandomQueryText();
        } else {
            value = getRandomValueForFieldName(fieldName);
        }

        final MultiMatchQueryBuilder query;
        if (rarely()) {
            query = new MultiMatchQueryBuilder(value, fieldName);
            if (randomBoolean()) {
                query.lenient(randomBoolean());
            }
            // field with random boost
            if (randomBoolean()) {
                query.field(fieldName, randomFloat() * 10);
            }
        } else {
            query = new MultiMatchQueryBuilder(value);
            query.lenient(true);
        }

        // sets other parameters of the multi match query
        if (randomBoolean()) {
            if (fieldName.equals(TEXT_FIELD_NAME) || fieldName.equals(TEXT_ALIAS_FIELD_NAME) || fieldName.equals(KEYWORD_FIELD_NAME)) {
                query.type(randomFrom(MultiMatchQueryBuilder.Type.values()));
            } else {
                query.type(
                    randomValueOtherThanMany(
                        (type) -> type == Type.PHRASE_PREFIX || type == Type.BOOL_PREFIX,
                        () -> randomFrom(MultiMatchQueryBuilder.Type.values())
                    )
                );
            }
        }
        if (randomBoolean()) {
            query.operator(randomFrom(Operator.values()));
        }
        if (randomBoolean() && fieldName.equals(TEXT_FIELD_NAME)) {
            query.analyzer(randomAnalyzer());
        }
        if (randomBoolean() && query.type() != Type.BOOL_PREFIX) {
            query.slop(randomIntBetween(0, 5));
        }
        if (fieldName.equals(TEXT_FIELD_NAME)
            && randomBoolean()
            && (query.type() == Type.BEST_FIELDS || query.type() == Type.MOST_FIELDS)) {
            query.fuzziness(randomFuzziness(fieldName));
        }
        if (randomBoolean()) {
            query.prefixLength(randomIntBetween(0, 5));
        }
        if (randomBoolean()) {
            query.maxExpansions(randomIntBetween(1, 5));
        }
        if (randomBoolean()) {
            query.minimumShouldMatch(randomMinimumShouldMatch());
        }
        if (randomBoolean()) {
            query.fuzzyRewrite(getRandomRewriteMethod());
        }
        if (randomBoolean()) {
            query.tieBreaker(randomFloat());
        }
        if (randomBoolean()) {
            query.zeroTermsQuery(randomFrom(ZeroTermsQueryOption.NONE, ZeroTermsQueryOption.ALL));
        }
        if (randomBoolean()) {
            query.autoGenerateSynonymsPhraseQuery(randomBoolean());
        }
        if (randomBoolean()) {
            query.fuzzyTranspositions(randomBoolean());
        }
        // test with fields with boost and patterns delegated to the tests further below
        return query;
    }

    @Override
    protected Map<String, MultiMatchQueryBuilder> getAlternateVersions() {
        Map<String, MultiMatchQueryBuilder> alternateVersions = new HashMap<>();
        String query = """
            {
                "multi_match": {
                    "query": "foo bar",
                    "fields": "myField"
                }
            }""";
        alternateVersions.put(query, new MultiMatchQueryBuilder("foo bar", "myField"));
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MultiMatchQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        // we rely on integration tests for deeper checks here
        assertThat(
            query,
            anyOf(
                Arrays.asList(
                    instanceOf(BoostQuery.class),
                    instanceOf(TermQuery.class),
                    instanceOf(BooleanQuery.class),
                    instanceOf(DisjunctionMaxQuery.class),
                    instanceOf(FuzzyQuery.class),
                    instanceOf(MultiPhrasePrefixQuery.class),
                    instanceOf(MatchAllDocsQuery.class),
                    instanceOf(MatchNoDocsQuery.class),
                    instanceOf(PhraseQuery.class),
                    instanceOf(PointRangeQuery.class),
                    instanceOf(IndexOrDocValuesQuery.class),
                    instanceOf(PrefixQuery.class)
                )
            )
        );
    }

    public void testIllegaArguments() {
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder(null, "field"));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", (String[]) null));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", new String[] { "" }));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", "field").type(null));
    }

    public void testToQueryBoost() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("test");
        multiMatchQueryBuilder.field(TEXT_FIELD_NAME, 5f);
        Query query = multiMatchQueryBuilder.toQuery(searchExecutionContext);
        assertTermOrBoostQuery(query, TEXT_FIELD_NAME, "test", 5f);

        multiMatchQueryBuilder = new MultiMatchQueryBuilder("test");
        multiMatchQueryBuilder.field(TEXT_FIELD_NAME, 5f);
        multiMatchQueryBuilder.boost(2f);
        query = multiMatchQueryBuilder.toQuery(searchExecutionContext);
        assertThat(query, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2f));
        assertTermOrBoostQuery(boostQuery.getQuery(), TEXT_FIELD_NAME, "test", 5f);
    }

    public void testToQueryMultipleTermsBooleanQuery() throws Exception {
        Query query = multiMatchQuery("test1 test2").field(TEXT_FIELD_NAME).toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "test1")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "test2")));
    }

    public void testToQueryMultipleFieldsDisableDismax() throws Exception {
        Query query = multiMatchQuery("test").field(TEXT_FIELD_NAME)
            .field(KEYWORD_FIELD_NAME)
            .tieBreaker(1.0f)
            .toQuery(createSearchExecutionContext());
        Query expected = new DisjunctionMaxQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, "test")), new TermQuery(new Term(KEYWORD_FIELD_NAME, "test"))),
            1
        );
        assertEquals(expected, query);
    }

    public void testToQueryMultipleFieldsDisMaxQuery() throws Exception {
        Query query = multiMatchQuery("test").field(TEXT_FIELD_NAME).field(KEYWORD_FIELD_NAME).toQuery(createSearchExecutionContext());
        Query expected = new DisjunctionMaxQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, "test")), new TermQuery(new Term(KEYWORD_FIELD_NAME, "test"))),
            0
        );
        assertEquals(expected, query);
    }

    public void testToQueryFieldMissing() throws Exception {
        assertThat(
            multiMatchQuery("test").field(MISSING_WILDCARD_FIELD_NAME).toQuery(createSearchExecutionContext()),
            instanceOf(MatchNoDocsQuery.class)
        );
        assertThat(
            multiMatchQuery("test").field(MISSING_FIELD_NAME).toQuery(createSearchExecutionContext()),
            instanceOf(MatchNoDocsQuery.class)
        );
    }

    public void testToQueryBooleanPrefixSingleField() throws IOException {
        final MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder("foo bar", TEXT_FIELD_NAME);
        builder.type(Type.BOOL_PREFIX);
        final Query query = builder.toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        final BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses(), hasSize(2));
        assertThat(assertBooleanSubQuery(booleanQuery, TermQuery.class, 0).getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "foo")));
        assertThat(assertBooleanSubQuery(booleanQuery, PrefixQuery.class, 1).getPrefix(), equalTo(new Term(TEXT_FIELD_NAME, "bar")));
    }

    public void testToQueryBooleanPrefixMultipleFields() throws IOException {
        {
            // STRING_FIELD_NAME_2 is a keyword field
            final MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder("foo bar", TEXT_FIELD_NAME, KEYWORD_FIELD_NAME);
            queryBuilder.type(Type.BOOL_PREFIX);
            final Query query = queryBuilder.toQuery(createSearchExecutionContext());
            Query expected = new DisjunctionMaxQuery(
                List.of(
                    new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "foo")), BooleanClause.Occur.SHOULD)
                        .add(new PrefixQuery(new Term(TEXT_FIELD_NAME, "bar")), BooleanClause.Occur.SHOULD)
                        .build(),
                    new PrefixQuery(new Term(KEYWORD_FIELD_NAME, "foo bar"))
                ),
                1
            );
            assertEquals(expected, query);
        }
    }

    public void testSimpleFromJson() throws IOException {
        String json = """
            {
              "multi_match" : {
                "query" : "quick brown fox",
                "fields" : [ "title^1.0", "title.original^1.0", "title.shingles^1.0" ]
              }
            }""";

        MultiMatchQueryBuilder parsed = (MultiMatchQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "quick brown fox", parsed.value());
        assertEquals(json, 3, parsed.fields().size());
        assertEquals(json, Type.BEST_FIELDS, parsed.type());
        assertEquals(json, Operator.OR, parsed.operator());
        assertEquals(json, true, parsed.fuzzyTranspositions());
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "multi_match" : {
                "query" : "quick brown fox",
                "fields" : [ "title^1.0", "title.original^1.0", "title.shingles^1.0" ],
                "type" : "most_fields",
                "operator" : "AND",
                "slop" : 1,
                "prefix_length" : 1,
                "max_expansions" : 40,
                "lenient" : true,
                "zero_terms_query" : "ALL",
                "auto_generate_synonyms_phrase_query" : false,
                "fuzzy_transpositions" : false,
                "boost" : 2.0
              }
            }""";

        MultiMatchQueryBuilder parsed = (MultiMatchQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "quick brown fox", parsed.value());
        assertEquals(json, 3, parsed.fields().size());
        assertEquals(json, MultiMatchQueryBuilder.Type.MOST_FIELDS, parsed.type());
        assertEquals(json, Operator.AND, parsed.operator());
        assertEquals(json, false, parsed.fuzzyTranspositions());
    }

    /**
     * `fuzziness` is not allowed for `cross_fields`, `phrase` and `phrase_prefix` and should throw an error
     */
    public void testFuzzinessNotAllowedTypes() throws IOException {
        String[] notAllowedTypes = new String[] {
            Type.CROSS_FIELDS.parseField().getPreferredName(),
            Type.PHRASE.parseField().getPreferredName(),
            Type.PHRASE_PREFIX.parseField().getPreferredName() };
        for (String type : notAllowedTypes) {
            String json = Strings.format("""
                {
                  "multi_match": {
                    "query": "quick brown fox",
                    "fields": [ "title^1.0", "title.original^1.0", "title.shingles^1.0" ],
                    "type": "%s",
                    "fuzziness": 1
                  }
                }""", type);

            ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
            assertEquals("Fuzziness not allowed for type [" + type + "]", e.getMessage());
        }
    }

    public void testQueryParameterArrayException() {
        String json = """
            {
              "multi_match" : {
                "query" : ["quick", "brown", "fox"]
                "fields" : [ "title^1.0", "title.original^1.0", "title.shingles^1.0" ]  }
            }""";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[multi_match] unknown token [START_ARRAY] after [query]", e.getMessage());
    }

    public void testExceptionUsingAnalyzerOnNumericField() {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(6.075210893508043E-4);
        multiMatchQueryBuilder.field(DOUBLE_FIELD_NAME);
        multiMatchQueryBuilder.analyzer("simple");
        NumberFormatException e = expectThrows(NumberFormatException.class, () -> multiMatchQueryBuilder.toQuery(searchExecutionContext));
        assertEquals("For input string: \"e\"", e.getMessage());
    }

    public void testFuzzinessOnNonStringField() throws Exception {
        MultiMatchQueryBuilder query = new MultiMatchQueryBuilder(42).field(INT_FIELD_NAME).field(BOOLEAN_FIELD_NAME);
        query.fuzziness(randomFuzziness(INT_FIELD_NAME));
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertThat(e.getMessage(), containsString("Can only use fuzzy queries on keyword and text fields"));
        query.analyzer("keyword"); // triggers a different code path
        e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertThat(e.getMessage(), containsString("Can only use fuzzy queries on keyword and text fields"));

        query.lenient(true);
        query.toQuery(context); // no exception
        query.analyzer(null);
        query.toQuery(context); // no exception
    }

    public void testToFuzzyQuery() throws Exception {
        MultiMatchQueryBuilder qb = new MultiMatchQueryBuilder("text").field(TEXT_FIELD_NAME);
        qb.fuzziness(Fuzziness.TWO);
        qb.prefixLength(2);
        qb.maxExpansions(5);
        qb.fuzzyTranspositions(false);

        Query query = qb.toQuery(createSearchExecutionContext());
        FuzzyQuery expected = new FuzzyQuery(new Term(TEXT_FIELD_NAME, "text"), 2, 2, 5, false);

        assertEquals(expected, query);
    }

    public void testWithStopWords() throws Exception {
        Query query = new MultiMatchQueryBuilder("the quick fox").field(TEXT_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createSearchExecutionContext());
        Query expected = new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "quick")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "fox")), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = new MultiMatchQueryBuilder("the quick fox").field(TEXT_FIELD_NAME)
            .field(KEYWORD_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createSearchExecutionContext());
        expected = new DisjunctionMaxQuery(
            Arrays.asList(
                new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "quick")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term(TEXT_FIELD_NAME, "fox")), BooleanClause.Occur.SHOULD)
                    .build(),
                new BooleanQuery.Builder().add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "quick")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "fox")), BooleanClause.Occur.SHOULD)
                    .build()
            ),
            0f
        );
        assertEquals(expected, query);

        query = new MultiMatchQueryBuilder("the").field(TEXT_FIELD_NAME)
            .field(KEYWORD_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createSearchExecutionContext());
        expected = new DisjunctionMaxQuery(Arrays.asList(new MatchNoDocsQuery(), new MatchNoDocsQuery()), 0f);
        assertEquals(expected, query);

        query = new BoolQueryBuilder().should(new MultiMatchQueryBuilder("the").field(TEXT_FIELD_NAME).analyzer("stop"))
            .toQuery(createSearchExecutionContext());
        expected = new BooleanQuery.Builder().add(new MatchNoDocsQuery(), BooleanClause.Occur.SHOULD).build();
        assertEquals(expected, query);

        query = new BoolQueryBuilder().should(
            new MultiMatchQueryBuilder("the").field(TEXT_FIELD_NAME).field(KEYWORD_FIELD_NAME).analyzer("stop")
        ).toQuery(createSearchExecutionContext());
        expected = new BooleanQuery.Builder().add(
            new DisjunctionMaxQuery(Arrays.asList(new MatchNoDocsQuery(), new MatchNoDocsQuery()), 0f),
            BooleanClause.Occur.SHOULD
        ).build();
        assertEquals(expected, query);
    }

    public void testNegativeFieldBoost() {
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> new MultiMatchQueryBuilder("the quick fox").field(TEXT_FIELD_NAME, -1.0f)
                .field(KEYWORD_FIELD_NAME)
                .toQuery(createSearchExecutionContext())
        );
        assertThat(exc.getMessage(), containsString("negative [boost]"));
    }

    /**
     * "now" on date fields should make the query non-cachable.
     */
    public void testCachingStrategiesWithNow() throws IOException {
        // if we hit a date field with "now", this should diable cachability
        MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder("now", DATE_FIELD_NAME);
        SearchExecutionContext context = createSearchExecutionContext();
        assert context.isCacheable();
        /*
         * We use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not. We do it
         * this way in SearchService where we first rewrite the query with a private context, then reset the context and then build the
         * actual lucene query
         */
        QueryBuilder rewritten = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    public void testLenientFlag() throws Exception {
        MultiMatchQueryBuilder query = new MultiMatchQueryBuilder("test", BINARY_FIELD_NAME);
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals("Field [mapped_binary] of type [binary] does not support match queries", e.getMessage());
        query.lenient(true);
        assertThat(query.toQuery(context), Matchers.instanceOf(MatchNoDocsQuery.class));
    }
}
