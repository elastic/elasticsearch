/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.search.SimpleQueryStringQueryParser;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SimpleQueryStringBuilderTests extends AbstractQueryTestCase<SimpleQueryStringBuilder> {

    @Override
    protected SimpleQueryStringBuilder doCreateTestQueryBuilder() {
        // we avoid strings with "now" since those can have different caching policies that are checked elsewhere
        String queryText = randomValueOtherThanMany(s -> s.toLowerCase(Locale.ROOT).contains("now"),
                () -> randomAlphaOfLengthBetween(1, 10));
        SimpleQueryStringBuilder result = new SimpleQueryStringBuilder(queryText);
        if (randomBoolean()) {
            result.analyzeWildcard(randomBoolean());
        }
        if (randomBoolean()) {
            result.minimumShouldMatch(randomMinimumShouldMatch());
        }
        if (randomBoolean()) {
            result.analyzer(randomAnalyzer());
        }
        if (randomBoolean()) {
            result.defaultOperator(randomFrom(Operator.values()));
        }
        if (randomBoolean()) {
            result.quoteFieldSuffix(TestUtil.randomSimpleString(random()));
        }
        if (randomBoolean()) {
            Set<SimpleQueryStringFlag> flagSet = new HashSet<>();
            int size = randomIntBetween(0, SimpleQueryStringFlag.values().length);
            for (int i = 0; i < size; i++) {
                flagSet.add(randomFrom(SimpleQueryStringFlag.values()));
            }
            if (flagSet.size() > 0) {
                result.flags(flagSet.toArray(new SimpleQueryStringFlag[flagSet.size()]));
            }
        }

        int fieldCount = randomIntBetween(0, 2);
        Map<String, Float> fields = new HashMap<>();
        for (int i = 0; i < fieldCount; i++) {
            if (i == 0) {
                String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
                fields.put(fieldName, AbstractQueryBuilder.DEFAULT_BOOST);
            } else {
                fields.put(KEYWORD_FIELD_NAME, 2.0f / randomIntBetween(1, 20));
            }
        }

        result.fields(fields);
        if (randomBoolean()) {
            result.autoGenerateSynonymsPhraseQuery(randomBoolean());
        }
        if (randomBoolean()) {
            result.fuzzyPrefixLength(randomIntBetween(0, 5));
        }
        if (randomBoolean()) {
            result.fuzzyMaxExpansions(randomIntBetween(1, 5));
        }
        if (randomBoolean()) {
            result.fuzzyTranspositions(randomBoolean());
        }
        return result;
    }

    public void testDefaults() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");

        assertEquals("Wrong default default boost.", AbstractQueryBuilder.DEFAULT_BOOST, qb.boost(), 0.001);
        assertEquals("Wrong default default boost field.", AbstractQueryBuilder.DEFAULT_BOOST, SimpleQueryStringBuilder.DEFAULT_BOOST,
                0.001);

        assertEquals("Wrong default flags.", SimpleQueryStringFlag.ALL.value, qb.flags());
        assertEquals("Wrong default flags field.", SimpleQueryStringFlag.ALL.value(), SimpleQueryStringBuilder.DEFAULT_FLAGS);

        assertEquals("Wrong default default operator.", Operator.OR, qb.defaultOperator());
        assertEquals("Wrong default default operator field.", Operator.OR, SimpleQueryStringBuilder.DEFAULT_OPERATOR);

        assertEquals("Wrong default default analyze_wildcard.", false, qb.analyzeWildcard());
        assertEquals("Wrong default default analyze_wildcard field.", false, SimpleQueryStringBuilder.DEFAULT_ANALYZE_WILDCARD);

        assertEquals("Wrong default default lenient.", false, qb.lenient());
        assertEquals("Wrong default default lenient field.", false, SimpleQueryStringBuilder.DEFAULT_LENIENT);

        assertEquals("Wrong default default fuzzy prefix length.", FuzzyQuery.defaultPrefixLength, qb.fuzzyPrefixLength());
        assertEquals("Wrong default default fuzzy prefix length field.",
            FuzzyQuery.defaultPrefixLength, SimpleQueryStringBuilder.DEFAULT_FUZZY_PREFIX_LENGTH);

        assertEquals("Wrong default default fuzzy max expansions.", FuzzyQuery.defaultMaxExpansions, qb.fuzzyMaxExpansions());
        assertEquals("Wrong default default fuzzy max expansions field.",
            FuzzyQuery.defaultMaxExpansions, SimpleQueryStringBuilder.DEFAULT_FUZZY_MAX_EXPANSIONS);

        assertEquals("Wrong default default fuzzy transpositions.", FuzzyQuery.defaultTranspositions, qb.fuzzyTranspositions());
        assertEquals("Wrong default default fuzzy transpositions field.",
            FuzzyQuery.defaultTranspositions, SimpleQueryStringBuilder.DEFAULT_FUZZY_TRANSPOSITIONS);
    }

    public void testDefaultNullComplainFlags() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.flags((SimpleQueryStringFlag[]) null);
        assertEquals("Setting flags to null should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_FLAGS,
                qb.flags());
    }

    public void testDefaultEmptyComplainFlags() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.flags(new SimpleQueryStringFlag[]{});
        assertEquals("Setting flags to empty should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_FLAGS,
                qb.flags());
    }

    public void testDefaultNullComplainOp() {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.");
        qb.defaultOperator(null);
        assertEquals("Setting operator to null should result in returning to default value.", SimpleQueryStringBuilder.DEFAULT_OPERATOR,
                qb.defaultOperator());
    }

    // Check operator handling, and default field handling.
    public void testDefaultOperatorHandling() throws IOException {
        SimpleQueryStringBuilder qb = new SimpleQueryStringBuilder("The quick brown fox.").field(TEXT_FIELD_NAME);
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        searchExecutionContext.setAllowUnmappedFields(true); // to avoid occasional cases
                                                   // in setup where we didn't
                                                   // add types but strict field
                                                   // resolution
        BooleanQuery boolQuery = (BooleanQuery) qb.toQuery(searchExecutionContext);
        assertThat(shouldClauses(boolQuery), is(4));

        qb.defaultOperator(Operator.AND);
        boolQuery = (BooleanQuery) qb.toQuery(searchExecutionContext);
        assertThat(shouldClauses(boolQuery), is(0));

        qb.defaultOperator(Operator.OR);
        boolQuery = (BooleanQuery) qb.toQuery(searchExecutionContext);
        assertThat(shouldClauses(boolQuery), is(4));
    }

    public void testIllegalConstructorArg() {
        expectThrows(IllegalArgumentException.class, () -> new SimpleQueryStringBuilder((String) null));
    }

    public void testFieldCannotBeNull() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qb.field(null));
        assertEquals("supplied field is null or empty", e.getMessage());
    }

    public void testFieldCannotBeNullAndWeighted() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qb.field(null, AbstractQueryBuilder.DEFAULT_BOOST));
        assertEquals("supplied field is null or empty", e.getMessage());
    }

    public void testFieldCannotBeEmpty() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> qb.field(""));
        assertEquals("supplied field is null or empty", e.getMessage());
    }

    public void testFieldCannotBeEmptyAndWeighted() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        IllegalArgumentException  e = expectThrows(IllegalArgumentException.class, () -> qb.field("", AbstractQueryBuilder.DEFAULT_BOOST));
        assertEquals("supplied field is null or empty", e.getMessage());
    }

    /**
     * The following should fail fast - never silently set the map containing
     * fields and weights to null but refuse to accept null instead.
     * */
    public void testFieldsCannotBeSetToNull() {
        SimpleQueryStringBuilder qb = createTestQueryBuilder();
        NullPointerException e = expectThrows(NullPointerException.class, () -> qb.fields(null));
        assertEquals("fields cannot be null", e.getMessage());
    }

    public void testDefaultFieldParsing() throws IOException {
        String query = randomAlphaOfLengthBetween(1, 10).toLowerCase(Locale.ROOT);
        String contentString = "{\n" +
                "    \"simple_query_string\" : {\n" +
                "      \"query\" : \"" + query + "\"" +
                "    }\n" +
                "}";
        SimpleQueryStringBuilder queryBuilder = (SimpleQueryStringBuilder) parseQuery(contentString);
        assertThat(queryBuilder.value(), equalTo(query));
        assertThat(queryBuilder.fields(), notNullValue());
        assertThat(queryBuilder.fields().size(), equalTo(0));
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();

        Query luceneQuery = queryBuilder.toQuery(searchExecutionContext);
        assertThat(luceneQuery, anyOf(instanceOf(BooleanQuery.class), instanceOf(DisjunctionMaxQuery.class)));
    }

    /*
     * This assumes that Lucene query parsing is being checked already, adding
     * checks only for our parsing extensions.
     *
     * Also this relies on {@link SimpleQueryStringTests} to test most of the
     * actual functionality of query parsing.
     */
    @Override
    protected void doAssertLuceneQuery(SimpleQueryStringBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, notNullValue());

        if (queryBuilder.value().isEmpty()) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else if (queryBuilder.fields().size() > 1) {
            assertThat(query, instanceOf(DisjunctionMaxQuery.class));
            DisjunctionMaxQuery maxQuery = (DisjunctionMaxQuery) query;
            for (Query disjunct : maxQuery.getDisjuncts()) {
                assertThat(disjunct, either(instanceOf(TermQuery.class))
                    .or(instanceOf(BoostQuery.class))
                    .or(instanceOf(MatchNoDocsQuery.class)));
                Query termQuery = disjunct;
                if (disjunct instanceof BoostQuery) {
                    termQuery = ((BoostQuery) disjunct).getQuery();
                }
                if (termQuery instanceof TermQuery) {
                    TermQuery inner = (TermQuery) termQuery;
                    assertThat(inner.getTerm().bytes().toString(), is(inner.getTerm().bytes().toString().toLowerCase(Locale.ROOT)));
                } else {
                    assertThat(termQuery, instanceOf(MatchNoDocsQuery.class));
                }
            }
        } else if (queryBuilder.fields().size() == 1) {
            Map.Entry<String, Float> field = queryBuilder.fields().entrySet().iterator().next();
            if (query instanceof MatchNoDocsQuery == false) {
                assertTermOrBoostQuery(query, field.getKey(), queryBuilder.value(), field.getValue());
            }
        } else if (queryBuilder.fields().size() == 0) {
            assertThat(query, either(instanceOf(DisjunctionMaxQuery.class))
                .or(instanceOf(MatchNoDocsQuery.class)).or(instanceOf(TermQuery.class)));
            if (query instanceof DisjunctionMaxQuery) {
                for (Query disjunct : (DisjunctionMaxQuery) query) {
                    assertThat(disjunct, either(instanceOf(TermQuery.class)).or(instanceOf(MatchNoDocsQuery.class)));
                }
            }
        } else {
            fail("Encountered lucene query type we do not have a validation implementation for in our "
                    + SimpleQueryStringBuilderTests.class.getSimpleName());
        }
    }

    private static int shouldClauses(BooleanQuery query) {
        int result = 0;
        for (BooleanClause c : query.clauses()) {
            if (c.getOccur() == BooleanClause.Occur.SHOULD) {
                result++;
            }
        }
        return result;
    }

    public void testToQueryBoost() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        SimpleQueryStringBuilder simpleQueryStringBuilder = new SimpleQueryStringBuilder("test");
        simpleQueryStringBuilder.field(TEXT_FIELD_NAME, 5);
        Query query = simpleQueryStringBuilder.toQuery(searchExecutionContext);
        assertThat(query, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(5f));
        assertThat(boostQuery.getQuery(), instanceOf(TermQuery.class));

        simpleQueryStringBuilder = new SimpleQueryStringBuilder("test");
        simpleQueryStringBuilder.field(TEXT_FIELD_NAME, 5);
        simpleQueryStringBuilder.boost(2);
        query = simpleQueryStringBuilder.toQuery(searchExecutionContext);
        boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2f));
        assertThat(boostQuery.getQuery(), instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) boostQuery.getQuery();
        assertThat(boostQuery.getBoost(), equalTo(5f));
        assertThat(boostQuery.getQuery(), instanceOf(TermQuery.class));
    }

    public void testNegativeFlags() throws IOException {
        String query = "{\"simple_query_string\": {\"query\": \"foo bar\", \"flags\": -1}}";
        SimpleQueryStringBuilder builder = new SimpleQueryStringBuilder("foo bar");
        builder.flags(SimpleQueryStringFlag.ALL);
        assertParsedQuery(query, builder);
        SimpleQueryStringBuilder otherBuilder = new SimpleQueryStringBuilder("foo bar");
        otherBuilder.flags(-1);
        assertThat(builder, equalTo(otherBuilder));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"simple_query_string\" : {\n" +
                "    \"query\" : \"\\\"fried eggs\\\" +(eggplant | potato) -frittata\",\n" +
                "    \"fields\" : [ \"body^5.0\" ],\n" +
                "    \"analyzer\" : \"snowball\",\n" +
                "    \"flags\" : -1,\n" +
                "    \"default_operator\" : \"and\",\n" +
                "    \"lenient\" : false,\n" +
                "    \"analyze_wildcard\" : false,\n" +
                "    \"quote_field_suffix\" : \".quote\",\n" +
                "    \"auto_generate_synonyms_phrase_query\" : true,\n" +
                "    \"fuzzy_prefix_length\" : 1,\n" +
                "    \"fuzzy_max_expansions\" : 5,\n" +
                "    \"fuzzy_transpositions\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        SimpleQueryStringBuilder parsed = (SimpleQueryStringBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "\"fried eggs\" +(eggplant | potato) -frittata", parsed.value());
        assertEquals(json, 1, parsed.fields().size());
        assertEquals(json, "snowball", parsed.analyzer());
        assertEquals(json, ".quote", parsed.quoteFieldSuffix());
        assertEquals(json, 1, parsed.fuzzyPrefixLength());
        assertEquals(json, 5, parsed.fuzzyMaxExpansions());
        assertEquals(json, false, parsed.fuzzyTranspositions());
    }

    public void testMinimumShouldMatch() throws IOException {
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext();
        int numberOfTerms = randomIntBetween(1, 4);
        StringBuilder queryString = new StringBuilder();
        for (int i = 0; i < numberOfTerms; i++) {
            queryString.append("t" + i + " ");
        }
        SimpleQueryStringBuilder simpleQueryStringBuilder = new SimpleQueryStringBuilder(queryString.toString().trim());
        if (randomBoolean()) {
            simpleQueryStringBuilder.defaultOperator(Operator.AND);
        }
        int numberOfFields = randomIntBetween(1, 4);
        for (int i = 0; i < numberOfFields; i++) {
            simpleQueryStringBuilder.field(TEXT_FIELD_NAME);
        }
        int percent = randomIntBetween(1, 100);
        simpleQueryStringBuilder.minimumShouldMatch(percent + "%");
        Query query = simpleQueryStringBuilder.toQuery(searchExecutionContext);

        // check special case: one term & one field should get simplified to a TermQuery
        if (numberOfFields * numberOfTerms == 1) {
            assertThat(query, instanceOf(TermQuery.class));
        } else if (numberOfTerms == 1) {
            assertThat(query, either(instanceOf(DisjunctionMaxQuery.class)).or(instanceOf(TermQuery.class)));
        } else {
            assertThat(query, instanceOf(BooleanQuery.class));
            BooleanQuery boolQuery = (BooleanQuery) query;
            int expectedMinimumShouldMatch = numberOfTerms * percent / 100;
            if (simpleQueryStringBuilder.defaultOperator().equals(Operator.AND)) {
                expectedMinimumShouldMatch = 0;
            }
            assertEquals(expectedMinimumShouldMatch, boolQuery.getMinimumNumberShouldMatch());
        }
    }

    public void testExpandedTerms() throws Exception {
        // Prefix
        Query query = new SimpleQueryStringBuilder("aBc*")
                .field(TEXT_FIELD_NAME)
                .analyzer("whitespace")
                .toQuery(createSearchExecutionContext());
        assertEquals(new PrefixQuery(new Term(TEXT_FIELD_NAME, "aBc")), query);
        query = new SimpleQueryStringBuilder("aBc*")
                .field(TEXT_FIELD_NAME)
                .analyzer("standard")
                .toQuery(createSearchExecutionContext());
        assertEquals(new PrefixQuery(new Term(TEXT_FIELD_NAME, "abc")), query);

        // Fuzzy
        query = new SimpleQueryStringBuilder("aBc~1")
                .field(TEXT_FIELD_NAME)
                .analyzer("whitespace")
                .toQuery(createSearchExecutionContext());
        FuzzyQuery expected = new FuzzyQuery(new Term(TEXT_FIELD_NAME, "aBc"), 1);
        assertEquals(expected, query);
        query = new SimpleQueryStringBuilder("aBc~1")
                .field(TEXT_FIELD_NAME)
                .analyzer("standard")
                .toQuery(createSearchExecutionContext());
        expected = new FuzzyQuery(new Term(TEXT_FIELD_NAME, "abc"), 1);
        assertEquals(expected, query);
    }

    public void testAnalyzeWildcard() throws IOException {
        SimpleQueryStringQueryParser.Settings settings = new SimpleQueryStringQueryParser.Settings();
        settings.analyzeWildcard(true);
        SimpleQueryStringQueryParser parser = new SimpleQueryStringQueryParser(new StandardAnalyzer(),
            Collections.singletonMap(TEXT_FIELD_NAME, 1.0f), -1, settings, createSearchExecutionContext());
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            parser.setDefaultOperator(defaultOp);
            Query query = parser.parse("first foo-bar-foobar* last");
            Query expectedQuery =
                new BooleanQuery.Builder()
                    .add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "first")), defaultOp))
                    .add(new BooleanQuery.Builder()
                        .add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "foo")), defaultOp))
                        .add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), defaultOp))
                        .add(new BooleanClause(new PrefixQuery(new Term(TEXT_FIELD_NAME, "foobar")), defaultOp))
                        .build(), defaultOp)
                    .add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "last")), defaultOp))
                    .build();
            assertThat(query, equalTo(expectedQuery));
        }
    }

    public void testAnalyzerWildcardWithSynonyms() throws IOException {
        SimpleQueryStringQueryParser.Settings settings = new SimpleQueryStringQueryParser.Settings();
        settings.analyzeWildcard(true);
        SimpleQueryStringQueryParser parser = new SimpleQueryStringQueryParser(new MockRepeatAnalyzer(),
            Collections.singletonMap(TEXT_FIELD_NAME, 1.0f), -1, settings, createSearchExecutionContext());
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            parser.setDefaultOperator(defaultOp);
            Query query = parser.parse("first foo-bar-foobar* last");
            Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanClause(new SynonymQuery(new Term(TEXT_FIELD_NAME, "first"),
                    new Term(TEXT_FIELD_NAME, "first")), defaultOp))
                .add(new BooleanQuery.Builder()
                    .add(new BooleanClause(new SynonymQuery(new Term(TEXT_FIELD_NAME, "foo"),
                        new Term(TEXT_FIELD_NAME, "foo")), defaultOp))
                    .add(new BooleanClause(new SynonymQuery(new Term(TEXT_FIELD_NAME, "bar"),
                        new Term(TEXT_FIELD_NAME, "bar")), defaultOp))
                    .add(new BooleanQuery.Builder()
                        .add(new BooleanClause(new PrefixQuery(new Term(TEXT_FIELD_NAME, "foobar")),
                            BooleanClause.Occur.SHOULD))
                        .add(new BooleanClause(new PrefixQuery(new Term(TEXT_FIELD_NAME, "foobar")),
                            BooleanClause.Occur.SHOULD))
                        .build(), defaultOp)
                    .build(), defaultOp)
                .add(new BooleanClause(new SynonymQuery(new Term(TEXT_FIELD_NAME, "last"),
                    new Term(TEXT_FIELD_NAME, "last")), defaultOp))
                .build();
            assertThat(query, equalTo(expectedQuery));
        }
    }

    public void testAnalyzerWithGraph() {
        SimpleQueryStringQueryParser.Settings settings = new SimpleQueryStringQueryParser.Settings();
        settings.analyzeWildcard(true);
        SimpleQueryStringQueryParser parser = new SimpleQueryStringQueryParser(new MockSynonymAnalyzer(),
            Collections.singletonMap(TEXT_FIELD_NAME, 1.0f), -1, settings, createSearchExecutionContext());
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            parser.setDefaultOperator(defaultOp);
            // non-phrase won't detect multi-word synonym because of whitespace splitting
            Query query = parser.parse("guinea pig");

            Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "guinea")), defaultOp))
                .add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "pig")), defaultOp))
                .build();
            assertThat(query, equalTo(expectedQuery));

            // phrase will pick it up
            query = parser.parse("\"guinea pig\"");
            SpanTermQuery span1 = new SpanTermQuery(new Term(TEXT_FIELD_NAME, "guinea"));
            SpanTermQuery span2 = new SpanTermQuery(new Term(TEXT_FIELD_NAME, "pig"));
            expectedQuery = new SpanOrQuery(
                new SpanNearQuery(new SpanQuery[] { span1, span2 }, 0, true),
                new SpanTermQuery(new Term(TEXT_FIELD_NAME, "cavy")));

            assertThat(query, equalTo(expectedQuery));

            // phrase with slop
            query = parser.parse("big \"tiny guinea pig\"~2");
            PhraseQuery pq1 = new PhraseQuery.Builder()
                .add(new Term(TEXT_FIELD_NAME, "tiny"))
                .add(new Term(TEXT_FIELD_NAME, "guinea"))
                .add(new Term(TEXT_FIELD_NAME, "pig"))
                .setSlop(2)
                .build();
            PhraseQuery pq2 = new PhraseQuery.Builder()
                .add(new Term(TEXT_FIELD_NAME, "tiny"))
                .add(new Term(TEXT_FIELD_NAME, "cavy"))
                .setSlop(2)
                .build();

            expectedQuery = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "big")), defaultOp)
                .add(new BooleanQuery.Builder()
                        .add(pq1, BooleanClause.Occur.SHOULD)
                        .add(pq2, BooleanClause.Occur.SHOULD)
                        .build(),
                    defaultOp)
                .build();
            assertThat(query, equalTo(expectedQuery));
        }
    }

    public void testQuoteFieldSuffix() {
        SimpleQueryStringQueryParser.Settings settings = new SimpleQueryStringQueryParser.Settings();
        settings.analyzeWildcard(true);
        settings.quoteFieldSuffix("_2");
        SimpleQueryStringQueryParser parser = new SimpleQueryStringQueryParser(new MockSynonymAnalyzer(),
            Collections.singletonMap(TEXT_FIELD_NAME, 1.0f), -1, settings, createSearchExecutionContext());
        assertEquals(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), parser.parse("bar"));
        assertEquals(new TermQuery(new Term(KEYWORD_FIELD_NAME, "bar")), parser.parse("\"bar\""));

        // Now check what happens if the quote field does not exist
        settings.quoteFieldSuffix(".quote");
        parser = new SimpleQueryStringQueryParser(new MockSynonymAnalyzer(),
            Collections.singletonMap(TEXT_FIELD_NAME, 1.0f), -1, settings, createSearchExecutionContext());
        assertEquals(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), parser.parse("bar"));
        assertEquals(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), parser.parse("\"bar\""));
    }

    public void testDefaultField() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext();
        // default value `*` sets leniency to true
        Query query = new SimpleQueryStringBuilder("hello")
            .toQuery(context);
        assertQueryWithAllFieldsWildcard(query);

        try {
            // `*` is in the list of the default_field => leniency set to true
            context.getIndexSettings().updateIndexMetadata(
                newIndexMeta("index", context.getIndexSettings().getSettings(), Settings.builder().putList("index.query.default_field",
                    TEXT_FIELD_NAME, "*", KEYWORD_FIELD_NAME).build())
            );
            query = new SimpleQueryStringBuilder("hello")
                .toQuery(context);
            assertQueryWithAllFieldsWildcard(query);

            context.getIndexSettings().updateIndexMetadata(
                newIndexMeta("index", context.getIndexSettings().getSettings(), Settings.builder().putList("index.query.default_field",
                    TEXT_FIELD_NAME, KEYWORD_FIELD_NAME + "^5").build())
            );
            query = new SimpleQueryStringBuilder("hello")
                .toQuery(context);
            Query expected = new DisjunctionMaxQuery(
                Arrays.asList(
                    new TermQuery(new Term(TEXT_FIELD_NAME, "hello")),
                    new BoostQuery(new TermQuery(new Term(KEYWORD_FIELD_NAME, "hello")), 5.0f)
                ), 1.0f
            );
            assertEquals(expected, query);
        } finally {
            // Reset to the default value
            context.getIndexSettings().updateIndexMetadata(
                newIndexMeta("index",
                    context.getIndexSettings().getSettings(), Settings.builder().putList("index.query.default_field", "*").build())
            );
        }
    }

    public void testAllFieldsWildcard() throws Exception {
        SearchExecutionContext context = createSearchExecutionContext();
        Query query = new SimpleQueryStringBuilder("hello")
            .field("*")
            .toQuery(context);
        assertQueryWithAllFieldsWildcard(query);

        query = new SimpleQueryStringBuilder("hello")
            .field(TEXT_FIELD_NAME)
            .field("*")
            .field(KEYWORD_FIELD_NAME)
            .toQuery(context);
        assertQueryWithAllFieldsWildcard(query);
    }

    public void testToFuzzyQuery() throws Exception {
        Query query = new SimpleQueryStringBuilder("text~2")
            .field(TEXT_FIELD_NAME)
            .fuzzyPrefixLength(2)
            .fuzzyMaxExpansions(5)
            .fuzzyTranspositions(false)
            .toQuery(createSearchExecutionContext());
        FuzzyQuery expected = new FuzzyQuery(new Term(TEXT_FIELD_NAME, "text"), 2, 2, 5, false);
        assertEquals(expected, query);
    }

    public void testLenientToPrefixQuery() throws Exception {
        Query query = new SimpleQueryStringBuilder("t*")
            .field(DATE_FIELD_NAME)
            .field(TEXT_FIELD_NAME)
            .lenient(true)
            .toQuery(createSearchExecutionContext());
        List<Query> expectedQueries = new ArrayList<>();
        expectedQueries.add(new MatchNoDocsQuery(""));
        expectedQueries.add(new PrefixQuery(new Term(TEXT_FIELD_NAME, "t")));
        DisjunctionMaxQuery expected = new DisjunctionMaxQuery(expectedQueries, 1.0f);
        assertEquals(expected, query);
    }

    public void testWithStopWords() throws Exception {
        Query query = new SimpleQueryStringBuilder("the quick fox")
            .field(TEXT_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createSearchExecutionContext());
        Query expected = new BooleanQuery.Builder()
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "quick")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "fox")), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = new SimpleQueryStringBuilder("the quick fox")
            .field(TEXT_FIELD_NAME)
            .field(KEYWORD_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createSearchExecutionContext());
        expected = new BooleanQuery.Builder()
            .add(new DisjunctionMaxQuery(
                Arrays.asList(
                    new TermQuery(new Term(TEXT_FIELD_NAME, "quick")),
                    new TermQuery(new Term(KEYWORD_FIELD_NAME, "quick"))
                ), 1.0f), BooleanClause.Occur.SHOULD)
            .add(new DisjunctionMaxQuery(
                Arrays.asList(
                    new TermQuery(new Term(TEXT_FIELD_NAME, "fox")),
                    new TermQuery(new Term(KEYWORD_FIELD_NAME, "fox"))
                ), 1.0f), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = new SimpleQueryStringBuilder("the")
            .field(TEXT_FIELD_NAME)
            .field(KEYWORD_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createSearchExecutionContext());
        assertEquals(new MatchNoDocsQuery(), query);

        query = new BoolQueryBuilder()
            .should(
                new SimpleQueryStringBuilder("the")
                    .field(TEXT_FIELD_NAME)
                    .analyzer("stop")
            )
            .toQuery(createSearchExecutionContext());
        expected = new BooleanQuery.Builder()
            .add(new MatchNoDocsQuery(), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = new BoolQueryBuilder()
            .should(
                new SimpleQueryStringBuilder("the")
                    .field(TEXT_FIELD_NAME)
                    .field(KEYWORD_FIELD_NAME)
                    .analyzer("stop")
            )
            .toQuery(createSearchExecutionContext());
        assertEquals(expected, query);
    }

    public void testWithPrefixStopWords() throws Exception {
        Query query = new SimpleQueryStringBuilder("the* quick fox")
            .field(TEXT_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createSearchExecutionContext());
        BooleanQuery expected = new BooleanQuery.Builder()
            .add(new PrefixQuery(new Term(TEXT_FIELD_NAME, "the")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "quick")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "fox")), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);
    }

    /**
     * Test for behavior reported in https://github.com/elastic/elasticsearch/issues/34708
     * Unmapped field can lead to MatchNoDocsQuerys in disjunction queries. If tokens are eliminated (e.g. because
     * the tokenizer removed them as punctuation) on regular fields, this can leave only MatchNoDocsQuerys in the
     * disjunction clause. Instead those disjunctions should be eliminated completely.
     */
    public void testUnmappedFieldNoTokenWithAndOperator() throws IOException {
        Query query = new SimpleQueryStringBuilder("first & second")
                .field(TEXT_FIELD_NAME)
                .field("unmapped")
                .field("another_unmapped")
                .defaultOperator(Operator.AND)
                .toQuery(createSearchExecutionContext());
        BooleanQuery expected = new BooleanQuery.Builder()
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "first")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "second")), BooleanClause.Occur.MUST)
                .build();
        assertEquals(expected, query);
        query = new SimpleQueryStringBuilder("first & second")
            .field("unmapped")
            .field("another_unmapped")
            .defaultOperator(Operator.AND)
            .toQuery(createSearchExecutionContext());
        expected = new BooleanQuery.Builder()
            .add(new MatchNoDocsQuery(), BooleanClause.Occur.MUST)
            .add(new MatchNoDocsQuery(), BooleanClause.Occur.MUST)
            .add(new MatchNoDocsQuery(), BooleanClause.Occur.MUST)
            .build();
        assertEquals(expected, query);
    }

    public void testNegativeFieldBoost() {
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> new SimpleQueryStringBuilder("the quick fox")
                .field(TEXT_FIELD_NAME, -1.0f)
                .field(KEYWORD_FIELD_NAME)
                .toQuery(createSearchExecutionContext()));
        assertThat(exc.getMessage(), containsString("negative [boost]"));
    }

    private static IndexMetadata newIndexMeta(String name, Settings oldIndexSettings, Settings indexSettings) {
        Settings build = Settings.builder().put(oldIndexSettings)
            .put(indexSettings)
            .build();
        return IndexMetadata.builder(name).settings(build).build();
    }

    private void assertQueryWithAllFieldsWildcard(Query query) {
        assertEquals(DisjunctionMaxQuery.class, query.getClass());
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) query;
        int noMatchNoDocsQueries = 0;
        for (Query q : disjunctionMaxQuery.getDisjuncts()) {
            if (q.getClass() == MatchNoDocsQuery.class) {
                noMatchNoDocsQueries++;
            }
        }
        assertEquals(9, noMatchNoDocsQueries);
        assertThat(disjunctionMaxQuery.getDisjuncts(), hasItems(new TermQuery(new Term(TEXT_FIELD_NAME, "hello")),
            new TermQuery(new Term(KEYWORD_FIELD_NAME, "hello"))));
    }

    /**
     * Query terms that contain "now" can trigger a query to not be cacheable.
     * This test checks the search context cacheable flag is updated accordingly.
     */
    public void testCachingStrategiesWithNow() throws IOException {
        // if we hit all fields, this should contain a date field and should diable cachability
        String query = "now " + randomAlphaOfLengthBetween(4, 10);
        SimpleQueryStringBuilder queryBuilder = new SimpleQueryStringBuilder(query);
        assertQueryCachability(queryBuilder, false);

        // if we hit a date field with "now", this should diable cachability
        queryBuilder = new SimpleQueryStringBuilder("now");
        queryBuilder.field(DATE_FIELD_NAME);
        assertQueryCachability(queryBuilder, false);

        // everything else is fine on all fields
        query = randomFrom("NoW", "nOw", "NOW") + " " + randomAlphaOfLengthBetween(4, 10);
        queryBuilder = new SimpleQueryStringBuilder(query);
        assertQueryCachability(queryBuilder, true);
    }

    private void assertQueryCachability(SimpleQueryStringBuilder qb, boolean cachingExpected) throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        assert context.isCacheable();
        /*
         * We use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not. We do it
         * this way in SearchService where we first rewrite the query with a private context, then reset the context and then build the
         * actual lucene query
         */
        QueryBuilder rewritten = rewriteQuery(qb, new SearchExecutionContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertEquals("query should " + (cachingExpected ? "" : "not") + " be cacheable: " + qb.toString(), cachingExpected,
                context.isCacheable());
    }

    public void testLenientFlag() throws Exception {
        SimpleQueryStringBuilder query = new SimpleQueryStringBuilder("test").field(BINARY_FIELD_NAME);
        SearchExecutionContext context = createSearchExecutionContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals("Field [mapped_binary] of type [binary] does not support match queries", e.getMessage());
        query.lenient(true);
        assertThat(query.toQuery(context), instanceOf(MatchNoDocsQuery.class));
    }
}
