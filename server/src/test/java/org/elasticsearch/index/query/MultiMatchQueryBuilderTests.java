/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder.Type;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBooleanSubQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertDisjunctionSubQuery;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class MultiMatchQueryBuilderTests extends AbstractQueryTestCase<MultiMatchQueryBuilder> {
    private static final String MISSING_WILDCARD_FIELD_NAME = "missing_*";
    private static final String MISSING_FIELD_NAME = "missing";

    @Override
    protected MultiMatchQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME,
                MISSING_FIELD_NAME, MISSING_WILDCARD_FIELD_NAME);

        final Object value;
        if (fieldName.equals(STRING_FIELD_NAME)) {
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
            if (fieldName.equals(STRING_FIELD_NAME) || fieldName.equals(STRING_ALIAS_FIELD_NAME) || fieldName.equals(STRING_FIELD_NAME_2)) {
                query.type(randomFrom(MultiMatchQueryBuilder.Type.values()));
            } else {
                query.type(randomValueOtherThanMany(
                    (type) -> type == Type.PHRASE_PREFIX || type == Type.BOOL_PREFIX,
                    () -> randomFrom(MultiMatchQueryBuilder.Type.values())));
            }
        }
        if (randomBoolean()) {
            query.operator(randomFrom(Operator.values()));
        }
        if (randomBoolean() && fieldName.equals(STRING_FIELD_NAME)) {
            query.analyzer(randomAnalyzer());
        }
        if (randomBoolean() && query.type() != Type.BOOL_PREFIX) {
            query.slop(randomIntBetween(0, 5));
        }
        if (fieldName.equals(STRING_FIELD_NAME) && randomBoolean() &&
                (query.type() == Type.BEST_FIELDS || query.type() == Type.MOST_FIELDS)) {
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
            query.zeroTermsQuery(randomFrom(MatchQuery.ZeroTermsQuery.NONE, MatchQuery.ZeroTermsQuery.ALL));
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
        String query = "{\n" +
                "    \"multi_match\": {\n" +
                "        \"query\": \"foo bar\",\n" +
                "        \"fields\": \"myField\"\n" +
                "    }\n" +
                "}";
        alternateVersions.put(query, new MultiMatchQueryBuilder("foo bar", "myField"));
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(MultiMatchQueryBuilder queryBuilder, Query query, SearchContext context) throws IOException {
        // we rely on integration tests for deeper checks here
        assertThat(query, anyOf(Arrays.asList(
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
        )));
    }

    public void testIllegaArguments() {
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder(null, "field"));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", (String[]) null));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", new String[]{""}));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", "field").type(null));
    }

    public void testToQueryBoost() throws IOException {
        QueryShardContext shardContext = createShardContext();
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder("test");
        multiMatchQueryBuilder.field(STRING_FIELD_NAME, 5f);
        Query query = multiMatchQueryBuilder.toQuery(shardContext);
        assertTermOrBoostQuery(query, STRING_FIELD_NAME, "test", 5f);

        multiMatchQueryBuilder = new MultiMatchQueryBuilder("test");
        multiMatchQueryBuilder.field(STRING_FIELD_NAME, 5f);
        multiMatchQueryBuilder.boost(2f);
        query = multiMatchQueryBuilder.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2f));
        assertTermOrBoostQuery(boostQuery.getQuery(), STRING_FIELD_NAME, "test", 5f);
    }

    public void testToQueryMultipleTermsBooleanQuery() throws Exception {
        Query query = multiMatchQuery("test1 test2").field(STRING_FIELD_NAME).toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test1")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test2")));
    }

    public void testToQueryMultipleFieldsDisableDismax() throws Exception {
        Query query = multiMatchQuery("test").field(STRING_FIELD_NAME).field(STRING_FIELD_NAME_2).tieBreaker(1.0f)
                .toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery dQuery = (DisjunctionMaxQuery) query;
        assertThat(dQuery.getTieBreakerMultiplier(), equalTo(1.0f));
        assertThat(dQuery.getDisjuncts().size(), equalTo(2));
        assertThat(assertDisjunctionSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(assertDisjunctionSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    public void testToQueryMultipleFieldsDisMaxQuery() throws Exception {
        Query query = multiMatchQuery("test").field(STRING_FIELD_NAME).field(STRING_FIELD_NAME_2).toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) query;
        assertThat(disMaxQuery.getTieBreakerMultiplier(), equalTo(0.0f));
        List<Query> disjuncts = disMaxQuery.getDisjuncts();
        assertThat(disjuncts.get(0), instanceOf(TermQuery.class));
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(disjuncts.get(1), instanceOf(TermQuery.class));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    public void testToQueryFieldsWildcard() throws Exception {
        Query query = multiMatchQuery("test").field("mapped_str*").tieBreaker(1.0f).toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery dQuery = (DisjunctionMaxQuery) query;
        assertThat(dQuery.getTieBreakerMultiplier(), equalTo(1.0f));
        assertThat(dQuery.getDisjuncts().size(), equalTo(2));
        assertThat(assertDisjunctionSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(assertDisjunctionSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    public void testToQueryFieldMissing() throws Exception {
        assertThat(multiMatchQuery("test").field(MISSING_WILDCARD_FIELD_NAME).toQuery(createShardContext()),
            instanceOf(MatchNoDocsQuery.class));
        assertThat(multiMatchQuery("test").field(MISSING_FIELD_NAME).toQuery(createShardContext()),
            instanceOf(MatchNoDocsQuery.class));
    }

    public void testToQueryBooleanPrefixSingleField() throws IOException {
        final MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder("foo bar", STRING_FIELD_NAME);
        builder.type(Type.BOOL_PREFIX);
        final Query query = builder.toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        final BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses(), hasSize(2));
        assertThat(assertBooleanSubQuery(booleanQuery, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "foo")));
        assertThat(assertBooleanSubQuery(booleanQuery, PrefixQuery.class, 1).getPrefix(), equalTo(new Term(STRING_FIELD_NAME, "bar")));
    }

    public void testToQueryBooleanPrefixMultipleFields() throws IOException {
        {
            // STRING_FIELD_NAME_2 is a keyword field
            final MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder("foo bar", STRING_FIELD_NAME, STRING_FIELD_NAME_2);
            queryBuilder.type(Type.BOOL_PREFIX);
            final Query query = queryBuilder.toQuery(createShardContext());
            assertThat(query, instanceOf(DisjunctionMaxQuery.class));
            final DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) query;
            assertThat(disMaxQuery.getDisjuncts(), hasSize(2));
            final BooleanQuery firstDisjunct = assertDisjunctionSubQuery(disMaxQuery, BooleanQuery.class, 0);
            assertThat(firstDisjunct.clauses(), hasSize(2));
            assertThat(assertBooleanSubQuery(firstDisjunct, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "foo")));
            final PrefixQuery secondDisjunct = assertDisjunctionSubQuery(disMaxQuery, PrefixQuery.class, 1);
            assertThat(secondDisjunct.getPrefix(), equalTo(new Term(STRING_FIELD_NAME_2, "foo bar")));
        }
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"multi_match\" : {\n" +
                "    \"query\" : \"quick brown fox\",\n" +
                "    \"fields\" : [ \"title^1.0\", \"title.original^1.0\", \"title.shingles^1.0\" ],\n" +
                "    \"type\" : \"most_fields\",\n" +
                "    \"operator\" : \"OR\",\n" +
                "    \"slop\" : 0,\n" +
                "    \"prefix_length\" : 0,\n" +
                "    \"max_expansions\" : 50,\n" +
                "    \"lenient\" : false,\n" +
                "    \"zero_terms_query\" : \"NONE\",\n" +
                "    \"auto_generate_synonyms_phrase_query\" : true,\n" +
                "    \"fuzzy_transpositions\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        MultiMatchQueryBuilder parsed = (MultiMatchQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "quick brown fox", parsed.value());
        assertEquals(json, 3, parsed.fields().size());
        assertEquals(json, MultiMatchQueryBuilder.Type.MOST_FIELDS, parsed.type());
        assertEquals(json, Operator.OR, parsed.operator());
        assertEquals(json, false, parsed.fuzzyTranspositions());
    }

    /**
     * `fuzziness` is not allowed for `cross_fields`, `phrase` and `phrase_prefix` and should throw an error
     */
    public void testFuzzinessNotAllowedTypes() throws IOException {
        String[] notAllowedTypes = new String[]{ Type.CROSS_FIELDS.parseField().getPreferredName(),
            Type.PHRASE.parseField().getPreferredName(), Type.PHRASE_PREFIX.parseField().getPreferredName()};
        for (String type : notAllowedTypes) {
            String json =
                    "{\n" +
                    "  \"multi_match\" : {\n" +
                    "    \"query\" : \"quick brown fox\",\n" +
                    "    \"fields\" : [ \"title^1.0\", \"title.original^1.0\", \"title.shingles^1.0\" ],\n" +
                    "    \"type\" : \"" + type + "\",\n" +
                    "    \"fuzziness\" : 1" +
                    "  }\n" +
                    "}";

            ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
            assertEquals("Fuzziness not allowed for type [" + type +"]", e.getMessage());
        }
    }

    public void testQueryParameterArrayException() {
        String json =
                "{\n" +
                "  \"multi_match\" : {\n" +
                "    \"query\" : [\"quick\", \"brown\", \"fox\"]\n" +
                "    \"fields\" : [ \"title^1.0\", \"title.original^1.0\", \"title.shingles^1.0\" ]" +
                "  }\n" +
                "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[multi_match] unknown token [START_ARRAY] after [query]", e.getMessage());
    }

    public void testExceptionUsingAnalyzerOnNumericField() {
        QueryShardContext shardContext = createShardContext();
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(6.075210893508043E-4);
        multiMatchQueryBuilder.field(DOUBLE_FIELD_NAME);
        multiMatchQueryBuilder.analyzer("simple");
        NumberFormatException e = expectThrows(NumberFormatException.class, () -> multiMatchQueryBuilder.toQuery(shardContext));
        assertEquals("For input string: \"e\"", e.getMessage());
    }

    public void testFuzzinessOnNonStringField() throws Exception {
        MultiMatchQueryBuilder query = new MultiMatchQueryBuilder(42).field(INT_FIELD_NAME).field(BOOLEAN_FIELD_NAME);
        query.fuzziness(randomFuzziness(INT_FIELD_NAME));
        QueryShardContext context = createShardContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> query.toQuery(context));
        assertThat(e.getMessage(), containsString("Can only use fuzzy queries on keyword and text fields"));
        query.analyzer("keyword"); // triggers a different code path
        e = expectThrows(IllegalArgumentException.class,
                () -> query.toQuery(context));
        assertThat(e.getMessage(), containsString("Can only use fuzzy queries on keyword and text fields"));

        query.lenient(true);
        query.toQuery(context); // no exception
        query.analyzer(null);
        query.toQuery(context); // no exception
    }

    public void testToFuzzyQuery() throws Exception {
        MultiMatchQueryBuilder qb = new MultiMatchQueryBuilder("text").field(STRING_FIELD_NAME);
        qb.fuzziness(Fuzziness.TWO);
        qb.prefixLength(2);
        qb.maxExpansions(5);
        qb.fuzzyTranspositions(false);

        Query query = qb.toQuery(createShardContext());
        FuzzyQuery expected = new FuzzyQuery(new Term(STRING_FIELD_NAME, "text"), 2, 2,
            5, false);

        assertEquals(expected, query);
    }

    public void testDefaultField() throws Exception {
        QueryShardContext context = createShardContext();
        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder("hello");
        // default value `*` sets leniency to true
        Query query = builder.toQuery(context);
        assertQueryWithAllFieldsWildcard(query);

        try {
            // `*` is in the list of the default_field => leniency set to true
            context.getIndexSettings().updateIndexMetaData(
                newIndexMeta("index", context.getIndexSettings().getSettings(), Settings.builder().putList("index.query.default_field",
                    STRING_FIELD_NAME, "*", STRING_FIELD_NAME_2).build())
            );
            query = new MultiMatchQueryBuilder("hello")
                .toQuery(context);
            assertQueryWithAllFieldsWildcard(query);

            context.getIndexSettings().updateIndexMetaData(
                newIndexMeta("index", context.getIndexSettings().getSettings(),
                    Settings.builder().putList("index.query.default_field", STRING_FIELD_NAME, STRING_FIELD_NAME_2 + "^5")
                        .build())
            );
            MultiMatchQueryBuilder qb = new MultiMatchQueryBuilder("hello");
            query = qb.toQuery(context);
            DisjunctionMaxQuery expected = new DisjunctionMaxQuery(
                Arrays.asList(
                    new TermQuery(new Term(STRING_FIELD_NAME, "hello")),
                    new BoostQuery(new TermQuery(new Term(STRING_FIELD_NAME_2, "hello")), 5.0f)
                ), 0.0f
            );
            assertEquals(expected, query);

            context.getIndexSettings().updateIndexMetaData(
                newIndexMeta("index", context.getIndexSettings().getSettings(),
                    Settings.builder().putList("index.query.default_field",
                        STRING_FIELD_NAME, STRING_FIELD_NAME_2 + "^5", INT_FIELD_NAME).build())
            );
            // should fail because lenient defaults to false
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> qb.toQuery(context));
            assertThat(exc, instanceOf(NumberFormatException.class));
            assertThat(exc.getMessage(), equalTo("For input string: \"hello\""));

            // explicitly sets lenient
            qb.lenient(true);
            query = qb.toQuery(context);
            expected = new DisjunctionMaxQuery(
                Arrays.asList(
                    new MatchNoDocsQuery("failed [mapped_int] query, caused by number_format_exception:[For input string: \"hello\"]"),
                    new TermQuery(new Term(STRING_FIELD_NAME, "hello")),
                    new BoostQuery(new TermQuery(new Term(STRING_FIELD_NAME_2, "hello")), 5.0f)
                ), 0.0f
            );
            assertEquals(expected, query);

        } finally {
            // Reset to the default value
            context.getIndexSettings().updateIndexMetaData(
                newIndexMeta("index", context.getIndexSettings().getSettings(),
                    Settings.builder().putNull("index.query.default_field").build())
            );
        }
    }

    public void testAllFieldsWildcard() throws Exception {
        QueryShardContext context = createShardContext();
        Query query = new MultiMatchQueryBuilder("hello")
            .field("*")
            .toQuery(context);
        assertQueryWithAllFieldsWildcard(query);

        query = new MultiMatchQueryBuilder("hello")
            .field(STRING_FIELD_NAME)
            .field("*")
            .field(STRING_FIELD_NAME_2)
            .toQuery(context);
        assertQueryWithAllFieldsWildcard(query);
    }

    public void testWithStopWords() throws Exception {
        Query query = new MultiMatchQueryBuilder("the quick fox")
            .field(STRING_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createShardContext());
        Query expected = new BooleanQuery.Builder()
            .add(new TermQuery(new Term(STRING_FIELD_NAME, "quick")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(STRING_FIELD_NAME, "fox")), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = new MultiMatchQueryBuilder("the quick fox")
            .field(STRING_FIELD_NAME)
            .field(STRING_FIELD_NAME_2)
            .analyzer("stop")
            .toQuery(createShardContext());
        expected = new DisjunctionMaxQuery(
            Arrays.asList(
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term(STRING_FIELD_NAME, "quick")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term(STRING_FIELD_NAME, "fox")), BooleanClause.Occur.SHOULD)
                    .build(),
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term(STRING_FIELD_NAME_2, "quick")), BooleanClause.Occur.SHOULD)
                    .add(new TermQuery(new Term(STRING_FIELD_NAME_2, "fox")), BooleanClause.Occur.SHOULD)
                    .build()
            ), 0f);
        assertEquals(expected, query);

        query = new MultiMatchQueryBuilder("the")
            .field(STRING_FIELD_NAME)
            .field(STRING_FIELD_NAME_2)
            .analyzer("stop")
            .toQuery(createShardContext());
        expected = new DisjunctionMaxQuery(Arrays.asList(new MatchNoDocsQuery(), new MatchNoDocsQuery()), 0f);
        assertEquals(expected, query);

        query = new BoolQueryBuilder()
            .should(
                new MultiMatchQueryBuilder("the")
                    .field(STRING_FIELD_NAME)
                    .analyzer("stop")
            )
            .toQuery(createShardContext());
        expected = new BooleanQuery.Builder()
            .add(new MatchNoDocsQuery(), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = new BoolQueryBuilder()
            .should(
                new MultiMatchQueryBuilder("the")
                    .field(STRING_FIELD_NAME)
                    .field(STRING_FIELD_NAME_2)
                    .analyzer("stop")
            )
            .toQuery(createShardContext());
        expected = new BooleanQuery.Builder()
            .add(new DisjunctionMaxQuery(Arrays.asList(new MatchNoDocsQuery(), new MatchNoDocsQuery()),
                0f), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);
    }

    public void testNegativeFieldBoost() {
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> new MultiMatchQueryBuilder("the quick fox")
                .field(STRING_FIELD_NAME, -1.0f)
                .field(STRING_FIELD_NAME_2)
                .toQuery(createShardContext()));
        assertThat(exc.getMessage(), containsString("negative [boost]"));
    }

    private static IndexMetaData newIndexMeta(String name, Settings oldIndexSettings, Settings indexSettings) {
        Settings build = Settings.builder().put(oldIndexSettings)
            .put(indexSettings)
            .build();
        return IndexMetaData.builder(name).settings(build).build();
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
        assertThat(disjunctionMaxQuery.getDisjuncts(), hasItems(new TermQuery(new Term(STRING_FIELD_NAME, "hello")),
            new TermQuery(new Term(STRING_FIELD_NAME_2, "hello"))));
    }

    /**
     * "now" on date fields should make the query non-cachable.
     */
    public void testCachingStrategiesWithNow() throws IOException {
        // if we hit a date field with "now", this should diable cachability
        MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder("now", DATE_FIELD_NAME);
        QueryShardContext context = createShardContext();
        assert context.isCacheable();
        /*
         * We use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not. We do it
         * this way in SearchService where we first rewrite the query with a private context, then reset the context and then build the
         * actual lucene query
         */
        QueryBuilder rewritten = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }
}
