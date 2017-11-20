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
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.all.AllTermQuery;
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
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class MultiMatchQueryBuilderTests extends AbstractQueryTestCase<MultiMatchQueryBuilder> {

    private static final String MISSING_WILDCARD_FIELD_NAME = "missing_*";
    private static final String MISSING_FIELD_NAME = "missing";

    @Override
    protected MultiMatchQueryBuilder doCreateTestQueryBuilder() {
        String fieldName = randomFrom(STRING_FIELD_NAME, INT_FIELD_NAME, DOUBLE_FIELD_NAME, BOOLEAN_FIELD_NAME, DATE_FIELD_NAME,
                MISSING_FIELD_NAME, MISSING_WILDCARD_FIELD_NAME);
        if (fieldName.equals(DATE_FIELD_NAME)) {
            assumeTrue("test with date fields runs only when at least a type is registered", getCurrentTypes().length > 0);
        }

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
            query.type(randomFrom(MultiMatchQueryBuilder.Type.values()));
        }
        if (randomBoolean()) {
            query.operator(randomFrom(Operator.values()));
        }
        if (randomBoolean() && fieldName.equals(STRING_FIELD_NAME)) {
            query.analyzer(randomAnalyzer());
        }
        if (randomBoolean()) {
            query.slop(randomIntBetween(0, 5));
        }
        if (fieldName.equals(STRING_FIELD_NAME) && randomBoolean() && (query.type() == Type.BEST_FIELDS || query.type() == Type.MOST_FIELDS)) {
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
            query.useDisMax(randomBoolean());
        }
        if (randomBoolean()) {
            query.tieBreaker(randomFloat());
        }
        if (randomBoolean()) {
            query.cutoffFrequency((float) 10 / randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            query.zeroTermsQuery(randomFrom(MatchQuery.ZeroTermsQuery.values()));
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
        assertThat(query, either(instanceOf(BoostQuery.class)).or(instanceOf(TermQuery.class)).or(instanceOf(AllTermQuery.class))
                .or(instanceOf(BooleanQuery.class)).or(instanceOf(DisjunctionMaxQuery.class))
                .or(instanceOf(FuzzyQuery.class)).or(instanceOf(MultiPhrasePrefixQuery.class))
                .or(instanceOf(MatchAllDocsQuery.class)).or(instanceOf(ExtendedCommonTermsQuery.class))
                .or(instanceOf(MatchNoDocsQuery.class)).or(instanceOf(PhraseQuery.class))
                .or(instanceOf(PointRangeQuery.class)).or(instanceOf(IndexOrDocValuesQuery.class))
                .or(instanceOf(BlendedTermQuery.class)));
    }

    public void testIllegaArguments() {
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder(null, "field"));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", (String[]) null));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", new String[]{""}));
        expectThrows(IllegalArgumentException.class, () -> new MultiMatchQueryBuilder("value", "field").type(null));
    }

    public void testToQueryBoost() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
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
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = multiMatchQuery("test1 test2").field(STRING_FIELD_NAME).useDisMax(false).toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test1")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test2")));
    }

    public void testToQueryMultipleFieldsDisableDismax() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = multiMatchQuery("test").field(STRING_FIELD_NAME).field(STRING_FIELD_NAME_2).useDisMax(false).toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery dQuery = (DisjunctionMaxQuery) query;
        assertThat(dQuery.getTieBreakerMultiplier(), equalTo(1.0f));
        assertThat(dQuery.getDisjuncts().size(), equalTo(2));
        assertThat(assertDisjunctionSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(assertDisjunctionSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    public void testToQueryMultipleFieldsDisMaxQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = multiMatchQuery("test").field(STRING_FIELD_NAME).field(STRING_FIELD_NAME_2).useDisMax(true).toQuery(createShardContext());
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
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = multiMatchQuery("test").field("mapped_str*").useDisMax(false).toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery dQuery = (DisjunctionMaxQuery) query;
        assertThat(dQuery.getTieBreakerMultiplier(), equalTo(1.0f));
        assertThat(dQuery.getDisjuncts().size(), equalTo(2));
        assertThat(assertDisjunctionSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
        assertThat(assertDisjunctionSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
    }

    public void testToQueryFieldMissing() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        assertThat(multiMatchQuery("test").field(MISSING_WILDCARD_FIELD_NAME).toQuery(createShardContext()), instanceOf(MatchNoDocsQuery.class));
        assertThat(multiMatchQuery("test").field(MISSING_FIELD_NAME).toQuery(createShardContext()), instanceOf(MatchNoDocsQuery.class));
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
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryShardContext shardContext = createShardContext();
        MultiMatchQueryBuilder multiMatchQueryBuilder = new MultiMatchQueryBuilder(6.075210893508043E-4);
        multiMatchQueryBuilder.field(DOUBLE_FIELD_NAME);
        multiMatchQueryBuilder.analyzer("simple");
        NumberFormatException e = expectThrows(NumberFormatException.class, () -> multiMatchQueryBuilder.toQuery(shardContext));
        assertEquals("For input string: \"e\"", e.getMessage());
    }

    public void testFuzzinessOnNonStringField() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
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
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);

        MultiMatchQueryBuilder qb = new MultiMatchQueryBuilder("text").field(STRING_FIELD_NAME);
        qb.fuzziness(Fuzziness.TWO);
        qb.prefixLength(2);
        qb.maxExpansions(5);
        qb.fuzzyTranspositions(false);

        Query query = qb.toQuery(createShardContext());
        FuzzyQuery expected = new FuzzyQuery(new Term(STRING_FIELD_NAME, "text"), 2, 2, 5, false);

        assertEquals(expected, query);
    }

    public void testDefaultField() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryShardContext context = createShardContext();
        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder("hello");
        // should pass because we set lenient to true when default field is `*`
        Query query = builder.toQuery(context);
        assertThat(query, anyOf(instanceOf(AllTermQuery.class), instanceOf(DisjunctionMaxQuery.class)));

        context.getIndexSettings().updateIndexMetaData(
            newIndexMeta("index", context.getIndexSettings().getSettings(), Settings.builder().putList("index.query.default_field",
                STRING_FIELD_NAME, STRING_FIELD_NAME_2 + "^5").build())
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
            newIndexMeta("index", context.getIndexSettings().getSettings(), Settings.builder().putList("index.query.default_field",
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
                new TermQuery(new Term(STRING_FIELD_NAME, "hello")),
                new BoostQuery(new TermQuery(new Term(STRING_FIELD_NAME_2, "hello")), 5.0f),
                new MatchNoDocsQuery("failed [mapped_int] query, caused by number_format_exception:[For input string: \"hello\"]")
            ), 0.0f
        );
        assertEquals(expected, query);
    }

    private static IndexMetaData newIndexMeta(String name, Settings oldIndexSettings, Settings indexSettings) {
        Settings build = Settings.builder().put(oldIndexSettings)
            .put(indexSettings)
            .build();
        return IndexMetaData.builder(name).settings(build).build();
    }
}
