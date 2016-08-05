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
import org.apache.lucene.queryparser.classic.MapperQueryParser;
import org.apache.lucene.queryparser.classic.QueryParserSettings;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.elasticsearch.common.lucene.all.AllTermQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matchers;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBooleanSubQuery;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class QueryStringQueryBuilderTests extends AbstractQueryTestCase<QueryStringQueryBuilder> {

    @Override
    protected QueryStringQueryBuilder doCreateTestQueryBuilder() {
        int numTerms = randomIntBetween(0, 5);
        String query = "";
        for (int i = 0; i < numTerms; i++) {
            //min length 4 makes sure that the text is not an operator (AND/OR) so toQuery won't break
            query += (randomBoolean() ? STRING_FIELD_NAME + ":" : "") + randomAsciiOfLengthBetween(4, 10) + " ";
        }
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder(query);
        if (randomBoolean()) {
            queryStringQueryBuilder.defaultField(randomBoolean() ?
                STRING_FIELD_NAME : randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            int numFields = randomIntBetween(1, 5);
            for (int i = 0; i < numFields; i++) {
                String fieldName = randomBoolean() ? STRING_FIELD_NAME : randomAsciiOfLengthBetween(1, 10);
                if (randomBoolean()) {
                    queryStringQueryBuilder.field(fieldName);
                } else {
                    queryStringQueryBuilder.field(fieldName, randomFloat());
                }
            }
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.defaultOperator(randomFrom(Operator.values()));
        }
        if (randomBoolean()) {
            //we only use string fields (either mapped or unmapped)
            queryStringQueryBuilder.fuzziness(randomFuzziness(STRING_FIELD_NAME));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.analyzer(randomAnalyzer());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.quoteAnalyzer(randomAnalyzer());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.allowLeadingWildcard(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.analyzeWildcard(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.maxDeterminizedStates(randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.lowercaseExpandedTerms(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.autoGeneratePhraseQueries(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.enablePositionIncrements(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.lenient(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.escape(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.phraseSlop(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.fuzzyMaxExpansions(randomIntBetween(0, 100));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.fuzzyPrefixLength(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.fuzzyRewrite(getRandomRewriteMethod());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.rewrite(getRandomRewriteMethod());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.quoteFieldSuffix(randomAsciiOfLengthBetween(1, 3));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.tieBreaker(randomFloat());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.minimumShouldMatch(randomMinimumShouldMatch());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.useDisMax(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.locale(randomLocale(random()));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.timeZone(randomDateTimeZone().getID());
        }
        return queryStringQueryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(QueryStringQueryBuilder queryBuilder,
                                       Query query, QueryShardContext context) throws IOException {
        if ("".equals(queryBuilder.queryString())) {
            assertThat(query, instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, either(instanceOf(TermQuery.class)).or(instanceOf(AllTermQuery.class))
                    .or(instanceOf(BooleanQuery.class)).or(instanceOf(DisjunctionMaxQuery.class)));
        }
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new QueryStringQueryBuilder((String) null));
    }

    public void testToQueryMatchAllQuery() throws Exception {
        Query query = queryStringQuery("*:*").toQuery(createShardContext());
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }

    public void testToQueryTermQuery() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = queryStringQuery("test").defaultField(STRING_FIELD_NAME).toQuery(createShardContext());
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertThat(termQuery.getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
    }

    public void testToQueryPhraseQuery() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = queryStringQuery("\"term1 term2\"")
            .defaultField(STRING_FIELD_NAME)
            .phraseSlop(3)
            .toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) query;
        assertThat(disjunctionMaxQuery.getDisjuncts().size(), equalTo(1));
        assertThat(disjunctionMaxQuery.getDisjuncts().get(0), instanceOf(PhraseQuery.class));
        PhraseQuery phraseQuery = (PhraseQuery)disjunctionMaxQuery.getDisjuncts().get(0);
        assertThat(phraseQuery.getTerms().length, equalTo(2));
        assertThat(phraseQuery.getTerms()[0], equalTo(new Term(STRING_FIELD_NAME, "term1")));
        assertThat(phraseQuery.getTerms()[1], equalTo(new Term(STRING_FIELD_NAME, "term2")));
        assertThat(phraseQuery.getSlop(), equalTo(3));
    }

    public void testToQueryBoosts() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryShardContext shardContext = createShardContext();
        QueryStringQueryBuilder queryStringQuery = queryStringQuery(STRING_FIELD_NAME + ":boosted^2");
        Query query = queryStringQuery.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) boostQuery.getQuery()).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "boosted")));
        queryStringQuery.boost(2.0f);
        query = queryStringQuery.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery   .getQuery(), instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) boostQuery.getQuery();
        assertThat(boostQuery.getBoost(), equalTo(2.0f));

        queryStringQuery =
            queryStringQuery("((" + STRING_FIELD_NAME + ":boosted^2) AND (" + STRING_FIELD_NAME + ":foo^1.5))^3");
        query = queryStringQuery.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(3.0f));
        BoostQuery boostQuery1 = assertBooleanSubQuery(boostQuery.getQuery(), BoostQuery.class, 0);
        assertThat(boostQuery1.getBoost(), equalTo(2.0f));
        assertThat(boostQuery1.getQuery(), instanceOf(TermQuery.class));
        assertThat(((TermQuery)boostQuery1.getQuery()).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "boosted")));
        BoostQuery boostQuery2 = assertBooleanSubQuery(boostQuery.getQuery(), BoostQuery.class, 1);
        assertThat(boostQuery2.getBoost(), equalTo(1.5f));
        assertThat(boostQuery2.getQuery(), instanceOf(TermQuery.class));
        assertThat(((TermQuery)boostQuery2.getQuery()).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "foo")));
        queryStringQuery.boost(2.0f);
        query = queryStringQuery.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
    }

    public void testToQueryMultipleTermsBooleanQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = queryStringQuery("test1 test2").field(STRING_FIELD_NAME)
            .useDisMax(false)
            .toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(),
            equalTo(new Term(STRING_FIELD_NAME, "test1")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(),
            equalTo(new Term(STRING_FIELD_NAME, "test2")));
    }

    public void testToQueryMultipleFieldsBooleanQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = queryStringQuery("test").field(STRING_FIELD_NAME)
            .field(STRING_FIELD_NAME_2)
            .useDisMax(false)
            .toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(),
            equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(),
            equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    public void testToQueryMultipleFieldsDisMaxQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = queryStringQuery("test").field(STRING_FIELD_NAME).field(STRING_FIELD_NAME_2)
            .useDisMax(true)
            .toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) query;
        List<Query> disjuncts = disMaxQuery.getDisjuncts();
        assertThat(((TermQuery) disjuncts.get(0)).getTerm(), equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(((TermQuery) disjuncts.get(1)).getTerm(), equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    public void testToQueryFieldsWildcard() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = queryStringQuery("test").field("mapped_str*").useDisMax(false).toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(),
            equalTo(new Term(STRING_FIELD_NAME, "test")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(),
            equalTo(new Term(STRING_FIELD_NAME_2, "test")));
    }

    public void testToQueryDisMaxQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = queryStringQuery("test").field(STRING_FIELD_NAME, 2.2f)
            .field(STRING_FIELD_NAME_2)
            .useDisMax(true)
            .toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disMaxQuery = (DisjunctionMaxQuery) query;
        List<Query> disjuncts = disMaxQuery.getDisjuncts();
        assertTermOrBoostQuery(disjuncts.get(0), STRING_FIELD_NAME, "test", 2.2f);
        assertTermOrBoostQuery(disjuncts.get(1), STRING_FIELD_NAME_2, "test", 1.0f);
    }

    public void testToQueryWildcarQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            MapperQueryParser queryParser = new MapperQueryParser(createShardContext());
            QueryParserSettings settings = new QueryParserSettings("first foo-bar-foobar* last");
            settings.defaultField(STRING_FIELD_NAME);
            settings.fieldsAndWeights(Collections.emptyMap());
            settings.analyzeWildcard(true);
            settings.fuzziness(Fuzziness.AUTO);
            settings.rewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
            settings.defaultOperator(op.toQueryParserOperator());
            queryParser.reset(settings);
            Query query = queryParser.parse("first foo-bar-foobar* last");
            Query expectedQuery =
                new BooleanQuery.Builder()
                    .add(new BooleanClause(new TermQuery(new Term(STRING_FIELD_NAME, "first")), defaultOp))
                    .add(new BooleanQuery.Builder()
                        .add(new BooleanClause(new TermQuery(new Term(STRING_FIELD_NAME, "foo")), defaultOp))
                        .add(new BooleanClause(new TermQuery(new Term(STRING_FIELD_NAME, "bar")), defaultOp))
                        .add(new BooleanClause(new PrefixQuery(new Term(STRING_FIELD_NAME, "foobar")), defaultOp))
                        .build(), defaultOp)
                    .add(new BooleanClause(new TermQuery(new Term(STRING_FIELD_NAME, "last")), defaultOp))
                    .build();
            assertThat(query, Matchers.equalTo(expectedQuery));
        }
    }

    public void testToQueryWilcardQueryWithSynonyms() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            MapperQueryParser queryParser = new MapperQueryParser(createShardContext());
            QueryParserSettings settings = new QueryParserSettings("first foo-bar-foobar* last");
            settings.defaultField(STRING_FIELD_NAME);
            settings.fieldsAndWeights(Collections.emptyMap());
            settings.analyzeWildcard(true);
            settings.fuzziness(Fuzziness.AUTO);
            settings.rewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
            settings.defaultOperator(op.toQueryParserOperator());
            settings.forceAnalyzer(new MockRepeatAnalyzer());
            queryParser.reset(settings);
            Query query = queryParser.parse("first foo-bar-foobar* last");

            Query expectedQuery = new BooleanQuery.Builder()
                .add(new BooleanClause(new SynonymQuery(new Term(STRING_FIELD_NAME, "first"),
                    new Term(STRING_FIELD_NAME, "first")), defaultOp))
                .add(new BooleanQuery.Builder()
                    .add(new BooleanClause(new SynonymQuery(new Term(STRING_FIELD_NAME, "foo"),
                        new Term(STRING_FIELD_NAME, "foo")), defaultOp))
                    .add(new BooleanClause(new SynonymQuery(new Term(STRING_FIELD_NAME, "bar"),
                        new Term(STRING_FIELD_NAME, "bar")), defaultOp))
                    .add(new BooleanQuery.Builder()
                        .add(new BooleanClause(new PrefixQuery(new Term(STRING_FIELD_NAME, "foobar")),
                            BooleanClause.Occur.SHOULD))
                        .add(new BooleanClause(new PrefixQuery(new Term(STRING_FIELD_NAME, "foobar")),
                            BooleanClause.Occur.SHOULD))
                        .setDisableCoord(true)
                        .build(), defaultOp)
                    .build(), defaultOp)
                .add(new BooleanClause(new SynonymQuery(new Term(STRING_FIELD_NAME, "last"),
                    new Term(STRING_FIELD_NAME, "last")), defaultOp))
                .build();
            assertThat(query, Matchers.equalTo(expectedQuery));
        }
    }

    public void testToQueryRegExpQuery() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        Query query = queryStringQuery("/foo*bar/").defaultField(STRING_FIELD_NAME)
            .maxDeterminizedStates(5000)
            .toQuery(createShardContext());
        assertThat(query, instanceOf(RegexpQuery.class));
        RegexpQuery regexpQuery = (RegexpQuery) query;
        assertTrue(regexpQuery.toString().contains("/foo*bar/"));
    }

    public void testToQueryRegExpQueryTooComplex() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryStringQueryBuilder queryBuilder = queryStringQuery("/[ac]*a[ac]{50,200}/").defaultField(STRING_FIELD_NAME);

        TooComplexToDeterminizeException e = expectThrows(TooComplexToDeterminizeException.class,
                () -> queryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("Determinizing [ac]*"));
        assertThat(e.getMessage(), containsString("would result in more than 10000 states"));
    }

    public void testFuzzyNumeric() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryStringQueryBuilder query = queryStringQuery("12~0.2").defaultField(INT_FIELD_NAME);
        QueryShardContext context = createShardContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> query.toQuery(context));
        assertEquals("Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
                e.getMessage());
        query.lenient(true);
        query.toQuery(context); // no exception
    }

    public void testPrefixNumeric() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryStringQueryBuilder query = queryStringQuery("12*").defaultField(INT_FIELD_NAME);
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class,
                () -> query.toQuery(context));
        assertEquals("Can only use prefix queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
                e.getMessage());
        query.lenient(true);
        query.toQuery(context); // no exception
    }

    public void testExactGeo() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryStringQueryBuilder query = queryStringQuery("2,3").defaultField(GEO_POINT_FIELD_NAME);
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class,
                () -> query.toQuery(context));
        assertEquals("Geo fields do not support exact searching, use dedicated geo queries instead: [mapped_geo_point]",
                e.getMessage());
        query.lenient(true);
        query.toQuery(context); // no exception
    }

    public void testTimezone() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        String queryAsString = "{\n" +
                "    \"query_string\":{\n" +
                "        \"time_zone\":\"Europe/Paris\",\n" +
                "        \"query\":\"" + DATE_FIELD_NAME + ":[2012 TO 2014]\"\n" +
                "    }\n" +
                "}";
        QueryBuilder queryBuilder = parseQuery(queryAsString);
        assertThat(queryBuilder, instanceOf(QueryStringQueryBuilder.class));
        QueryStringQueryBuilder queryStringQueryBuilder = (QueryStringQueryBuilder) queryBuilder;
        assertThat(queryStringQueryBuilder.timeZone(), equalTo(DateTimeZone.forID("Europe/Paris")));

        String invalidQueryAsString = "{\n" +
                "    \"query_string\":{\n" +
                "        \"time_zone\":\"This timezone does not exist\",\n" +
                "        \"query\":\"" + DATE_FIELD_NAME + ":[2012 TO 2014]\"\n" +
                "    }\n" +
                "}";
        expectThrows(IllegalArgumentException.class, () -> parseQuery(invalidQueryAsString));
    }

    public void testToQueryBooleanQueryMultipleBoosts() throws Exception {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        int numBoosts = randomIntBetween(2, 10);
        float[] boosts = new float[numBoosts + 1];
        String queryStringPrefix = "";
        String queryStringSuffix = "";
        for (int i = 0; i < boosts.length - 1; i++) {
            float boost = 2.0f / randomIntBetween(3, 20);
            boosts[i] = boost;
            queryStringPrefix += "(";
            queryStringSuffix += ")^" + boost;
        }
        String queryString = queryStringPrefix + "foo bar" + queryStringSuffix;

        float mainBoost = 2.0f / randomIntBetween(3, 20);
        boosts[boosts.length - 1] = mainBoost;
        QueryStringQueryBuilder queryStringQueryBuilder =
            new QueryStringQueryBuilder(queryString).field(STRING_FIELD_NAME)
                .minimumShouldMatch("2").boost(mainBoost);
        Query query = queryStringQueryBuilder.toQuery(createShardContext());

        for (int i = boosts.length - 1; i >= 0; i--) {
            assertThat(query, instanceOf(BoostQuery.class));
            BoostQuery boostQuery = (BoostQuery) query;
            assertThat(boostQuery.getBoost(), equalTo(boosts[i]));
            query = boostQuery.getQuery();
        }

        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(2));
        assertThat(booleanQuery.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(booleanQuery.clauses().get(0).getQuery(),
            equalTo(new TermQuery(new Term(STRING_FIELD_NAME, "foo"))));
        assertThat(booleanQuery.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(booleanQuery.clauses().get(1).getQuery(),
            equalTo(new TermQuery(new Term(STRING_FIELD_NAME, "bar"))));
    }

    public void testToQueryPhraseQueryBoostAndSlop() throws IOException {
        assumeTrue("test runs only when at least a type is registered", getCurrentTypes().length > 0);
        QueryStringQueryBuilder queryStringQueryBuilder =
            new QueryStringQueryBuilder("\"test phrase\"~2").field(STRING_FIELD_NAME, 5f);
        Query query = queryStringQueryBuilder.toQuery(createShardContext());
        assertThat(query, instanceOf(DisjunctionMaxQuery.class));
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) query;
        assertThat(disjunctionMaxQuery.getDisjuncts().size(), equalTo(1));
        assertThat(disjunctionMaxQuery.getDisjuncts().get(0), instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) disjunctionMaxQuery.getDisjuncts().get(0);
        assertThat(boostQuery.getBoost(), equalTo(5f));
        assertThat(boostQuery.getQuery(), instanceOf(PhraseQuery.class));
        PhraseQuery phraseQuery = (PhraseQuery) boostQuery.getQuery();
        assertThat(phraseQuery.getSlop(), Matchers.equalTo(2));
        assertThat(phraseQuery.getTerms().length, equalTo(2));
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"query_string\" : {\n" +
                "    \"query\" : \"this AND that OR thus\",\n" +
                "    \"default_field\" : \"content\",\n" +
                "    \"fields\" : [ ],\n" +
                "    \"use_dis_max\" : true,\n" +
                "    \"tie_breaker\" : 0.0,\n" +
                "    \"default_operator\" : \"or\",\n" +
                "    \"auto_generate_phrase_queries\" : false,\n" +
                "    \"max_determined_states\" : 10000,\n" +
                "    \"lowercase_expanded_terms\" : true,\n" +
                "    \"enable_position_increment\" : true,\n" +
                "    \"fuzziness\" : \"AUTO\",\n" +
                "    \"fuzzy_prefix_length\" : 0,\n" +
                "    \"fuzzy_max_expansions\" : 50,\n" +
                "    \"phrase_slop\" : 0,\n" +
                "    \"locale\" : \"und\",\n" +
                "    \"escape\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";

        QueryStringQueryBuilder parsed = (QueryStringQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "this AND that OR thus", parsed.queryString());
        assertEquals(json, "content", parsed.defaultField());
    }

}
