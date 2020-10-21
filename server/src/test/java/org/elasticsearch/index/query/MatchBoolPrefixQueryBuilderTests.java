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

import org.apache.lucene.analysis.MockSynonymAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.search.MatchQuery;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;

public class MatchBoolPrefixQueryBuilderTests extends AbstractQueryTestCase<MatchBoolPrefixQueryBuilder> {

    @Override
    protected MatchBoolPrefixQueryBuilder doCreateTestQueryBuilder() {
        final String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
        final Object value = IntStream.rangeClosed(0, randomIntBetween(0, 3))
                .mapToObj(i -> randomAlphaOfLengthBetween(1, 10) + " ")
                .collect(Collectors.joining())
                .trim();

        final MatchBoolPrefixQueryBuilder queryBuilder = new MatchBoolPrefixQueryBuilder(fieldName, value);

        if (randomBoolean() && isTextField(fieldName)) {
            queryBuilder.analyzer(randomFrom("simple", "keyword", "whitespace"));
        }

        if (randomBoolean()) {
            queryBuilder.operator(randomFrom(Operator.values()));
        }

        if (randomBoolean()) {
            queryBuilder.minimumShouldMatch(randomMinimumShouldMatch());
        }

        if (randomBoolean()) {
            queryBuilder.fuzziness(randomFuzziness(fieldName));
        }

        if (randomBoolean()) {
            queryBuilder.prefixLength(randomIntBetween(0, 10));
        }

        if (randomBoolean()) {
            queryBuilder.maxExpansions(randomIntBetween(1, 1000));
        }

        if (randomBoolean()) {
            queryBuilder.fuzzyTranspositions(randomBoolean());
        }

        if (randomBoolean()) {
            queryBuilder.fuzzyRewrite(getRandomRewriteMethod());
        }

        return queryBuilder;
    }

    @Override
    protected void doAssertLuceneQuery(MatchBoolPrefixQueryBuilder queryBuilder,
                                       Query query,
                                       QueryShardContext context) throws IOException {
        assertThat(query, notNullValue());
        assertThat(query, anyOf(instanceOf(BooleanQuery.class), instanceOf(PrefixQuery.class)));

        if (query instanceof PrefixQuery) {
            final PrefixQuery prefixQuery = (PrefixQuery) query;
            assertThat(prefixQuery.getPrefix().text(), equalToIgnoringCase((String) queryBuilder.value()));
        } else {
            assertThat(query, instanceOf(BooleanQuery.class));
            final BooleanQuery booleanQuery = (BooleanQuery) query;
            // all queries except the last should be TermQuery or SynonymQuery
            final Set<Query> allQueriesExceptLast = IntStream.range(0, booleanQuery.clauses().size() - 1)
                .mapToObj(booleanQuery.clauses()::get)
                .map(BooleanClause::getQuery)
                .collect(Collectors.toSet());
            assertThat(allQueriesExceptLast, anyOf(
                everyItem(instanceOf(TermQuery.class)),
                everyItem(instanceOf(SynonymQuery.class)),
                everyItem(instanceOf(FuzzyQuery.class))
            ));

            if (allQueriesExceptLast.stream().anyMatch(subQuery -> subQuery instanceof FuzzyQuery)) {
                assertThat(queryBuilder.fuzziness(), notNullValue());
            }
            allQueriesExceptLast.stream().filter(subQuery -> subQuery instanceof FuzzyQuery).forEach(subQuery -> {
                final FuzzyQuery fuzzyQuery = (FuzzyQuery) subQuery;
                assertThat(fuzzyQuery.getPrefixLength(), equalTo(queryBuilder.prefixLength()));
                assertThat(fuzzyQuery.getTranspositions(), equalTo(queryBuilder.fuzzyTranspositions()));
            });

            // the last query should be PrefixQuery
            final Query shouldBePrefixQuery = booleanQuery.clauses().get(booleanQuery.clauses().size() - 1).getQuery();
            assertThat(shouldBePrefixQuery, instanceOf(PrefixQuery.class));

            if (queryBuilder.minimumShouldMatch() != null) {
                final int optionalClauses =
                    (int) booleanQuery.clauses().stream().filter(clause -> clause.getOccur() == BooleanClause.Occur.SHOULD).count();
                final int expected = Queries.calculateMinShouldMatch(optionalClauses, queryBuilder.minimumShouldMatch());
                assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(expected));
            }
        }
    }

    public void testIllegalValues() {
        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchBoolPrefixQueryBuilder(null, "value"));
            assertEquals("[match_bool_prefix] requires fieldName", e.getMessage());
        }

        {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new MatchBoolPrefixQueryBuilder("name", null));
            assertEquals("[match_bool_prefix] requires query value", e.getMessage());
        }

        {
            final MatchBoolPrefixQueryBuilder builder = new MatchBoolPrefixQueryBuilder("name", "value");
            builder.analyzer("bogusAnalyzer");
            QueryShardException e = expectThrows(QueryShardException.class, () -> builder.toQuery(createShardContext()));
            assertThat(e.getMessage(), containsString("analyzer [bogusAnalyzer] not found"));
        }
    }

    public void testFromSimpleJson() throws IOException {
        final String simple =
            "{" +
                "\"match_bool_prefix\": {" +
                    "\"fieldName\": \"fieldValue\"" +
                "}" +
            "}";
        final String expected =
            "{" +
                "\"match_bool_prefix\": {" +
                    "\"fieldName\": {" +
                        "\"query\": \"fieldValue\"," +
                        "\"operator\": \"OR\"," +
                        "\"prefix_length\": 0," +
                        "\"max_expansions\": 50," +
                        "\"fuzzy_transpositions\": true," +
                        "\"boost\": 1.0" +
                    "}" +
                "}" +
            "}";

        final MatchBoolPrefixQueryBuilder builder = (MatchBoolPrefixQueryBuilder) parseQuery(simple);
        checkGeneratedJson(expected, builder);
    }

    public void testFromJson() throws IOException {
        final String expected =
            "{" +
                "\"match_bool_prefix\": {" +
                    "\"fieldName\": {" +
                        "\"query\": \"fieldValue\"," +
                        "\"analyzer\": \"simple\"," +
                        "\"operator\": \"AND\"," +
                        "\"minimum_should_match\": \"2\"," +
                        "\"fuzziness\": \"1\"," +
                        "\"prefix_length\": 1," +
                        "\"max_expansions\": 10," +
                        "\"fuzzy_transpositions\": false," +
                        "\"fuzzy_rewrite\": \"constant_score\"," +
                        "\"boost\": 2.0" +
                    "}" +
                "}" +
            "}";

        final MatchBoolPrefixQueryBuilder builder = (MatchBoolPrefixQueryBuilder) parseQuery(expected);
        checkGeneratedJson(expected, builder);
    }

    public void testParseFailsWithMultipleFields() {
        {
            final String json =
                "{" +
                    "\"match_bool_prefix\" : {" +
                        "\"field_name_1\" : {" +
                            "\"query\" : \"foo\"" +
                        "}," +
                        "\"field_name_2\" : {" +
                            "\"query\" : \"foo\"\n" +
                        "}" +
                    "}" +
                "}";
            final ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
            assertEquals(
                "[match_bool_prefix] query doesn't support multiple fields, found [field_name_1] and [field_name_2]", e.getMessage());
        }

        {
            final String simpleJson =
                "{" +
                    "\"match_bool_prefix\" : {" +
                        "\"field_name_1\" : \"foo\"," +
                        "\"field_name_2\" : \"foo\"" +
                    "}" +
                "}";
            final ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(simpleJson));
            assertEquals(
                "[match_bool_prefix] query doesn't support multiple fields, found [field_name_1] and [field_name_2]", e.getMessage());
        }
    }

    public void testAnalysis() throws Exception {
        final MatchBoolPrefixQueryBuilder builder = new MatchBoolPrefixQueryBuilder(TEXT_FIELD_NAME, "foo bar baz");
        final Query query = builder.toQuery(createShardContext());

        assertBooleanQuery(query, asList(
            new TermQuery(new Term(TEXT_FIELD_NAME, "foo")),
            new TermQuery(new Term(TEXT_FIELD_NAME, "bar")),
            new PrefixQuery(new Term(TEXT_FIELD_NAME, "baz"))
        ));
    }

    public void testAnalysisSynonym() throws Exception {
        final MatchQuery matchQuery = new MatchQuery(createShardContext());
        matchQuery.setAnalyzer(new MockSynonymAnalyzer());
        final Query query = matchQuery.parse(MatchQuery.Type.BOOLEAN_PREFIX, TEXT_FIELD_NAME, "fox dogs red");

        assertBooleanQuery(query, asList(
            new TermQuery(new Term(TEXT_FIELD_NAME, "fox")),
            new SynonymQuery(new Term(TEXT_FIELD_NAME, "dogs"), new Term(TEXT_FIELD_NAME, "dog")),
            new PrefixQuery(new Term(TEXT_FIELD_NAME, "red"))
        ));
    }

    public void testAnalysisSingleTerm() throws Exception {
        final MatchBoolPrefixQueryBuilder builder = new MatchBoolPrefixQueryBuilder(TEXT_FIELD_NAME, "foo");
        final Query query = builder.toQuery(createShardContext());
        assertThat(query, equalTo(new PrefixQuery(new Term(TEXT_FIELD_NAME, "foo"))));
    }

    private static void assertBooleanQuery(Query actual, List<Query> expectedClauseQueries) {
        assertThat(actual, instanceOf(BooleanQuery.class));
        final BooleanQuery actualBooleanQuery = (BooleanQuery) actual;
        assertThat(actualBooleanQuery.clauses(), hasSize(expectedClauseQueries.size()));
        assertThat(actualBooleanQuery.clauses(), everyItem(hasProperty("occur", equalTo(BooleanClause.Occur.SHOULD))));

        for (int i = 0; i < actualBooleanQuery.clauses().size(); i++) {
            final Query clauseQuery = actualBooleanQuery.clauses().get(i).getQuery();
            assertThat(clauseQuery, equalTo(expectedClauseQueries.get(i)));
        }
    }
}
