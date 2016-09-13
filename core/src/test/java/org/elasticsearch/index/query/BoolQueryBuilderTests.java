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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;

public class BoolQueryBuilderTests extends AbstractQueryTestCase<BoolQueryBuilder> {
    @Override
    protected BoolQueryBuilder doCreateTestQueryBuilder() {
        BoolQueryBuilder query = new BoolQueryBuilder();
        if (randomBoolean()) {
            query.adjustPureNegative(randomBoolean());
        }
        if (randomBoolean()) {
            query.disableCoord(randomBoolean());
        }
        if (randomBoolean()) {
            query.minimumNumberShouldMatch(randomMinimumShouldMatch());
        }
        int mustClauses = randomIntBetween(0, 3);
        for (int i = 0; i < mustClauses; i++) {
            query.must(RandomQueryBuilder.createQuery(random()));
        }
        int mustNotClauses = randomIntBetween(0, 3);
        for (int i = 0; i < mustNotClauses; i++) {
            query.mustNot(RandomQueryBuilder.createQuery(random()));
        }
        int shouldClauses = randomIntBetween(0, 3);
        for (int i = 0; i < shouldClauses; i++) {
            query.should(RandomQueryBuilder.createQuery(random()));
        }
        int filterClauses = randomIntBetween(0, 3);
        for (int i = 0; i < filterClauses; i++) {
            query.filter(RandomQueryBuilder.createQuery(random()));
        }
        return query;
    }

    @Override
    protected void doAssertLuceneQuery(BoolQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        if (!queryBuilder.hasClauses()) {
            assertThat(query, instanceOf(MatchAllDocsQuery.class));
        } else {
            List<BooleanClause> clauses = new ArrayList<>();
            clauses.addAll(getBooleanClauses(queryBuilder.must(), BooleanClause.Occur.MUST, context));
            clauses.addAll(getBooleanClauses(queryBuilder.mustNot(), BooleanClause.Occur.MUST_NOT, context));
            clauses.addAll(getBooleanClauses(queryBuilder.should(), BooleanClause.Occur.SHOULD, context));
            clauses.addAll(getBooleanClauses(queryBuilder.filter(), BooleanClause.Occur.FILTER, context));

            if (clauses.isEmpty()) {
                assertThat(query, instanceOf(MatchAllDocsQuery.class));
            } else {
                assertThat(query, instanceOf(BooleanQuery.class));
                BooleanQuery booleanQuery = (BooleanQuery) query;
                assertThat(booleanQuery.isCoordDisabled(), equalTo(queryBuilder.disableCoord()));
                if (queryBuilder.adjustPureNegative()) {
                    boolean isNegative = true;
                    for (BooleanClause clause : clauses) {
                        if (clause.isProhibited() == false) {
                            isNegative = false;
                            break;
                        }
                    }
                    if (isNegative) {
                        clauses.add(new BooleanClause(new MatchAllDocsQuery(), BooleanClause.Occur.MUST));
                    }
                }
                assertThat(booleanQuery.clauses().size(), equalTo(clauses.size()));
                Iterator<BooleanClause> clauseIterator = clauses.iterator();
                for (BooleanClause booleanClause : booleanQuery.clauses()) {
                    assertThat(booleanClause, instanceOf(clauseIterator.next().getClass()));
                }
            }
        }
    }

    private static List<BooleanClause> getBooleanClauses(List<QueryBuilder> queryBuilders, BooleanClause.Occur occur, QueryShardContext context) throws IOException {
        List<BooleanClause> clauses = new ArrayList<>();
        for (QueryBuilder query : queryBuilders) {
            Query innerQuery = query.toQuery(context);
            if (innerQuery != null) {
                clauses.add(new BooleanClause(innerQuery, occur));
            }
        }
        return clauses;
    }

    @Override
    protected Map<String, BoolQueryBuilder> getAlternateVersions() {
        Map<String, BoolQueryBuilder> alternateVersions = new HashMap<>();
        BoolQueryBuilder tempQueryBuilder = createTestQueryBuilder();
        BoolQueryBuilder expectedQuery = new BoolQueryBuilder();
        String contentString = "{\n" +
                "    \"bool\" : {\n";
        if (tempQueryBuilder.must().size() > 0) {
            QueryBuilder must = tempQueryBuilder.must().get(0);
            contentString += "\"must\": " + must.toString() + ",";
            expectedQuery.must(must);
        }
        if (tempQueryBuilder.mustNot().size() > 0) {
            QueryBuilder mustNot = tempQueryBuilder.mustNot().get(0);
            contentString += (randomBoolean() ? "\"must_not\": " : "\"mustNot\": ") + mustNot.toString() + ",";
            expectedQuery.mustNot(mustNot);
        }
        if (tempQueryBuilder.should().size() > 0) {
            QueryBuilder should = tempQueryBuilder.should().get(0);
            contentString += "\"should\": " + should.toString() + ",";
            expectedQuery.should(should);
        }
        if (tempQueryBuilder.filter().size() > 0) {
            QueryBuilder filter = tempQueryBuilder.filter().get(0);
            contentString += "\"filter\": " + filter.toString() + ",";
            expectedQuery.filter(filter);
        }
        contentString = contentString.substring(0, contentString.length() - 1);
        contentString += "    }    \n" + "}";
        alternateVersions.put(contentString, expectedQuery);
        return alternateVersions;
    }

    public void testIllegalArguments() {
        BoolQueryBuilder booleanQuery = new BoolQueryBuilder();
        expectThrows(IllegalArgumentException.class, () -> booleanQuery.must(null));
        expectThrows(IllegalArgumentException.class, () -> booleanQuery.mustNot(null));
        expectThrows(IllegalArgumentException.class, () -> booleanQuery.filter(null));
        expectThrows(IllegalArgumentException.class, () -> booleanQuery.should(null));
    }

    // https://github.com/elastic/elasticsearch/issues/7240
    public void testEmptyBooleanQuery() throws Exception {
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(randomFrom(XContentType.values()));
        BytesReference query = contentBuilder.startObject().startObject("bool").endObject().endObject().bytes();
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertThat(parsedQuery, Matchers.instanceOf(MatchAllDocsQuery.class));
    }

    public void testDefaultMinShouldMatch() throws Exception {
        // Queries have a minShouldMatch of 0
        BooleanQuery bq = (BooleanQuery) parseQuery(boolQuery().must(termQuery("foo", "bar")).buildAsBytes()).toQuery(createShardContext());
        assertEquals(0, bq.getMinimumNumberShouldMatch());

        bq = (BooleanQuery) parseQuery(boolQuery().should(termQuery("foo", "bar")).buildAsBytes()).toQuery(createShardContext());
        assertEquals(0, bq.getMinimumNumberShouldMatch());

        // Filters have a minShouldMatch of 0/1
        ConstantScoreQuery csq = (ConstantScoreQuery) parseQuery(constantScoreQuery(boolQuery().must(termQuery("foo", "bar"))).buildAsBytes()).toQuery(createShardContext());
        bq = (BooleanQuery) csq.getQuery();
        assertEquals(0, bq.getMinimumNumberShouldMatch());

        csq = (ConstantScoreQuery) parseQuery(constantScoreQuery(boolQuery().should(termQuery("foo", "bar"))).buildAsBytes()).toQuery(createShardContext());
        bq = (BooleanQuery) csq.getQuery();
        assertEquals(1, bq.getMinimumNumberShouldMatch());
    }

    public void testMinShouldMatchFilterWithoutShouldClauses() throws Exception {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new BoolQueryBuilder().must(new MatchAllQueryBuilder()));
        Query query = boolQueryBuilder.toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(0));
        assertThat(booleanQuery.clauses().size(), equalTo(1));
        BooleanClause booleanClause = booleanQuery.clauses().get(0);
        assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.FILTER));
        assertThat(booleanClause.getQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery innerBooleanQuery = (BooleanQuery) booleanClause.getQuery();
        //we didn't set minimum should match initially, there are no should clauses so it should be 0
        assertThat(innerBooleanQuery.getMinimumNumberShouldMatch(), equalTo(0));
        assertThat(innerBooleanQuery.clauses().size(), equalTo(1));
        BooleanClause innerBooleanClause = innerBooleanQuery.clauses().get(0);
        assertThat(innerBooleanClause.getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(innerBooleanClause.getQuery(), instanceOf(MatchAllDocsQuery.class));
    }

    public void testMinShouldMatchFilterWithShouldClauses() throws Exception {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new BoolQueryBuilder().must(new MatchAllQueryBuilder()).should(new MatchAllQueryBuilder()));
        Query query = boolQueryBuilder.toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(0));
        assertThat(booleanQuery.clauses().size(), equalTo(1));
        BooleanClause booleanClause = booleanQuery.clauses().get(0);
        assertThat(booleanClause.getOccur(), equalTo(BooleanClause.Occur.FILTER));
        assertThat(booleanClause.getQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery innerBooleanQuery = (BooleanQuery) booleanClause.getQuery();
        //we didn't set minimum should match initially, but there are should clauses so it should be 1
        assertThat(innerBooleanQuery.getMinimumNumberShouldMatch(), equalTo(1));
        assertThat(innerBooleanQuery.clauses().size(), equalTo(2));
        BooleanClause innerBooleanClause1 = innerBooleanQuery.clauses().get(0);
        assertThat(innerBooleanClause1.getOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(innerBooleanClause1.getQuery(), instanceOf(MatchAllDocsQuery.class));
        BooleanClause innerBooleanClause2 = innerBooleanQuery.clauses().get(1);
        assertThat(innerBooleanClause2.getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(innerBooleanClause2.getQuery(), instanceOf(MatchAllDocsQuery.class));
    }

    public void testMinShouldMatchBiggerThanNumberOfShouldClauses() throws Exception {
        BooleanQuery bq = (BooleanQuery) parseQuery(
            boolQuery()
                .should(termQuery("foo", "bar"))
                .should(termQuery("foo2", "bar2"))
                .minimumNumberShouldMatch("3")
                .buildAsBytes()).toQuery(createShardContext());
        assertEquals(3, bq.getMinimumNumberShouldMatch());

        bq = (BooleanQuery) parseQuery(
            boolQuery()
                .should(termQuery("foo", "bar"))
                .should(termQuery("foo2", "bar2"))
                .minimumNumberShouldMatch(3)
                .buildAsBytes()).toQuery(createShardContext());
        assertEquals(3, bq.getMinimumNumberShouldMatch());
    }

    public void testMinShouldMatchDisableCoord() throws Exception {
        BooleanQuery bq = (BooleanQuery) parseQuery(
                boolQuery()
                        .should(termQuery("foo", "bar"))
                        .should(termQuery("foo2", "bar2"))
                        .minimumNumberShouldMatch("3")
                        .disableCoord(true)
                        .buildAsBytes()).toQuery(createShardContext());
        assertEquals(3, bq.getMinimumNumberShouldMatch());
    }

    public void testFromJson() throws IOException {
        String query =
                "{" +
                "\"bool\" : {" +
                "  \"must\" : [ {" +
                "    \"term\" : {" +
                "      \"user\" : {" +
                "        \"value\" : \"kimchy\"," +
                "        \"boost\" : 1.0" +
                "      }" +
                "    }" +
                "  } ]," +
                "  \"filter\" : [ {" +
                "    \"term\" : {" +
                "      \"tag\" : {" +
                "        \"value\" : \"tech\"," +
                "        \"boost\" : 1.0" +
                "      }" +
                "    }" +
                "  } ]," +
                "  \"must_not\" : [ {" +
                "    \"range\" : {" +
                "      \"age\" : {" +
                "        \"from\" : 10," +
                "        \"to\" : 20," +
                "        \"include_lower\" : true," +
                "        \"include_upper\" : true," +
                "        \"boost\" : 1.0" +
                "      }" +
                "    }" +
                "  } ]," +
                "  \"should\" : [ {" +
                "    \"term\" : {" +
                "      \"tag\" : {" +
                "        \"value\" : \"wow\"," +
                "        \"boost\" : 1.0" +
                "      }" +
                "    }" +
                "  }, {" +
                "    \"term\" : {" +
                "      \"tag\" : {" +
                "        \"value\" : \"elasticsearch\"," +
                "        \"boost\" : 1.0" +
                "      }" +
                "    }" +
                "  } ]," +
                "  \"disable_coord\" : false," +
                "  \"adjust_pure_negative\" : true," +
                "  \"minimum_should_match\" : \"23\"," +
                "  \"boost\" : 42.0" +
                "}" +
              "}";

        BoolQueryBuilder queryBuilder = (BoolQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);

        assertEquals(query, 42, queryBuilder.boost, 0.00001);
        assertEquals(query, "23", queryBuilder.minimumShouldMatch());
        assertEquals(query, "kimchy", ((TermQueryBuilder)queryBuilder.must().get(0)).value());
    }

    /**
     * we ignore empty query bodies if we are not in strict mode
     */
    public void testFromJsonEmptyQueryBody() throws IOException {
        String query =
                "{" +
                "\"bool\" : {" +
                "  \"must\" : [ { } ]," +
                "  \"filter\" : { }," +
                "  \"must_not\" : [ { \"constant_score\" : {\"filter\" : { } } } ]" +
                "}" +
              "}";

        BoolQueryBuilder queryBuilder = (BoolQueryBuilder) parseQuery(query, ParseFieldMatcher.EMPTY);
        assertEquals(query, 0, queryBuilder.must().size());
        assertEquals(query, 0, queryBuilder.filter().size());
        assertEquals(query, 0, queryBuilder.mustNot().size());
        assertEquals(query, 0, queryBuilder.should().size());

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parseQuery(query, ParseFieldMatcher.STRICT));
        assertThat(ex.getMessage(), startsWith("query malformed, empty clause found at"));
    }

    /**
     * test that unknown query names in the clauses throw an error
     */
    public void testUnknownQueryName() throws IOException {
        String query = "{\"bool\" : {\"must\" : { \"unknown_query\" : { } } } }";

        ParsingException ex = expectThrows(ParsingException.class, () -> parseQuery(query, ParseFieldMatcher.EMPTY));
        assertEquals("no [query] registered for [unknown_query]", ex.getMessage());
    }

    /**
     * test that two queries in object throws error
     */
    public void testTooManyQueriesInObject() throws IOException {
        String clauseType = randomFrom(new String[] {"must", "should", "must_not", "filter"});
        // should also throw error if invalid query is preceded by a valid one
        String query = "{\"bool\" : {\"" + clauseType
                + "\" : { \"match\" : { \"foo\" : \"bar\" } , \"match\" : { \"baz\" : \"buzz\" } } } }";
        ParsingException ex = expectThrows(ParsingException.class, () -> parseQuery(query, ParseFieldMatcher.EMPTY));
        assertEquals("expected [END_OBJECT] but got [FIELD_NAME], possibly too many query clauses", ex.getMessage());
    }

    public void testRewrite() throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolean mustRewrite = false;
        if (randomBoolean()) {
            mustRewrite = true;
            boolQueryBuilder.must(new WrapperQueryBuilder(new TermsQueryBuilder("foo", "must").toString()));
        }
        if (randomBoolean()) {
            mustRewrite = true;
            boolQueryBuilder.should(new WrapperQueryBuilder(new TermsQueryBuilder("foo", "should").toString()));
        }
        if (randomBoolean()) {
            mustRewrite = true;
            boolQueryBuilder.filter(new WrapperQueryBuilder(new TermsQueryBuilder("foo", "filter").toString()));
        }
        if (randomBoolean()) {
            mustRewrite = true;
            boolQueryBuilder.mustNot(new WrapperQueryBuilder(new TermsQueryBuilder("foo", "must_not").toString()));
        }
        if (mustRewrite == false && randomBoolean()) {
            boolQueryBuilder.must(new TermsQueryBuilder("foo", "no_rewrite"));
        }
        QueryBuilder rewritten = boolQueryBuilder.rewrite(createShardContext());
        if (mustRewrite == false && boolQueryBuilder.must().isEmpty()) {
            // if it's empty we rewrite to match all
            assertEquals(rewritten, new MatchAllQueryBuilder());
        } else {
            BoolQueryBuilder rewrite = (BoolQueryBuilder) rewritten;
            if (mustRewrite) {
                assertNotSame(rewrite, boolQueryBuilder);
                if (boolQueryBuilder.must().isEmpty() == false) {
                    assertEquals(new TermsQueryBuilder("foo", "must"), rewrite.must().get(0));
                }
                if (boolQueryBuilder.should().isEmpty() == false) {
                    assertEquals(new TermsQueryBuilder("foo", "should"), rewrite.should().get(0));
                }
                if (boolQueryBuilder.mustNot().isEmpty() == false) {
                    assertEquals(new TermsQueryBuilder("foo", "must_not"), rewrite.mustNot().get(0));
                }
                if (boolQueryBuilder.filter().isEmpty() == false) {
                    assertEquals(new TermsQueryBuilder("foo", "filter"), rewrite.filter().get(0));
                }
            } else {
                assertSame(rewrite, boolQueryBuilder);
                if (boolQueryBuilder.must().isEmpty() == false) {
                    assertSame(boolQueryBuilder.must().get(0), rewrite.must().get(0));
                }
            }
        }
    }

    public void testRewriteMultipleTimes() throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new WrapperQueryBuilder(new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()).toString()));
        QueryBuilder rewritten = boolQueryBuilder.rewrite(createShardContext());
        BoolQueryBuilder expected = new BoolQueryBuilder();
        expected.must(new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()));
        assertEquals(expected, rewritten);

        expected = new BoolQueryBuilder();
        expected.must(new MatchAllQueryBuilder());
        QueryBuilder rewrittenAgain = rewritten.rewrite(createShardContext());
        assertEquals(rewrittenAgain, expected);
        assertEquals(QueryBuilder.rewriteQuery(boolQueryBuilder, createShardContext()), expected);
    }
}
