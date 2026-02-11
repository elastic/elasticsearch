/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;

public class BoolQueryBuilderTests extends AbstractQueryTestCase<BoolQueryBuilder> {
    @Override
    protected BoolQueryBuilder doCreateTestQueryBuilder() {
        BoolQueryBuilder query = new BoolQueryBuilder();
        if (randomBoolean()) {
            query.adjustPureNegative(randomBoolean());
        }
        if (randomBoolean()) {
            query.minimumShouldMatch(randomMinimumShouldMatch());
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
    protected BoolQueryBuilder createQueryWithInnerQuery(QueryBuilder queryBuilder) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().must(queryBuilder);
        if (randomBoolean()) {
            addRandomClause(boolQueryBuilder, queryBuilder);
        }
        return boolQueryBuilder;
    }

    private static void addRandomClause(BoolQueryBuilder boolQueryBuilder, QueryBuilder innerQueryBuilder) {
        int iters = randomIntBetween(1, 3);
        for (int i = 0; i < iters; i++) {
            if (randomBoolean()) {
                boolQueryBuilder.filter(randomBoolean() ? innerQueryBuilder : new MatchAllQueryBuilder());
            }
            if (randomBoolean()) {
                boolQueryBuilder.should(randomBoolean() ? innerQueryBuilder : new MatchAllQueryBuilder());
            }
            if (randomBoolean()) {
                boolQueryBuilder.mustNot(randomBoolean() ? innerQueryBuilder : new MatchAllQueryBuilder());
            }
            if (randomBoolean()) {
                boolQueryBuilder.filter(randomBoolean() ? innerQueryBuilder : new MatchAllQueryBuilder());
            }
        }
    }

    @Override
    protected void doAssertLuceneQuery(BoolQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        if (queryBuilder.hasClauses() == false) {
            assertThat(query, instanceOf(MatchAllDocsQuery.class));
        } else {
            List<BooleanClause> clauses = new ArrayList<>();
            clauses.addAll(getBooleanClauses(queryBuilder.must(), BooleanClause.Occur.MUST, context));
            clauses.addAll(getBooleanClauses(queryBuilder.mustNot(), BooleanClause.Occur.MUST_NOT, context));
            clauses.addAll(getBooleanClauses(queryBuilder.should(), BooleanClause.Occur.SHOULD, context));
            clauses.addAll(getBooleanClauses(queryBuilder.filter(), BooleanClause.Occur.FILTER, context));

            if (clauses.isEmpty()) {
                assertThat(query, instanceOf(MatchAllDocsQuery.class));
            } else if (query instanceof MatchNoDocsQuery == false) {
                assertThat(query, instanceOf(BooleanQuery.class));
                BooleanQuery booleanQuery = (BooleanQuery) query;
                if (queryBuilder.adjustPureNegative()) {
                    boolean isNegative = true;
                    for (BooleanClause clause : clauses) {
                        if (clause.isProhibited() == false) {
                            isNegative = false;
                            break;
                        }
                    }
                    if (isNegative) {
                        clauses.add(new BooleanClause(Queries.ALL_DOCS_INSTANCE, BooleanClause.Occur.MUST));
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

    private static List<BooleanClause> getBooleanClauses(
        List<QueryBuilder> queryBuilders,
        BooleanClause.Occur occur,
        SearchExecutionContext context
    ) throws IOException {
        List<BooleanClause> clauses = new ArrayList<>();
        for (QueryBuilder query : queryBuilders) {
            Query innerQuery = query.rewrite(context).toQuery(context);
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
        String contentString = """
            {
                "bool" : {
            """;
        if (tempQueryBuilder.must().size() > 0) {
            QueryBuilder must = tempQueryBuilder.must().get(0);
            contentString += "\"must\": " + must.toString() + ",";
            expectedQuery.must(must);
        }
        if (tempQueryBuilder.mustNot().size() > 0) {
            QueryBuilder mustNot = tempQueryBuilder.mustNot().get(0);
            contentString += "\"must_not\":" + mustNot.toString() + ",";
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
        contentString += "    }    \n}";
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
        contentBuilder.startObject().startObject("bool").endObject().endObject();
        try (XContentParser xParser = createParser(contentBuilder)) {
            Query parsedQuery = parseQuery(xParser).toQuery(createSearchExecutionContext());
            assertThat(parsedQuery, Matchers.instanceOf(MatchAllDocsQuery.class));
        }
    }

    public void testMinShouldMatchFilterWithoutShouldClauses() throws Exception {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new BoolQueryBuilder().must(new MatchAllQueryBuilder()));
        Query query = boolQueryBuilder.toQuery(createSearchExecutionContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(0));
        assertThat(booleanQuery.clauses().size(), equalTo(1));
        BooleanClause booleanClause = booleanQuery.clauses().get(0);
        assertThat(booleanClause.occur(), equalTo(BooleanClause.Occur.FILTER));
        assertThat(booleanClause.query(), instanceOf(BooleanQuery.class));
        BooleanQuery innerBooleanQuery = (BooleanQuery) booleanClause.query();
        // we didn't set minimum should match initially, there are no should clauses so it should be 0
        assertThat(innerBooleanQuery.getMinimumNumberShouldMatch(), equalTo(0));
        assertThat(innerBooleanQuery.clauses().size(), equalTo(1));
        BooleanClause innerBooleanClause = innerBooleanQuery.clauses().get(0);
        assertThat(innerBooleanClause.occur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(innerBooleanClause.query(), instanceOf(MatchAllDocsQuery.class));
    }

    public void testMinShouldMatchBiggerThanNumberOfShouldClauses() throws Exception {
        BooleanQuery bq = (BooleanQuery) parseQuery(
            boolQuery().should(termQuery(TEXT_FIELD_NAME, "bar")).should(termQuery(KEYWORD_FIELD_NAME, "bar2")).minimumShouldMatch("3")
        ).toQuery(createSearchExecutionContext());
        assertEquals(3, bq.getMinimumNumberShouldMatch());

        bq = (BooleanQuery) parseQuery(
            boolQuery().should(termQuery(TEXT_FIELD_NAME, "bar")).should(termQuery(KEYWORD_FIELD_NAME, "bar2")).minimumShouldMatch(3)
        ).toQuery(createSearchExecutionContext());
        assertEquals(3, bq.getMinimumNumberShouldMatch());
    }

    public void testMinShouldMatchDisableCoord() throws Exception {
        BooleanQuery bq = (BooleanQuery) parseQuery(
            boolQuery().should(termQuery(TEXT_FIELD_NAME, "bar")).should(termQuery(TEXT_FIELD_NAME, "bar2")).minimumShouldMatch("3")
        ).toQuery(createSearchExecutionContext());
        assertEquals(3, bq.getMinimumNumberShouldMatch());
    }

    public void testFromJson() throws IOException {
        String query = "{"
            + "\"bool\" : {"
            + "  \"must\" : [ {"
            + "    \"term\" : {"
            + "      \"user\" : {"
            + "        \"value\" : \"kimchy\""
            + "      }"
            + "    }"
            + "  } ],"
            + "  \"filter\" : [ {"
            + "    \"term\" : {"
            + "      \"tag\" : {"
            + "        \"value\" : \"tech\""
            + "      }"
            + "    }"
            + "  } ],"
            + "  \"must_not\" : [ {"
            + "    \"range\" : {"
            + "      \"age\" : {"
            + "        \"gte\" : 10,"
            + "        \"lte\" : 20,"
            + "        \"boost\" : 1.0"
            + "      }"
            + "    }"
            + "  } ],"
            + "  \"should\" : [ {"
            + "    \"term\" : {"
            + "      \"tag\" : {"
            + "        \"value\" : \"wow\""
            + "      }"
            + "    }"
            + "  }, {"
            + "    \"term\" : {"
            + "      \"tag\" : {"
            + "        \"value\" : \"elasticsearch\""
            + "      }"
            + "    }"
            + "  } ],"
            + "  \"minimum_should_match\" : \"23\","
            + "  \"boost\" : 42.0"
            + "}"
            + "}";

        BoolQueryBuilder queryBuilder = (BoolQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);

        assertEquals(query, 42, queryBuilder.boost, 0.00001);
        assertEquals(query, "23", queryBuilder.minimumShouldMatch());
        assertEquals(query, "kimchy", ((TermQueryBuilder) queryBuilder.must().get(0)).value());
    }

    public void testMinimumShouldMatchNumber() throws IOException {
        String query = """
            {"bool" : {"must" : { "term" : { "field" : "value" } }, "minimum_should_match" : 1 } }""";
        BoolQueryBuilder builder = (BoolQueryBuilder) parseQuery(query);
        assertEquals("1", builder.minimumShouldMatch());
    }

    public void testMinimumShouldMatchNull() throws IOException {
        String query = """
            {"bool" : {"must" : { "term" : { "field" : "value" } }, "minimum_should_match" : null } }""";
        BoolQueryBuilder builder = (BoolQueryBuilder) parseQuery(query);
        assertEquals(null, builder.minimumShouldMatch());
    }

    public void testMustNull() throws IOException {
        String query = "{\"bool\" : {\"must\" : null } }";
        BoolQueryBuilder builder = (BoolQueryBuilder) parseQuery(query);
        assertTrue(builder.must().isEmpty());
    }

    public void testMustNotNull() throws IOException {
        String query = "{\"bool\" : {\"must_not\" : null } }";
        BoolQueryBuilder builder = (BoolQueryBuilder) parseQuery(query);
        assertTrue(builder.mustNot().isEmpty());
    }

    public void testShouldNull() throws IOException {
        String query = "{\"bool\" : {\"should\" : null } }";
        BoolQueryBuilder builder = (BoolQueryBuilder) parseQuery(query);
        assertTrue(builder.should().isEmpty());
    }

    public void testFilterNull() throws IOException {
        String query = "{\"bool\" : {\"filter\" : null } }";
        BoolQueryBuilder builder = (BoolQueryBuilder) parseQuery(query);
        assertTrue(builder.filter().isEmpty());
    }

    /**
     * test that unknown query names in the clauses throw an error
     */
    public void testUnknownQueryName() throws IOException {
        String query = "{\"bool\" : {\"must\" : { \"unknown_query\" : { } } } }";

        XContentParseException ex = expectThrows(XContentParseException.class, () -> parseQuery(query));
        assertEquals("[1:41] [bool] failed to parse field [must]", ex.getMessage());
        Throwable e = ex.getCause();
        assertThat(e.getMessage(), containsString("unknown query [unknown_query]"));

    }

    public void testDeprecation() throws IOException {
        String query = "{\"bool\" : {\"mustNot\" : { \"match_all\" : { } } } }";
        QueryBuilder q = parseQuery(query);
        QueryBuilder expected = new BoolQueryBuilder().mustNot(new MatchAllQueryBuilder());
        assertEquals(expected, q);
        assertWarnings("Deprecated field [mustNot] used, expected [must_not] instead");
    }

    public void testRewrite() throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolean mustRewrite = false;
        if (randomBoolean()) {
            mustRewrite = true;
            boolQueryBuilder.must(new WrapperQueryBuilder(new TermsQueryBuilder(TEXT_FIELD_NAME, "must").toString()));
        }
        if (randomBoolean()) {
            mustRewrite = true;
            boolQueryBuilder.should(new WrapperQueryBuilder(new TermsQueryBuilder(TEXT_FIELD_NAME, "should").toString()));
        }
        if (randomBoolean()) {
            mustRewrite = true;
            boolQueryBuilder.filter(new WrapperQueryBuilder(new TermsQueryBuilder(TEXT_FIELD_NAME, "filter").toString()));
        }
        if (randomBoolean()) {
            mustRewrite = true;
            boolQueryBuilder.mustNot(new WrapperQueryBuilder(new TermsQueryBuilder(TEXT_FIELD_NAME, "must_not").toString()));
        }
        if (mustRewrite == false && randomBoolean()) {
            boolQueryBuilder.must(new TermsQueryBuilder(TEXT_FIELD_NAME, "no_rewrite"));
        }
        QueryBuilder rewritten = boolQueryBuilder.rewrite(createSearchExecutionContext());
        if (mustRewrite == false && boolQueryBuilder.must().isEmpty()) {
            // if it's empty we rewrite to match all
            assertEquals(rewritten, new MatchAllQueryBuilder());
        } else {
            BoolQueryBuilder rewrite = (BoolQueryBuilder) rewritten;
            if (mustRewrite) {
                assertNotSame(rewrite, boolQueryBuilder);
                if (boolQueryBuilder.must().isEmpty() == false) {
                    assertEquals(new TermsQueryBuilder(TEXT_FIELD_NAME, "must"), rewrite.must().get(0));
                }
                if (boolQueryBuilder.should().isEmpty() == false) {
                    assertEquals(new TermsQueryBuilder(TEXT_FIELD_NAME, "should"), rewrite.should().get(0));
                }
                if (boolQueryBuilder.mustNot().isEmpty() == false) {
                    assertEquals(new TermsQueryBuilder(TEXT_FIELD_NAME, "must_not"), rewrite.mustNot().get(0));
                }
                if (boolQueryBuilder.filter().isEmpty() == false) {
                    assertEquals(new TermsQueryBuilder(TEXT_FIELD_NAME, "filter"), rewrite.filter().get(0));
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
        QueryBuilder rewritten = boolQueryBuilder.rewrite(createSearchExecutionContext());
        BoolQueryBuilder expected = new BoolQueryBuilder();
        expected.must(new MatchAllQueryBuilder());
        assertEquals(expected, rewritten);

        expected = new BoolQueryBuilder();
        expected.must(new MatchAllQueryBuilder());
        QueryBuilder rewrittenAgain = rewritten.rewrite(createSearchExecutionContext());
        assertEquals(rewrittenAgain, expected);
        assertEquals(Rewriteable.rewrite(boolQueryBuilder, createSearchExecutionContext()), expected);
    }

    public void testRewriteWithMatchNone() throws IOException {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new WrapperQueryBuilder(new WrapperQueryBuilder(new MatchNoneQueryBuilder().toString()).toString()));
        QueryBuilder rewritten = boolQueryBuilder.rewrite(createSearchExecutionContext());
        assertEquals(new MatchNoneQueryBuilder(), rewritten);

        boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new TermQueryBuilder(TEXT_FIELD_NAME, "bar"));
        boolQueryBuilder.filter(new WrapperQueryBuilder(new WrapperQueryBuilder(new MatchNoneQueryBuilder().toString()).toString()));
        rewritten = boolQueryBuilder.rewrite(createSearchExecutionContext());
        assertEquals(new MatchNoneQueryBuilder(), rewritten);

        boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.must(new TermQueryBuilder(TEXT_FIELD_NAME, "bar"));
        boolQueryBuilder.filter(
            new BoolQueryBuilder().should(new TermQueryBuilder(TEXT_FIELD_NAME, "bar")).filter(new MatchNoneQueryBuilder())
        );
        rewritten = Rewriteable.rewrite(boolQueryBuilder, createSearchExecutionContext());
        assertEquals(new MatchNoneQueryBuilder(), rewritten);

        boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(new WrapperQueryBuilder(new MatchNoneQueryBuilder().toString()));
        rewritten = Rewriteable.rewrite(boolQueryBuilder, createSearchExecutionContext());
        assertEquals(new MatchNoneQueryBuilder(), rewritten);

        boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(new TermQueryBuilder(TEXT_FIELD_NAME, "bar"));
        boolQueryBuilder.should(new WrapperQueryBuilder(new MatchNoneQueryBuilder().toString()));
        rewritten = Rewriteable.rewrite(boolQueryBuilder, createSearchExecutionContext());
        assertNotEquals(new MatchNoneQueryBuilder(), rewritten);

        boolQueryBuilder = new BoolQueryBuilder();
        rewritten = Rewriteable.rewrite(boolQueryBuilder, createSearchExecutionContext());
        assertNotEquals(new MatchNoneQueryBuilder(), rewritten);

        boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(new WrapperQueryBuilder(new MatchNoneQueryBuilder().toString()));
        boolQueryBuilder.mustNot(new TermQueryBuilder(TEXT_FIELD_NAME, "bar"));
        rewritten = Rewriteable.rewrite(boolQueryBuilder, createSearchExecutionContext());
        assertNotEquals(new MatchNoneQueryBuilder(), rewritten);

        boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder(TEXT_FIELD_NAME, "bar"));
        boolQueryBuilder.mustNot(new WrapperQueryBuilder(new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()).toString()));
        rewritten = boolQueryBuilder.rewrite(createSearchExecutionContext());
        assertEquals(new MatchNoneQueryBuilder(), rewritten);
    }

    @Override
    public void testMustRewrite() throws IOException {
        SearchExecutionContext context = createSearchExecutionContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder termQuery = new TermQueryBuilder("unmapped_field", 42);
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        boolQuery.must(termQuery);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> boolQuery.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testShallowCopy() {
        BoolQueryBuilder orig = createTestQueryBuilder();
        BoolQueryBuilder shallowCopy = orig.shallowCopy();
        assertThat(shallowCopy.adjustPureNegative(), equalTo(orig.adjustPureNegative()));
        assertThat(shallowCopy.minimumShouldMatch(), equalTo(orig.minimumShouldMatch()));
        assertThat(shallowCopy.must(), equalTo(orig.must()));
        assertThat(shallowCopy.mustNot(), equalTo(orig.mustNot()));
        assertThat(shallowCopy.should(), equalTo(orig.should()));
        assertThat(shallowCopy.filter(), equalTo(orig.filter()));

        QueryBuilder b = new MatchQueryBuilder("foo", "bar");
        switch (between(0, 3)) {
            case 0 -> {
                shallowCopy.must(b);
                assertThat(shallowCopy.must(), hasItem(b));
                assertThat(orig.must(), not(hasItem(b)));
            }
            case 1 -> {
                shallowCopy.mustNot(b);
                assertThat(shallowCopy.mustNot(), hasItem(b));
                assertThat(orig.mustNot(), not(hasItem(b)));
            }
            case 2 -> {
                shallowCopy.should(b);
                assertThat(shallowCopy.should(), hasItem(b));
                assertThat(orig.should(), not(hasItem(b)));
            }
            case 3 -> {
                shallowCopy.filter(b);
                assertThat(shallowCopy.filter(), hasItem(b));
                assertThat(orig.filter(), not(hasItem(b)));
            }
        }
    }

    public void testAutoPrefiltering_GivenSingleMustPrefilteringClause() throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndNoFilters(BooleanClause.Occur.MUST);
    }

    public void testAutoPrefiltering_GivenSingleShouldPrefilteringClause() throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndNoFilters(BooleanClause.Occur.SHOULD);
    }

    public void testAutoPrefiltering_GivenSingleFilterPrefilteringClause() throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndNoFilters(BooleanClause.Occur.FILTER);
    }

    public void testAutoPrefiltering_GivenSingleMustNotPrefilteringClause() throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndNoFilters(BooleanClause.Occur.MUST_NOT);
    }

    public void testAutoPrefiltering_GivenSingleMustPrefilteringClauseAndRandomGroupOfClauses() throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndSingleGroupOfFilters(
            BooleanClause.Occur.MUST,
            randomFrom(BooleanClause.Occur.values())
        );
    }

    public void testAutoPrefiltering_GivenSingleShouldPrefilteringClauseAndRandomGroupOfClauses() throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndSingleGroupOfFilters(
            BooleanClause.Occur.SHOULD,
            randomFrom(BooleanClause.Occur.values())
        );
    }

    public void testAutoPrefiltering_GivenSingleFilterPrefilteringClauseAndRandomGroupOfClauses() throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndSingleGroupOfFilters(
            BooleanClause.Occur.FILTER,
            randomFrom(BooleanClause.Occur.values())
        );
    }

    public void testAutoPrefiltering_GivenSingleMustNotPrefilteringClauseAndRandomGroupOfClauses() throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndSingleGroupOfFilters(
            BooleanClause.Occur.MUST_NOT,
            randomFrom(BooleanClause.Occur.values())
        );
    }

    private void doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndNoFilters(BooleanClause.Occur prefilteringOccur)
        throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndSingleGroupOfFilters(
            prefilteringOccur,
            randomFrom(BooleanClause.Occur.values()),
            true
        );
    }

    private void doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndSingleGroupOfFilters(
        BooleanClause.Occur prefilteringOccur,
        BooleanClause.Occur filterOccur
    ) throws IOException {
        doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndSingleGroupOfFilters(prefilteringOccur, filterOccur, false);
    }

    private void doTestAutoPrefiltering_GivenSinglePrefilteringClauseAndSingleGroupOfFilters(
        BooleanClause.Occur prefilteringOccur,
        BooleanClause.Occur filterOccur,
        boolean noFilters
    ) throws IOException {
        final BoolQueryBuilder query = new BoolQueryBuilder();
        Map<String, Set<QueryBuilder>> prefiltersToQueryNameMap = new HashMap<>();
        QueryBuilder prefilteringClause = new TestAutoPrefilteringQueryBuilder("test", prefiltersToQueryNameMap);
        switch (prefilteringOccur) {
            case MUST -> query.must(prefilteringClause);
            case SHOULD -> query.should(prefilteringClause);
            case FILTER -> query.filter(prefilteringClause);
            case MUST_NOT -> query.mustNot(prefilteringClause);
        }
        if (noFilters == false) {
            randomList(1, 5, () -> randomTermQueryMaybeWrappedInCompoundQuery()).forEach(f -> {
                switch (filterOccur) {
                    case MUST -> query.must(f);
                    case SHOULD -> query.should(f);
                    case FILTER -> query.filter(f);
                    case MUST_NOT -> query.mustNot(f);
                }
            });
        }

        QueryBuilder rewritten = Rewriteable.rewrite(query, createQueryRewriteContext());
        rewritten.toQuery(createSearchExecutionContext());

        Set<QueryBuilder> expectedFilters = new HashSet<>();
        if (noFilters == false) {
            switch (filterOccur) {
                case MUST -> expectedFilters.addAll(query.must());
                case FILTER -> expectedFilters.addAll(query.filter());
                case MUST_NOT -> expectedFilters.addAll(
                    query.mustNot().stream().map(q -> new BoolQueryBuilder().mustNot(q)).collect(Collectors.toList())
                );
            }
            switch (prefilteringOccur) {
                case MUST -> expectedFilters.remove(prefilteringClause);
                case SHOULD -> expectedFilters.remove(prefilteringClause);
                case FILTER -> expectedFilters.remove(prefilteringClause);
                case MUST_NOT -> expectedFilters.remove(new BoolQueryBuilder().mustNot(prefilteringClause));
            }
        }
        assertThat(prefiltersToQueryNameMap.get("test"), equalTo(expectedFilters));
    }

    public void testAutoPrefiltering_GivenRandomMultipleClauses() throws IOException {
        for (int i = 0; i < 100; i++) {
            BoolQueryBuilder rootQuery = new BoolQueryBuilder();
            Map<String, Set<QueryBuilder>> prefiltersToQueryNameMap = new HashMap<>();
            TestAutoPrefilteringQueryBuilder must_1 = new TestAutoPrefilteringQueryBuilder("must_1", prefiltersToQueryNameMap);
            TestAutoPrefilteringQueryBuilder must_2 = new TestAutoPrefilteringQueryBuilder("must_2", prefiltersToQueryNameMap);
            TestAutoPrefilteringQueryBuilder should_1 = new TestAutoPrefilteringQueryBuilder("should_1", prefiltersToQueryNameMap);
            TestAutoPrefilteringQueryBuilder should_2 = new TestAutoPrefilteringQueryBuilder("should_2", prefiltersToQueryNameMap);
            TestAutoPrefilteringQueryBuilder filter_1 = new TestAutoPrefilteringQueryBuilder("filter_1", prefiltersToQueryNameMap);
            TestAutoPrefilteringQueryBuilder filter_2 = new TestAutoPrefilteringQueryBuilder("filter_2", prefiltersToQueryNameMap);
            TestAutoPrefilteringQueryBuilder must_not_1 = new TestAutoPrefilteringQueryBuilder("must_not_1", prefiltersToQueryNameMap);
            TestAutoPrefilteringQueryBuilder must_not_2 = new TestAutoPrefilteringQueryBuilder("must_not_2", prefiltersToQueryNameMap);
            rootQuery.must(must_1);
            rootQuery.must(must_2);
            rootQuery.should(should_1);
            rootQuery.should(should_2);
            rootQuery.filter(filter_1);
            rootQuery.filter(filter_2);
            rootQuery.mustNot(must_not_1);
            rootQuery.mustNot(must_not_2);

            randomList(5, () -> randomTermQueryMaybeWrappedInCompoundQuery()).forEach(rootQuery::must);
            randomList(5, () -> randomTermQueryMaybeWrappedInCompoundQuery()).forEach(rootQuery::should);
            randomList(5, () -> randomTermQueryMaybeWrappedInCompoundQuery()).forEach(rootQuery::filter);
            randomList(5, () -> randomTermQueryMaybeWrappedInCompoundQuery()).forEach(rootQuery::mustNot);

            // We add a must clause that is another bool query containing a prefiltering clause
            BoolQueryBuilder bool_1 = new BoolQueryBuilder();
            TestAutoPrefilteringQueryBuilder must_3 = new TestAutoPrefilteringQueryBuilder("must_3", prefiltersToQueryNameMap);
            bool_1.must(must_3);
            randomList(5, () -> randomTermQueryMaybeWrappedInCompoundQuery()).forEach(bool_1::filter);
            rootQuery.must(bool_1);

            rootQuery = (BoolQueryBuilder) Rewriteable.rewrite(rootQuery, createQueryRewriteContext());
            rootQuery.toQuery(createSearchExecutionContext());

            Set<QueryBuilder> expectedPrefilters = collectExpectedPrefilters(rootQuery);
            assertThat(
                prefiltersToQueryNameMap.get("must_1"),
                equalTo(expectedPrefilters.stream().filter(q -> q != must_1).collect(Collectors.toSet()))
            );
            assertThat(
                prefiltersToQueryNameMap.get("must_2"),
                equalTo(expectedPrefilters.stream().filter(q -> q != must_2).collect(Collectors.toSet()))
            );
            assertThat(prefiltersToQueryNameMap.get("should_1"), containsInAnyOrder(expectedPrefilters.toArray()));
            assertThat(prefiltersToQueryNameMap.get("should_2"), containsInAnyOrder(expectedPrefilters.toArray()));
            assertThat(
                prefiltersToQueryNameMap.get("filter_1"),
                equalTo(expectedPrefilters.stream().filter(q -> q != filter_1).collect(Collectors.toSet()))
            );
            assertThat(
                prefiltersToQueryNameMap.get("filter_2"),
                equalTo(expectedPrefilters.stream().filter(q -> q != filter_2).collect(Collectors.toSet()))
            );
            assertThat(
                prefiltersToQueryNameMap.get("must_not_1"),
                equalTo(
                    expectedPrefilters.stream().filter(q -> q.equals(boolQuery().mustNot(must_not_1)) == false).collect(Collectors.toSet())
                )
            );
            assertThat(
                prefiltersToQueryNameMap.get("must_not_2"),
                equalTo(
                    expectedPrefilters.stream().filter(q -> q.equals(boolQuery().mustNot(must_not_2)) == false).collect(Collectors.toSet())
                )
            );

            expectedPrefilters = collectExpectedPrefilters(rootQuery, bool_1);
            assertThat(
                prefiltersToQueryNameMap.get("must_3"),
                equalTo(expectedPrefilters.stream().filter(q -> q != must_3 && q != bool_1).collect(Collectors.toSet()))
            );
        }
    }

    private static QueryBuilder randomTermQueryMaybeWrappedInCompoundQuery() {
        QueryBuilder termQuery = randomTermQuery();
        if (randomBoolean()) {
            return termQuery;
        }
        return switch (randomIntBetween(0, 4)) {
            case 0 -> QueryBuilders.constantScoreQuery(termQuery);
            case 1 -> QueryBuilders.functionScoreQuery(termQuery);
            case 2 -> QueryBuilders.boostingQuery(termQuery, randomTermQuery());
            case 3 -> QueryBuilders.disMaxQuery().add(termQuery).add(randomTermQuery());
            case 4 -> QueryBuilders.boolQuery().filter(termQuery);
            default -> throw new IllegalStateException("Unexpected value: " + randomIntBetween(0, 2));
        };
    }

    private static QueryBuilder randomTermQuery() {
        String filterFieldName = randomBoolean() ? KEYWORD_FIELD_NAME : TEXT_FIELD_NAME;
        return termQuery(filterFieldName, randomAlphaOfLength(10));
    }

    private static Set<QueryBuilder> collectExpectedPrefilters(BoolQueryBuilder... queries) {
        Set<QueryBuilder> expectedPrefilters = new HashSet<>();
        for (BoolQueryBuilder query : queries) {
            expectedPrefilters.addAll(query.must());
            expectedPrefilters.addAll(query.filter());
            expectedPrefilters.addAll(query.mustNot().stream().map(q -> boolQuery().mustNot(q)).collect(Collectors.toList()));
        }
        return expectedPrefilters;
    }

    private static final class TestAutoPrefilteringQueryBuilder extends AbstractQueryBuilder<TestAutoPrefilteringQueryBuilder> {

        Map<String, Set<QueryBuilder>> prefiltersToQueryNameMap;

        private TestAutoPrefilteringQueryBuilder(String queryName, Map<String, Set<QueryBuilder>> prefiltersToQueryNameMap) {
            super();
            queryName(queryName);
            this.prefiltersToQueryNameMap = prefiltersToQueryNameMap;
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {}

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            prefiltersToQueryNameMap.put(queryName(), context.autoPrefilteringScope().getPrefilters().stream().collect(Collectors.toSet()));
            return Queries.ALL_DOCS_INSTANCE;
        }

        @Override
        protected boolean doEquals(TestAutoPrefilteringQueryBuilder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return "";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }
    }
}
