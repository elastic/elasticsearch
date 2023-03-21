/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.index.query.QueryBuilders.wrapperQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;

public class MatchedQueriesIT extends ESIntegTestCase {
    public void testSimpleMatchedQueryFromFilteredQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "test1", "number", 1).get();
        client().prepareIndex("test").setId("2").setSource("name", "test2", "number", 2).get();
        client().prepareIndex("test").setId("3").setSource("name", "test3", "number", 3).get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(
                boolQuery().must(matchAllQuery())
                    .filter(
                        boolQuery().should(rangeQuery("number").lt(2).queryName("test1"))
                            .should(rangeQuery("number").gte(2).queryName("test2"))
                    )
            )
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("3") || hit.getId().equals("2")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("test2"));
                assertThat(hit.getMatchedQueryScore("test2"), equalTo(1f));
            } else if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("test1"));
                assertThat(hit.getMatchedQueryScore("test1"), equalTo(1f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }

        searchResponse = client().prepareSearch()
            .setQuery(
                boolQuery().should(rangeQuery("number").lte(2).queryName("test1")).should(rangeQuery("number").gt(2).queryName("test2"))
            )
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1") || hit.getId().equals("2")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("test1"));
                assertThat(hit.getMatchedQueryScore("test1"), equalTo(1f));
            } else if (hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("test2"));
                assertThat(hit.getMatchedQueryScore("test2"), equalTo(1f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testSimpleMatchedQueryFromTopLevelFilter() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "test", "title", "title1").get();
        client().prepareIndex("test").setId("2").setSource("name", "test").get();
        client().prepareIndex("test").setId("3").setSource("name", "test").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(
                boolQuery().should(termQuery("name", "test").queryName("name")).should(termQuery("title", "title1").queryName("title"))
            )
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(2));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("name"));
                assertThat(hit.getMatchedQueryScore("name"), greaterThan(0f));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("title"));
                assertThat(hit.getMatchedQueryScore("title"), greaterThan(0f));
            } else if (hit.getId().equals("2") || hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("name"));
                assertThat(hit.getMatchedQueryScore("name"), greaterThan(0f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setPostFilter(
                boolQuery().should(termQuery("name", "test").queryName("name")).should(termQuery("title", "title1").queryName("title"))
            )
            .get();

        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(2));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("name"));
                assertThat(hit.getMatchedQueryScore("name"), greaterThan(0f));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("title"));
                assertThat(hit.getMatchedQueryScore("title"), greaterThan(0f));
            } else if (hit.getId().equals("2") || hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("name"));
                assertThat(hit.getMatchedQueryScore("name"), greaterThan(0f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testSimpleMatchedQueryFromTopLevelFilterAndFilteredQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("name", "test", "title", "title1").get();
        client().prepareIndex("test").setId("2").setSource("name", "test", "title", "title2").get();
        client().prepareIndex("test").setId("3").setSource("name", "test", "title", "title3").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(boolQuery().must(matchAllQuery()).filter(termsQuery("title", "title1", "title2", "title3").queryName("title")))
            .setPostFilter(termQuery("name", "test").queryName("name"))
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1") || hit.getId().equals("2") || hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(2));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("name"));
                assertThat(hit.getMatchedQueryScore("name"), greaterThan(0f));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("title"));
                assertThat(hit.getMatchedQueryScore("title"), greaterThan(0f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }

        searchResponse = client().prepareSearch()
            .setQuery(termsQuery("title", "title1", "title2", "title3").queryName("title"))
            .setPostFilter(matchQuery("name", "test").queryName("name"))
            .get();
        assertHitCount(searchResponse, 3L);
        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1") || hit.getId().equals("2") || hit.getId().equals("3")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(2));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("name"));
                assertThat(hit.getMatchedQueryScore("name"), greaterThan(0f));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("title"));
                assertThat(hit.getMatchedQueryScore("title"), greaterThan(0f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testRegExpQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.regexpQuery("title", "title1").queryName("regex"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("regex"));
                assertThat(hit.getMatchedQueryScore("regex"), equalTo(1f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testPrefixQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.prefixQuery("title", "title").queryName("prefix"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("prefix"));
                assertThat(hit.getMatchedQueryScore("prefix"), equalTo(1f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testFuzzyQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.fuzzyQuery("title", "titel1").queryName("fuzzy"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("fuzzy"));
                assertThat(hit.getMatchedQueryScore("fuzzy"), greaterThan(0f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testWildcardQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.wildcardQuery("title", "titl*").queryName("wildcard"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("wildcard"));
                assertThat(hit.getMatchedQueryScore("wildcard"), equalTo(1f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    public void testSpanFirstQuerySupportsName() {
        createIndex("test1");
        ensureGreen();

        client().prepareIndex("test1").setId("1").setSource("title", "title1 title2").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(QueryBuilders.spanFirstQuery(QueryBuilders.spanTermQuery("title", "title1"), 10).queryName("span"))
            .get();
        assertHitCount(searchResponse, 1L);

        for (SearchHit hit : searchResponse.getHits()) {
            if (hit.getId().equals("1")) {
                assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                assertThat(hit.getMatchedQueriesAndScores(), hasKey("span"));
                assertThat(hit.getMatchedQueryScore("span"), greaterThan(0f));
            } else {
                fail("Unexpected document returned with id " + hit.getId());
            }
        }
    }

    /**
     * Test case for issue #4361: https://github.com/elastic/elasticsearch/issues/4361
     */
    public void testMatchedWithShould() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("content", "Lorem ipsum dolor sit amet").get();
        client().prepareIndex("test").setId("2").setSource("content", "consectetur adipisicing elit").get();
        refresh();

        // Execute search at least two times to load it in cache
        int iter = scaledRandomIntBetween(2, 10);
        for (int i = 0; i < iter; i++) {
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(
                    boolQuery().minimumShouldMatch(1)
                        .should(queryStringQuery("dolor").queryName("dolor"))
                        .should(queryStringQuery("elit").queryName("elit"))
                )
                .get();

            assertHitCount(searchResponse, 2L);
            for (SearchHit hit : searchResponse.getHits()) {
                if (hit.getId().equals("1")) {
                    assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                    assertThat(hit.getMatchedQueriesAndScores(), hasKey("dolor"));
                    assertThat(hit.getMatchedQueryScore("dolor"), greaterThan(0f));
                } else if (hit.getId().equals("2")) {
                    assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
                    assertThat(hit.getMatchedQueriesAndScores(), hasKey("elit"));
                    assertThat(hit.getMatchedQueryScore("elit"), greaterThan(0f));
                } else {
                    fail("Unexpected document returned with id " + hit.getId());
                }
            }
        }
    }

    public void testMatchedWithWrapperQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("content", "Lorem ipsum dolor sit amet").get();
        refresh();

        MatchQueryBuilder matchQueryBuilder = matchQuery("content", "amet").queryName("abc");
        BytesReference matchBytes = XContentHelper.toXContent(matchQueryBuilder, XContentType.JSON, false);
        TermQueryBuilder termQueryBuilder = termQuery("content", "amet").queryName("abc");
        BytesReference termBytes = XContentHelper.toXContent(termQueryBuilder, XContentType.JSON, false);
        QueryBuilder[] queries = new QueryBuilder[] { wrapperQuery(matchBytes), constantScoreQuery(wrapperQuery(termBytes)) };
        for (QueryBuilder query : queries) {
            SearchResponse searchResponse = client().prepareSearch().setQuery(query).get();
            assertHitCount(searchResponse, 1L);
            SearchHit hit = searchResponse.getHits().getAt(0);
            assertThat(hit.getMatchedQueriesAndScores().size(), equalTo(1));
            assertThat(hit.getMatchedQueriesAndScores(), hasKey("abc"));
            assertThat(hit.getMatchedQueryScore("abc"), greaterThan(0f));
        }
    }

    public void testMatchedWithRescoreQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("content", "hello world").get();
        client().prepareIndex("test").setId("2").setSource("content", "hello you").get();
        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(new MatchAllQueryBuilder().queryName("all"))
            .setRescorer(
                new QueryRescorerBuilder(new MatchPhraseQueryBuilder("content", "hello you").boost(10).queryName("rescore_phrase"))
            )
            .get();
        assertHitCount(searchResponse, 2L);
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries().length, equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).getMatchedQueries(), equalTo(new String[] { "all", "rescore_phrase" }));

        assertThat(searchResponse.getHits().getAt(1).getMatchedQueries().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(1).getMatchedQueries(), equalTo(new String[] { "all" }));
    }
}
