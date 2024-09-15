/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.tests.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.search.profile.query.RandomQueryGenerator.randomQueryBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class QueryProfilerIT extends ESIntegTestCase {

    /**
     * This test simply checks to make sure nothing crashes.  Test indexes 100-150 documents,
     * constructs 20-100 random queries and tries to profile them
     */
    public void testProfileQuery() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        List<String> stringFields = Arrays.asList("field1");
        List<String> numericFields = Arrays.asList("field2");

        indexRandom(true, docs);

        refresh();
        int iters = between(20, 100);
        for (int i = 0; i < iters; i++) {
            QueryBuilder q = randomQueryBuilder(stringFields, numericFields, numDocs, 3);
            logger.info("Query: {}", q);
            assertResponse(
                prepareSearch().setQuery(q).setTrackTotalHits(true).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH),
                response -> {
                    assertNotNull("Profile response element should not be null", response.getProfileResults());
                    assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));
                    for (Map.Entry<String, SearchProfileShardResult> shard : response.getProfileResults().entrySet()) {
                        for (QueryProfileShardResult searchProfiles : shard.getValue().getQueryProfileResults()) {
                            for (ProfileResult result : searchProfiles.getQueryResults()) {
                                assertNotNull(result.getQueryName());
                                assertNotNull(result.getLuceneDescription());
                                assertThat(result.getTime(), greaterThan(0L));
                            }

                            CollectorResult result = searchProfiles.getCollectorResult();
                            assertThat(result.getName(), is(not(emptyOrNullString())));
                            assertThat(result.getTime(), greaterThan(0L));
                        }
                    }
                }
            );
        }
    }

    /**
     * This test generates a random query and executes a profiled and non-profiled
     * search for each query.  It then does some basic sanity checking of score and hits
     * to make sure the profiling doesn't interfere with the hits being returned
     */
    public void testProfileMatchesRegular() throws Exception {
        createIndex("test", 1, 0);
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i))
                .setSource("id", String.valueOf(i), "field1", English.intToEnglish(i), "field2", i);
        }

        List<String> stringFields = Arrays.asList("field1");
        List<String> numericFields = Arrays.asList("field2");

        indexRandom(true, docs);

        refresh();
        QueryBuilder q = randomQueryBuilder(stringFields, numericFields, numDocs, 3);
        logger.debug("Query: {}", q);

        SearchRequestBuilder vanilla = prepareSearch("test").setQuery(q)
            .setProfile(false)
            .addSort("id.keyword", SortOrder.ASC)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setRequestCache(false);

        SearchRequestBuilder profile = prepareSearch("test").setQuery(q)
            .setProfile(true)
            .addSort("id.keyword", SortOrder.ASC)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .setRequestCache(false);

        assertResponse(client().prepareMultiSearch().add(vanilla).add(profile), response -> {
            Item[] responses = response.getResponses();

            SearchResponse vanillaResponse = responses[0].getResponse();
            SearchResponse profileResponse = responses[1].getResponse();

            assertThat(vanillaResponse.getFailedShards(), equalTo(0));
            assertThat(profileResponse.getFailedShards(), equalTo(0));
            assertThat(vanillaResponse.getSuccessfulShards(), equalTo(profileResponse.getSuccessfulShards()));

            float vanillaMaxScore = vanillaResponse.getHits().getMaxScore();
            float profileMaxScore = profileResponse.getHits().getMaxScore();
            if (Float.isNaN(vanillaMaxScore)) {
                assertTrue("Vanilla maxScore is NaN but Profile is not [" + profileMaxScore + "]", Float.isNaN(profileMaxScore));
            } else {
                assertEquals(
                    "Profile maxScore of [" + profileMaxScore + "] is not close to Vanilla maxScore [" + vanillaMaxScore + "]",
                    vanillaMaxScore,
                    profileMaxScore,
                    0.001
                );
            }

            if (vanillaResponse.getHits().getTotalHits().value() != profileResponse.getHits().getTotalHits().value()) {
                Set<SearchHit> vanillaSet = new HashSet<>(Arrays.asList(vanillaResponse.getHits().getHits()));
                Set<SearchHit> profileSet = new HashSet<>(Arrays.asList(profileResponse.getHits().getHits()));
                if (vanillaResponse.getHits().getTotalHits().value() > profileResponse.getHits().getTotalHits().value()) {
                    vanillaSet.removeAll(profileSet);
                    fail("Vanilla hits were larger than profile hits.  Non-overlapping elements were: " + vanillaSet.toString());
                } else {
                    profileSet.removeAll(vanillaSet);
                    fail("Profile hits were larger than vanilla hits.  Non-overlapping elements were: " + profileSet.toString());
                }
            }

            SearchHit[] vanillaHits = vanillaResponse.getHits().getHits();
            SearchHit[] profileHits = profileResponse.getHits().getHits();

            for (int j = 0; j < vanillaHits.length; j++) {
                assertThat(
                    "Profile hit #" + j + " has a different ID from Vanilla",
                    vanillaHits[j].getId(),
                    equalTo(profileHits[j].getId())
                );
            }
        });
    }

    /**
     * This test verifies that the output is reasonable for a simple, non-nested query
     */
    public void testSimpleMatch() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);
        ensureGreen();

        QueryBuilder q = QueryBuilders.matchQuery("field1", "one");

        assertResponse(prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH), response -> {
            Map<String, SearchProfileShardResult> p = response.getProfileResults();
            assertNotNull(p);
            assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

            for (Map.Entry<String, SearchProfileShardResult> shardResult : response.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertEquals(result.getQueryName(), "TermQuery");
                        assertEquals(result.getLuceneDescription(), "field1:one");
                        assertThat(result.getTime(), greaterThan(0L));
                        assertNotNull(result.getTimeBreakdown());
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }
        });
    }

    /**
     * This test verifies that the output is reasonable for a nested query
     */
    public void testBool() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        QueryBuilder q = QueryBuilders.boolQuery()
            .must(QueryBuilders.matchQuery("field1", "one"))
            .must(QueryBuilders.matchQuery("field1", "two"));

        assertResponse(prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH), response -> {
            Map<String, SearchProfileShardResult> p = response.getProfileResults();
            assertNotNull(p);
            assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

            for (Map.Entry<String, SearchProfileShardResult> shardResult : response.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertEquals(result.getQueryName(), "BooleanQuery");
                        assertEquals(result.getLuceneDescription(), "+field1:one +field1:two");
                        assertThat(result.getTime(), greaterThan(0L));
                        assertNotNull(result.getTimeBreakdown());
                        assertEquals(result.getProfiledChildren().size(), 2);

                        // Check the children
                        List<ProfileResult> children = result.getProfiledChildren();
                        assertEquals(children.size(), 2);

                        ProfileResult childProfile = children.get(0);
                        assertEquals(childProfile.getQueryName(), "TermQuery");
                        assertEquals(childProfile.getLuceneDescription(), "field1:one");
                        assertThat(childProfile.getTime(), greaterThan(0L));
                        assertNotNull(childProfile.getTimeBreakdown());
                        assertEquals(childProfile.getProfiledChildren().size(), 0);

                        childProfile = children.get(1);
                        assertEquals(childProfile.getQueryName(), "TermQuery");
                        assertEquals(childProfile.getLuceneDescription(), "field1:two");
                        assertThat(childProfile.getTime(), greaterThan(0L));
                        assertNotNull(childProfile.getTimeBreakdown());
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }
        });
    }

    /**
     * Tests a boolean query with no children clauses
     */
    public void testEmptyBool() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boolQuery();
        logger.info("Query: {}", q);

        assertResponse(prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH), response -> {
            assertNotNull("Profile response element should not be null", response.getProfileResults());
            assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

            for (Map.Entry<String, SearchProfileShardResult> shardResult : response.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                        assertNotNull(result.getTimeBreakdown());
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }
        });
    }

    /**
     * Tests a series of three nested boolean queries with a single "leaf" match query.
     * The rewrite process will "collapse" this down to a single bool, so this tests to make sure
     * nothing catastrophic happens during that fairly substantial rewrite
     */
    public void testCollapsingBool() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boolQuery()
            .must(QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("field1", "one"))));

        logger.info("Query: {}", q);

        assertResponse(prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH), response -> {
            assertNotNull("Profile response element should not be null", response.getProfileResults());
            assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

            for (Map.Entry<String, SearchProfileShardResult> shardResult : response.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                        assertNotNull(result.getTimeBreakdown());
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }
        });
    }

    public void testBoosting() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boostingQuery(QueryBuilders.matchQuery("field1", "one"), QueryBuilders.matchQuery("field1", "two"))
            .boost(randomFloat())
            .negativeBoost(randomFloat());
        logger.info("Query: {}", q);

        assertResponse(prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH), response -> {
            assertNotNull("Profile response element should not be null", response.getProfileResults());
            assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

            for (Map.Entry<String, SearchProfileShardResult> shardResult : response.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                        assertNotNull(result.getTimeBreakdown());
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }
        });
    }

    public void testDisMaxRange() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.disMaxQuery()
            .boost(0.33703882f)
            .add(QueryBuilders.rangeQuery("field2").from(null).to(73).includeLower(true).includeUpper(true));
        logger.info("Query: {}", q);

        assertResponse(prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH), response -> {
            assertNotNull("Profile response element should not be null", response.getProfileResults());
            assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

            for (Map.Entry<String, SearchProfileShardResult> shardResult : response.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                        assertNotNull(result.getTimeBreakdown());
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }
        });
    }

    public void testRange() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.rangeQuery("field2").from(0).to(5);

        logger.info("Query: {}", q.toString());

        assertResponse(prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH), response -> {
            assertNotNull("Profile response element should not be null", response.getProfileResults());
            assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

            for (Map.Entry<String, SearchProfileShardResult> shardResult : response.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                        assertNotNull(result.getTimeBreakdown());
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }
        });
    }

    public void testPhrase() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i))
                .setSource("field1", English.intToEnglish(i) + " " + English.intToEnglish(i + 1), "field2", i);
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.matchPhraseQuery("field1", "one two");

        logger.info("Query: {}", q);

        assertResponse(
            prepareSearch().setQuery(q).setIndices("test").setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH),
            response -> {
                if (response.getShardFailures().length > 0) {
                    for (ShardSearchFailure f : response.getShardFailures()) {
                        logger.error("Shard search failure: {}", f);
                    }
                    fail();
                }

                assertNotNull("Profile response element should not be null", response.getProfileResults());
                assertThat("Profile response should not be an empty array", response.getProfileResults().size(), not(0));

                for (Map.Entry<String, SearchProfileShardResult> shardResult : response.getProfileResults().entrySet()) {
                    for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                        for (ProfileResult result : searchProfiles.getQueryResults()) {
                            assertNotNull(result.getQueryName());
                            assertNotNull(result.getLuceneDescription());
                            assertThat(result.getTime(), greaterThan(0L));
                            assertNotNull(result.getTimeBreakdown());
                        }

                        CollectorResult result = searchProfiles.getCollectorResult();
                        assertThat(result.getName(), is(not(emptyOrNullString())));
                        assertThat(result.getTime(), greaterThan(0L));
                    }
                }
            }
        );
    }

    /**
     * This test makes sure no profile results are returned when profiling is disabled
     */
    public void testNoProfile() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = prepareIndex("test").setId(String.valueOf(i)).setSource("field1", English.intToEnglish(i), "field2", i);
        }

        indexRandom(true, docs);
        refresh();
        QueryBuilder q = QueryBuilders.rangeQuery("field2").from(0).to(5);

        logger.info("Query: {}", q);

        assertResponse(
            prepareSearch().setQuery(q).setProfile(false),
            response -> assertThat("Profile response element should be an empty map", response.getProfileResults().size(), equalTo(0))
        );
    }
}
