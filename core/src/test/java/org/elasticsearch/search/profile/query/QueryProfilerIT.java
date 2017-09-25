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

package org.elasticsearch.search.profile.query;

import org.apache.lucene.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.profile.query.RandomQueryGenerator.randomQueryBuilder;
import static org.elasticsearch.test.hamcrest.DoubleMatcher.nearlyEqual;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isEmptyOrNullString;
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
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        List<String> stringFields = Arrays.asList("field1");
        List<String> numericFields = Arrays.asList("field2");

        indexRandom(true, docs);

        refresh();
        int iters = between(20, 100);
        for (int i = 0; i < iters; i++) {
            QueryBuilder q = randomQueryBuilder(stringFields, numericFields, numDocs, 3);
            logger.info("Query: {}", q);

            SearchResponse resp = client().prepareSearch()
                    .setQuery(q)
                    .setProfile(true)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .execute().actionGet();

            assertNotNull("Profile response element should not be null", resp.getProfileResults());
            assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));
            for (Map.Entry<String, ProfileShardResult> shard : resp.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shard.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                    }

                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), not(isEmptyOrNullString()));
                    assertThat(result.getTime(), greaterThan(0L));
                }
            }

        }
    }

    /**
     * This test generates 1-10 random queries and executes a profiled and non-profiled
     * search for each query.  It then does some basic sanity checking of score and hits
     * to make sure the profiling doesn't interfere with the hits being returned
     */
    public void testProfileMatchesRegular() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        List<String> stringFields = Arrays.asList("field1");
        List<String> numericFields = Arrays.asList("field2");

        indexRandom(true, docs);

        refresh();
        int iters = between(1, 10);
        for (int i = 0; i < iters; i++) {
            QueryBuilder q = randomQueryBuilder(stringFields, numericFields, numDocs, 3);
            logger.info("Query: {}", q);

            SearchRequestBuilder vanilla = client().prepareSearch("test")
                    .setQuery(q)
                    .setProfile(false)
                    .addSort("_id", SortOrder.ASC)
                    .setPreference("_primary")
                    .setSearchType(SearchType.QUERY_THEN_FETCH);

            SearchRequestBuilder profile = client().prepareSearch("test")
                    .setQuery(q)
                    .setProfile(true)
                    .addSort("_id", SortOrder.ASC)
                    .setPreference("_primary")
                    .setSearchType(SearchType.QUERY_THEN_FETCH);

            MultiSearchResponse.Item[] responses = client().prepareMultiSearch()
                    .add(vanilla)
                    .add(profile)
                    .execute().actionGet().getResponses();

            SearchResponse vanillaResponse = responses[0].getResponse();
            SearchResponse profileResponse = responses[1].getResponse();

            float vanillaMaxScore = vanillaResponse.getHits().getMaxScore();
            float profileMaxScore = profileResponse.getHits().getMaxScore();
            if (Float.isNaN(vanillaMaxScore)) {
                assertTrue("Vanilla maxScore is NaN but Profile is not [" + profileMaxScore + "]",
                        Float.isNaN(profileMaxScore));
            } else {
                assertTrue("Profile maxScore of [" + profileMaxScore + "] is not close to Vanilla maxScore [" + vanillaMaxScore + "]",
                        nearlyEqual(vanillaMaxScore, profileMaxScore, 0.001));
            }

            assertThat(
                    "Profile totalHits of [" + profileResponse.getHits().getTotalHits() + "] is not close to Vanilla totalHits ["
                            + vanillaResponse.getHits().getTotalHits() + "]",
                    vanillaResponse.getHits().getTotalHits(), equalTo(profileResponse.getHits().getTotalHits()));

            SearchHit[] vanillaHits = vanillaResponse.getHits().getHits();
            SearchHit[] profileHits = profileResponse.getHits().getHits();

            for (int j = 0; j < vanillaHits.length; j++) {
                assertThat("Profile hit #" + j + " has a different ID from Vanilla",
                    vanillaHits[j].getId(), equalTo(profileHits[j].getId()));
            }

        }
    }

    /**
     * This test verifies that the output is reasonable for a simple, non-nested query
     */
    public void testSimpleMatch() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);
        ensureGreen();

        QueryBuilder q = QueryBuilders.matchQuery("field1", "one");

        SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .execute().actionGet();

        Map<String, ProfileShardResult> p = resp.getProfileResults();
        assertNotNull(p);
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertEquals(result.getQueryName(), "TermQuery");
                    assertEquals(result.getLuceneDescription(), "field1:one");
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), not(isEmptyOrNullString()));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
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
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);

        QueryBuilder q = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("field1", "one"))
                .must(QueryBuilders.matchQuery("field1", "two"));

        SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .execute().actionGet();

        Map<String, ProfileShardResult> p = resp.getProfileResults();
        assertNotNull(p);
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
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
                assertThat(result.getName(), not(isEmptyOrNullString()));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }


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
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boolQuery();
        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .execute().actionGet();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), not(isEmptyOrNullString()));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
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
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boolQuery()
                .must(QueryBuilders.boolQuery().must(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("field1", "one"))));

        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .execute().actionGet();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), not(isEmptyOrNullString()));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testBoosting() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.boostingQuery(QueryBuilders.matchQuery("field1", "one"), QueryBuilders.matchQuery("field1", "two"))
                .boost(randomFloat())
                .negativeBoost(randomFloat());
        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .execute().actionGet();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), not(isEmptyOrNullString()));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testDisMaxRange() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.disMaxQuery()
                .boost(0.33703882f)
                .add(QueryBuilders.rangeQuery("field2").from(null).to(73).includeLower(true).includeUpper(true));
        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .execute().actionGet();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), not(isEmptyOrNullString()));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testRange() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.rangeQuery("field2").from(0).to(5);

        logger.info("Query: {}", q.toString());

        SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .execute().actionGet();

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), not(isEmptyOrNullString()));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
    }

    public void testPhrase() throws Exception {
        createIndex("test");
        ensureGreen();

        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i) + " " + English.intToEnglish(i+1),
                    "field2", i
            );
        }

        indexRandom(true, docs);

        refresh();

        QueryBuilder q = QueryBuilders.matchPhraseQuery("field1", "one two");

        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setIndices("test")
                .setTypes("type1")
                .setProfile(true)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .execute().actionGet();

        if (resp.getShardFailures().length > 0) {
            for (ShardSearchFailure f : resp.getShardFailures()) {
                logger.error("Shard search failure: {}", f);
            }
            fail();
        }

        assertNotNull("Profile response element should not be null", resp.getProfileResults());
        assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));

        for (Map.Entry<String, ProfileShardResult> shardResult : resp.getProfileResults().entrySet()) {
            for (QueryProfileShardResult searchProfiles : shardResult.getValue().getQueryProfileResults()) {
                for (ProfileResult result : searchProfiles.getQueryResults()) {
                    assertNotNull(result.getQueryName());
                    assertNotNull(result.getLuceneDescription());
                    assertThat(result.getTime(), greaterThan(0L));
                    assertNotNull(result.getTimeBreakdown());
                }

                CollectorResult result = searchProfiles.getCollectorResult();
                assertThat(result.getName(), not(isEmptyOrNullString()));
                assertThat(result.getTime(), greaterThan(0L));
            }
        }
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
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource(
                    "field1", English.intToEnglish(i),
                    "field2", i
            );
        }

        indexRandom(true, docs);
        refresh();
        QueryBuilder q = QueryBuilders.rangeQuery("field2").from(0).to(5);

        logger.info("Query: {}", q);

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(false).execute().actionGet();
        assertThat("Profile response element should be an empty map", resp.getProfileResults().size(), equalTo(0));
    }

}

