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

package org.elasticsearch.search.profile;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;


import org.apache.lucene.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.*;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.query.RandomQueryGenerator.randomQueryBuilder;


@ElasticsearchIntegrationTest.ClusterScope(scope=ElasticsearchIntegrationTest.Scope.TEST)
public class ProfileQueryTests extends ElasticsearchIntegrationTest {


    @Test
    /**
     * This test simply checks to make sure nothing crashes.  Unsure how best to validate the Profile response...
     */
    public void testProfileQuery() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
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
            logger.info(q.toString());

            SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
            assertNotNull("Profile response element should not be null", resp.getProfile());

        }
    }

    @Test
    /**
     * This test simply checks to make sure nothing crashes.  Unsure how best to validate the Profile response...
     */
    public void testProfileMatchesRegular() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
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
            logger.info(q.toString());

            SearchResponse vanilla = client().prepareSearch().setQuery(q).setProfile(false).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
            SearchResponse profile = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();

            assertTrue("Profile maxScore of [" + profile.getHits().getMaxScore() + "] does not match Vanilla maxScore [" + vanilla.getHits().getMaxScore() + "]", vanilla.getHits().getMaxScore() == profile.getHits().getMaxScore());
            assertTrue("Profile totalHits of [" + profile.getHits().totalHits() + "] does not match Vanilla totalHist [" + vanilla.getHits().totalHits() + "]", Float.compare(vanilla.getHits().getTotalHits(), profile.getHits().getTotalHits()) == 0);

            SearchHit[] vanillaHits = vanilla.getHits().getHits();
            SearchHit[] profileHits = profile.getHits().getHits();

            for (int j = 0; j < vanillaHits.length; j++) {
                assertTrue("Profile hit #" + j + " has a different ID from Vanilla", vanillaHits[j].getId().equals(profileHits[j].getId()));
                assertTrue("Profile hit #" + j + " has a different score from Vanilla", Float.compare(vanillaHits[j].getScore(),profileHits[j].getScore()) == 0);
            }

        }
    }

    @Test
    /**
     * This test verifies that the output is reasonable (timing is equal, etc) for a simple, non-nested query
     */
    public void testSimpleMatch() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
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

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
        Profile p = resp.getProfile();
        assertNotNull(p);
        assertEquals(p.getComponents().size(), 0);
        assertEquals(p.getClassName(), "TermQuery");
        assertEquals(p.getLuceneDetails(), "field1:one");
        assertTrue(p.time() > 0);
        assertEquals(p.time(), p.totalTime());
    }

    @Test
    /**
     * This test verifies that the output is reasonable (non-zero times, etc) for a nested query
     */
    public void testBool() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
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

        QueryBuilder q = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("field1", "one")).must(QueryBuilders.matchQuery("field1", "two"));

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).execute().actionGet();
        Profile p = resp.getProfile();

        assertNotNull(p);
        assertEquals(p.getComponents().size(), 2);
        assertEquals(p.getClassName(), "BooleanQuery");
        assertEquals(p.getLuceneDetails(), "+field1:one +field1:two");
        assertEquals(p.time(), p.totalTime());
        assertTrue(p.time() > 0);


        Profile first = p.getComponents().get(0);
        assertEquals(first.getComponents().size(), 0);
        assertEquals(first.getClassName(), "TermQuery");
        assertEquals(first.getLuceneDetails(), "field1:one");
        assertEquals(first.totalTime(), p.totalTime());
        assertTrue(first.time() < first.totalTime());

        Profile second = p.getComponents().get(1);
        assertEquals(second.getComponents().size(), 0);
        assertEquals(second.getClassName(), "TermQuery");
        assertEquals(second.getLuceneDetails(), "field1:two");
        assertEquals(second.totalTime(), p.totalTime());
        assertTrue(second.time() < first.totalTime());
    }

    @Test
    public void testEmptyBool() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
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
        logger.info(q.toString());

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
        assertNotNull("Profile response element should not be null", resp.getProfile());


    }

    @Test
    public void testBoosting() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
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

        QueryBuilder q = QueryBuilders.boostingQuery().boost(randomFloat())
                .positive(QueryBuilders.matchQuery("field1", "one"))
                .negativeBoost(randomFloat())
                .negative(QueryBuilders.matchQuery("field1", "two"));
        logger.info(q.toString());

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
        assertNotNull("Profile response element should not be null", resp.getProfile());


    }

    @Test
    public void testDisMaxRange() throws Exception {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(indexSettings());
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
        logger.info(q.toString());

        SearchResponse resp = client().prepareSearch().setQuery(q).setProfile(true).setSearchType(SearchType.QUERY_THEN_FETCH).execute().actionGet();
        assertNotNull("Profile response element should not be null", resp.getProfile());

    }

}


