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

package org.elasticsearch.index.search;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class MatchQueryIT extends ESIntegTestCase {
    private static final String INDEX = "test";

    /**
     * Test setup.
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        CreateIndexRequestBuilder builder = prepareCreate(INDEX).setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.analysis.filter.syns.type", "synonym")
                .putList("index.analysis.filter.syns.synonyms", "wtf, what the fudge", "foo, bar baz")
                .put("index.analysis.analyzer.lower_syns.type", "custom")
                .put("index.analysis.analyzer.lower_syns.tokenizer", "standard")
                .putList("index.analysis.analyzer.lower_syns.filter", "lowercase", "syns")
                .put("index.analysis.filter.graphsyns.type", "synonym_graph")
                .putList("index.analysis.filter.graphsyns.synonyms", "wtf, what the fudge", "foo, bar baz")
                .put("index.analysis.analyzer.lower_graphsyns.type", "custom")
                .put("index.analysis.analyzer.lower_graphsyns.tokenizer", "standard")
                .putList("index.analysis.analyzer.lower_graphsyns.filter", "lowercase", "graphsyns")
        );

        assertAcked(builder.addMapping(INDEX, createMapping()));
        ensureGreen();
    }

    private List<IndexRequestBuilder> getDocs() {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("test", "test", "1").setSource("field", "say wtf happened foo"));
        builders.add(client().prepareIndex("test", "test", "2").setSource("field", "bar baz what the fudge man"));
        builders.add(client().prepareIndex("test", "test", "3").setSource("field", "wtf"));
        builders.add(client().prepareIndex("test", "test", "4").setSource("field", "what is the name for fudge"));
        builders.add(client().prepareIndex("test", "test", "5").setSource("field", "bar two three"));
        builders.add(client().prepareIndex("test", "test", "6").setSource("field", "bar baz two three"));

        return builders;
    }

    /**
     * Setup the index mappings for the test index.
     *
     * @return the json builder with the index mappings
     * @throws IOException on error creating mapping json
     */
    private XContentBuilder createMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
                .startObject(INDEX)
                    .startObject("properties")
                        .startObject("field")
                            .field("type", "text")
                        .endObject()
                    .endObject()
                .endObject()
            .endObject();
    }

    public void testSimpleMultiTermPhrase() throws ExecutionException, InterruptedException {
        indexRandom(true, false, getDocs());

        // first search using regular synonym field using phrase
        SearchResponse searchResponse = client().prepareSearch(INDEX)
            .setQuery(QueryBuilders.matchPhraseQuery("field", "foo two three").analyzer("lower_syns")).get();

        // because foo -> "bar baz" where "foo" and "bar" at position 0, "baz" and "two" at position 1.
        // "bar two three", "bar baz three", "foo two three", "foo baz three"
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "5"); // we should not match this but we do

        // same query using graph should find correct result
        searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.matchPhraseQuery("field", "foo two three")
            .analyzer("lower_graphsyns")).get();

        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "6");
    }

    public void testSimpleMultiTermAnd() throws ExecutionException, InterruptedException {
        indexRandom(true, false, getDocs());

        // first search using regular synonym field using phrase
        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery("field", "say what the fudge")
            .operator(Operator.AND).analyzer("lower_syns")).get();

        // Old synonyms work fine in that case, but it is coincidental
        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");

        // same query using graph should find correct result
        searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery("field", "say what the fudge")
            .operator(Operator.AND).analyzer("lower_graphsyns")).get();

        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");
    }

    public void testMinShouldMatch() throws ExecutionException, InterruptedException {
        indexRandom(true, false, getDocs());

        // no min should match
        SearchResponse searchResponse = client().prepareSearch(INDEX)
            .setQuery(
                QueryBuilders.matchQuery("field", "three what the fudge foo")
                    .operator(Operator.OR).analyzer("lower_graphsyns").autoGenerateSynonymsPhraseQuery(false)
            )
            .get();

        assertHitCount(searchResponse, 6L);
        assertSearchHits(searchResponse, "1", "2", "3", "4", "5", "6");

        // same query, with min_should_match of 2
        searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.matchQuery("field", "three what the fudge foo")
            .operator(Operator.OR).analyzer("lower_graphsyns").minimumShouldMatch("80%")).get();

        // three wtf foo = 2 terms, match #1
        // three wtf bar baz = 3 terms, match #6
        // three what the fudge foo = 4 terms, no match
        // three what the fudge bar baz = 4 terms, match #2
        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse, "1", "2", "6");
    }

    public void testMultiTermsSynonymsPhrase() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> builders = getDocs();
        indexRandom(true, false, builders);
        SearchResponse searchResponse = client().prepareSearch(INDEX)
            .setQuery(
                QueryBuilders.matchQuery("field", "wtf")
                    .analyzer("lower_graphsyns")
                    .operator(Operator.AND))
            .get();
        assertHitCount(searchResponse, 3L);
        assertSearchHits(searchResponse, "1", "2", "3");
    }

    public void testPhrasePrefix() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> builders = getDocs();
        builders.add(client().prepareIndex("test", "test", "7").setSource("field", "WTFD!"));
        builders.add(client().prepareIndex("test", "test", "8").setSource("field", "Weird Al's WHAT THE FUDGESICLE"));
        indexRandom(true, false, builders);

        SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(QueryBuilders.matchPhrasePrefixQuery("field", "wtf")
            .analyzer("lower_graphsyns")).get();

        assertHitCount(searchResponse, 5L);
        assertSearchHits(searchResponse, "1", "2", "3", "7", "8");
    }

    public void testCommonTerms() throws ExecutionException, InterruptedException {
        String route = "commonTermsTest";
        List<IndexRequestBuilder> builders = getDocs();
        for (IndexRequestBuilder indexRequet : builders) {
            // route all docs to same shard for this test
            indexRequet.setRouting(route);
        }
        indexRandom(true, false, builders);

        // do a search with no cutoff frequency to show which docs should match
        SearchResponse searchResponse = client().prepareSearch(INDEX)
            .setRouting(route)
            .setQuery(QueryBuilders.matchQuery("field", "bar three happened")
                .operator(Operator.OR)).get();

        assertHitCount(searchResponse, 4L);
        assertSearchHits(searchResponse, "1", "2", "5", "6");

        // do same search with cutoff and see less documents match
        // in this case, essentially everything but "happened" gets excluded
        searchResponse = client().prepareSearch(INDEX)
            .setRouting(route)
            .setQuery(QueryBuilders.matchQuery("field", "bar three happened")
                .operator(Operator.OR).cutoffFrequency(1f)).get();

        assertHitCount(searchResponse, 1L);
        assertSearchHits(searchResponse, "1");
    }
}
