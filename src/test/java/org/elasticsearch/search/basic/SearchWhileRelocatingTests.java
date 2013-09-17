/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.basic;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders.gaussDecayFunction;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class SearchWhileRelocatingTests extends AbstractIntegrationTest {

    @Test
    public void testSearchAndRelocateConcurrently() throws Exception {
        final int numShards = between(10, 20);
        String mapping = XContentFactory.jsonBuilder().
                startObject().
                    startObject("type").
                        startObject("properties").
                            startObject("loc").field("type", "geo_point").endObject().
                            startObject("test").field("type", "string").endObject().
                        endObject().
                    endObject()
                .endObject().string();
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0))
                .addMapping("type1", mapping).execute().actionGet();
        ensureYellow();
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        final int numDocs = between(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexBuilders.add(client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 21)
                                    .endObject().endObject()));
        }
        indexRandom(true, indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]));
        final int numIters = atLeast(3);
        for (int i = 0; i < numIters; i++) {
            allowNodes("test", between(1,3));
            client().admin().cluster().prepareReroute().get();
            final AtomicBoolean stop = new AtomicBoolean(false);
            final List<Throwable> thrownExceptions = new CopyOnWriteArrayList<Throwable>();
            final Thread t = new Thread() {
                public void run() {
                    final List<Float> lonlat = new ArrayList<Float>();
                    lonlat.add(new Float(20));
                    lonlat.add(new Float(11));
                    try {
                        while (!stop.get()) {
                            SearchResponse sr = client().search(
                                    searchRequest().searchType(SearchType.QUERY_THEN_FETCH).source(
                                            searchSource().size(numDocs).query(
                                                    functionScoreQuery(termQuery("test", "value"),
                                                            gaussDecayFunction("loc", lonlat, "1000km")).boostMode(
                                                            CombineFunction.MULT.getName())))).get();
                            assertHitCount(sr, (long) (numDocs));
                            final SearchHits sh = sr.getHits();
                            assertThat("Expected hits to be the same size the actual hits array", sh.getTotalHits(),
                                    equalTo((long) (sh.getHits().length)));
                        }
                    } catch (Throwable t) {
                        thrownExceptions.add(t);
                    }
                }
            };
            t.start();
            ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).execute().actionGet();
            stop.set(true);
            t.join();
            assertThat(resp.isTimedOut(), equalTo(false));
            assertThat("failed in iteration "+i, thrownExceptions, Matchers.emptyIterable());
        }
    }
}
