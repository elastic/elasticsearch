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

package org.elasticsearch.search.basic;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.formatShardStatus;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchWhileRelocatingIT extends ESIntegTestCase {

    public void testSearchAndRelocateConcurrentlyRandomReplicas() throws Exception {
        testSearchAndRelocateConcurrently(randomIntBetween(0, 1));
    }

    private void testSearchAndRelocateConcurrently(final int numberOfReplicas) throws Exception {
        final int numShards = between(1, 20);
        client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", numberOfReplicas))
                .addMapping("type", "loc", "type=geo_point", "test", "type=text").get();
        ensureGreen();
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        final int numDocs = between(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexBuilders.add(client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 21)
                                    .endObject().endObject()));
        }
        indexRandom(true, indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]));
        assertHitCount(client().prepareSearch().get(), (numDocs));
        final int numIters = scaledRandomIntBetween(5, 20);
        for (int i = 0; i < numIters; i++) {
            final AtomicBoolean stop = new AtomicBoolean(false);
            final List<String> nonCriticalExceptions = new CopyOnWriteArrayList<>();

            Thread[] threads = new Thread[scaledRandomIntBetween(1, 3)];
            for (int j = 0; j < threads.length; j++) {
                threads[j] = new Thread() {
                    @Override
                    public void run() {
                        try {
                            while (!stop.get()) {
                                SearchResponse sr = client().prepareSearch().setSize(numDocs).get();
                                if (sr.getHits().getTotalHits().value != numDocs) {
                                    // if we did not search all shards but had no failures that is potentially fine
                                    // if only the hit-count is wrong. this can happen if the cluster-state is behind when the
                                    // request comes in. It's a small window but a known limitation.
                                    if (sr.getTotalShards() != sr.getSuccessfulShards() && sr.getFailedShards() == 0) {
                                        nonCriticalExceptions.add("Count is " + sr.getHits().getTotalHits().value + " but " + numDocs +
                                            " was expected. " + formatShardStatus(sr));
                                    } else {
                                        assertHitCount(sr, numDocs);
                                    }
                                }

                                final SearchHits sh = sr.getHits();
                                assertThat("Expected hits to be the same size the actual hits array", sh.getTotalHits().value,
                                        equalTo((long) (sh.getHits().length)));
                                // this is the more critical but that we hit the actual hit array has a different size than the
                                // actual number of hits.
                            }
                        } catch (SearchPhaseExecutionException ex) {
                            // it's possible that all shards fail if we have a small number of shards.
                            // with replicas this should not happen
                            if (numberOfReplicas == 1 || !ex.getMessage().contains("all shards failed")) {
                                throw ex;
                            }
                        }
                    }
                };
            }
            for (int j = 0; j < threads.length; j++) {
                threads[j].start();
            }
            allowNodes("test", between(1, 3));
            client().admin().cluster().prepareReroute().get();
            stop.set(true);
            for (int j = 0; j < threads.length; j++) {
                threads[j].join();
            }
            // this might time out on some machines if they are really busy and you hit lots of throttling
            ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForYellowStatus()
                    .setWaitForNoRelocatingShards(true).setWaitForEvents(Priority.LANGUID).setTimeout("5m").get();
            assertNoTimeout(resp);
            // if we hit only non-critical exceptions we make sure that the post search works
            if (!nonCriticalExceptions.isEmpty()) {
                logger.info("non-critical exceptions: {}", nonCriticalExceptions);
                for (int j = 0; j < 10; j++) {
                    assertHitCount(client().prepareSearch().get(), numDocs);
                }
            }
        }
    }
}
