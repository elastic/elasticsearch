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

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class SearchWhileRelocatingTests extends ElasticsearchIntegrationTest {

    @LuceneTestCase.AwaitsFix(bugUrl = "problem with search searching on 1 shard (no replica), " +
            "and between getting the cluster state to do the search, and executing it, " +
            "the shard has fully relocated (moved from started on one node, to fully started on another node")
    @Test
    public void testSearchAndRelocateConcurrently0Replicas() throws Exception {
        testSearchAndRelocateConcurrently(0);
    }

    @TestLogging("org.elasticsearch.action.search.type:TRACE")
    @Test
    public void testSearchAndRelocateConcurrently1Replicas() throws Exception {
        testSearchAndRelocateConcurrently(1);
    }

    private void testSearchAndRelocateConcurrently(int numberOfReplicas) throws Exception {
        final int numShards = between(10, 20);
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", numShards).put("index.number_of_replicas", numberOfReplicas))
                .addMapping("type1", "loc", "type=geo_point", "test", "type=string").execute().actionGet();
        ensureGreen();
        List<IndexRequestBuilder> indexBuilders = new ArrayList<IndexRequestBuilder>();
        final int numDocs = between(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexBuilders.add(client().prepareIndex("test", "type", Integer.toString(i))
                    .setSource(
                            jsonBuilder().startObject().field("test", "value").startObject("loc").field("lat", 11).field("lon", 21)
                                    .endObject().endObject()));
        }
        indexRandom(true, indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]));
        assertHitCount(client().prepareSearch().get(), (long) (numDocs));
        final int numIters = atLeast(10);
        for (int i = 0; i < numIters; i++) {
            final AtomicBoolean stop = new AtomicBoolean(false);
            final List<Throwable> thrownExceptions = new CopyOnWriteArrayList<Throwable>();
            Thread[] threads = new Thread[atLeast(1)];
            for (int j = 0; j < threads.length; j++) {
                threads[j] = new Thread() {
                    public void run() {
                        try {
                            while (!stop.get()) {
                                SearchResponse sr = client().prepareSearch().setSize(numDocs).get();
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
            }
            for (int j = 0; j < threads.length; j++) {
                threads[j].start();
            }
            allowNodes("test", between(1, 3));
            client().admin().cluster().prepareReroute().get();
            ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForRelocatingShards(0).execute().actionGet();
            stop.set(true);
            for (int j = 0; j < threads.length; j++) {
                threads[j].join();
            }
            assertThat(resp.isTimedOut(), equalTo(false));

            if (!thrownExceptions.isEmpty()) {
                Client client = client();
                String verified = "POST SEARCH OK";
                for (int j = 0; j < 10; j++) {
                    if (client.prepareSearch().get().getHits().getTotalHits() != numDocs) {
                        verified = "POST SEARCH FAIL";
                        break;
                    }
                }

                assertThat("failed in iteration " + i + ", verification: " + verified, thrownExceptions, Matchers.emptyIterable());
            }
        }
    }
}
