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

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SearchWhileRelocatingTests extends ElasticsearchIntegrationTest {

// @LuceneTestCase.AwaitsFix(bugUrl = "problem with search searching on 1 shard (no replica), " +
//   "and between getting the cluster state to do the search, and executing it, " +
//   "the shard has fully relocated (moved from started on one node, to fully started on another node")
//   ^^ the current impl of the test handles this case gracefully since it can happen with 1 replica as well
//   we just make sure if we get a partial result without a failure that the postsearch is ok!
    @Test
    @Nightly
    public void testSearchAndRelocateConcurrently0Replicas() throws Exception {
        testSearchAndRelocateConcurrently(0);
    }

    @Test
    @Nightly
    public void testSearchAndRelocateConcurrently1Replicas() throws Exception {
        testSearchAndRelocateConcurrently(1);
    }

    @Test
    public void testSearchAndRelocateConcurrentlyRanodmReplicas() throws Exception {
        testSearchAndRelocateConcurrently(randomIntBetween(0, 1));
    }

    private void testSearchAndRelocateConcurrently(final int numberOfReplicas) throws Exception {
        final int numShards = between(1, 20);
        client().admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", numShards).put("index.number_of_replicas", numberOfReplicas))
                .addMapping("type1", "loc", "type=geo_point", "test", "type=string").execute().actionGet();
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
        assertHitCount(client().prepareSearch().get(), (long) (numDocs));
        final int numIters = scaledRandomIntBetween(5, 20);
        for (int i = 0; i < numIters; i++) {
            final AtomicBoolean stop = new AtomicBoolean(false);
            final List<Throwable> thrownExceptions = new CopyOnWriteArrayList<>();
            final List<Throwable> nonCriticalExceptions = new CopyOnWriteArrayList<>();

            Thread[] threads = new Thread[scaledRandomIntBetween(1, 3)];
            for (int j = 0; j < threads.length; j++) {
                threads[j] = new Thread() {
                    @Override
                    public void run() {
                        boolean criticalException = true;
                        try {
                            while (!stop.get()) {
                                SearchResponse sr = client().prepareSearch().setSize(numDocs).get();
                                // if we did not search all shards but had no failures that is potentially fine
                                // if only the hit-count is wrong. this can happen if the cluster-state is behind when the
                                // request comes in. It's a small window but a known limitation.
                                //
                                criticalException = sr.getTotalShards() == sr.getSuccessfulShards() || sr.getFailedShards() > 0;
                                assertHitCount(sr, (long) (numDocs));
                                criticalException = true;
                                final SearchHits sh = sr.getHits();
                                assertThat("Expected hits to be the same size the actual hits array", sh.getTotalHits(),
                                        equalTo((long) (sh.getHits().length)));
                                // this is the more critical but that we hit the actual hit array has a different size than the
                                // actual number of hits.
                            }
                        } catch (SearchPhaseExecutionException ex) {
                            // it's possible that all shards fail if we have a small number of shards.
                            // with replicas this should not happen
                            if (numberOfReplicas == 1 || !ex.getMessage().contains("all shards failed")) {
                                thrownExceptions.add(ex);
                            }
                        } catch (Throwable t) {
                            if (!criticalException) {
                                nonCriticalExceptions.add(t);
                            } else {
                                thrownExceptions.add(t);
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
            ClusterHealthResponse resp = client().admin().cluster().prepareHealth().setWaitForYellowStatus().setWaitForRelocatingShards(0).setWaitForEvents(Priority.LANGUID).setTimeout("5m").get();
            assertNoTimeout(resp);
            if (!thrownExceptions.isEmpty() || !nonCriticalExceptions.isEmpty()) {
                Client client = client();
                boolean postSearchOK = true;
                String verified = "POST SEARCH OK";
                for (int j = 0; j < 10; j++) {
                    if (client.prepareSearch().get().getHits().getTotalHits() != numDocs) {
                        verified = "POST SEARCH FAIL";
                        postSearchOK = false;
                        break;
                    }
                }
                assertThat("numberOfReplicas: " + numberOfReplicas + " failed in iteration " + i + ", verification: " + verified, thrownExceptions, Matchers.emptyIterable());
                // if we hit only non-critical exceptions we only make sure that the post search works
                logger.info("Non-CriticalExceptions: " + nonCriticalExceptions.toString());
                assertThat("numberOfReplicas: " + numberOfReplicas + " failed in iteration " + i + ", verification: " + verified, postSearchOK, is(true));
            }
        }
    }
}
