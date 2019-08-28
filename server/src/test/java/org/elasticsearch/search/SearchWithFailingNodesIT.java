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

package org.elasticsearch.search;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;


/**
 * This test is a disruption style test that restarts data nodes to see if search behaves well under extreme conditions.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, minNumDataNodes = 2)
public class SearchWithFailingNodesIT extends ESIntegTestCase {

    public void testDisallowPartialsWithRedStateRecovering() throws Exception {
        int docCount = scaledRandomIntBetween(10, 1000);
        logger.info("Using docCount [{}]", docCount);
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", cluster().numDataNodes() + 2).put("index.number_of_replicas", 1)));
        ensureGreen();
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex("test", "_doc", ""+i).setSource("field1", i).get();
        }
        refresh("test");

        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> searchThreads = new ArrayList<>();
        // this is a little extreme, but necessary to provoke spectacular timings like hitting a recovery on a replica
        for (int i = 0; i < 100; ++i) {
            Thread searchThread = new Thread() {
                {
                    setDaemon(true);
                }

                @Override
                public void run() {
                    while (stop.get() == false) {
                        // todo: the timeouts below should not be necessary, but this test sometimes hangs without, to be fixed (or
                        //  explained)
                        verify(() -> client().prepareSearch("test").setQuery(new RangeQueryBuilder("field1").gte(0))
                            .setSize(100).setAllowPartialSearchResults(false).get(TimeValue.timeValueSeconds(10)));
                        verify(() -> client().prepareSearch("test")
                            .setSize(100).setAllowPartialSearchResults(false).get(TimeValue.timeValueSeconds(10)));
                    }
                }

                void verify(Supplier<SearchResponse> call) {
                    try {
                        SearchResponse response = call.get();
                        assertThat(response.getHits().getHits().length, equalTo(Math.min(100, docCount)));
                        assertThat(response.getHits().getTotalHits().value, equalTo((long) docCount));
                    } catch (Exception e) {
                        // this is OK.
                        logger.info("Failed with : " + e);
                    }
                }
            };
            searchThreads.add(searchThread);
            searchThread.start();
        }
        try {
            // have two threads do restarts, with a replica of 1, this means we will sometimes have no copies (RED)
            Thread restartThread = new Thread() {
                {
                    setDaemon(true);
                }

                @Override
                public void run() {
                    try {
                        for (int i = 0; i < 5; ++i) {
                            internalCluster().restartRandomDataNode();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            restartThread.start();
            for (int i = 0; i < 5; ++i) {
                internalCluster().restartRandomDataNode();
            }
            restartThread.join(30000);
            assertFalse(restartThread.isAlive());
        } finally {
            stop.set(true);
            searchThreads.forEach(thread -> {
                try {
                    thread.join(30000);
                    if (thread.isAlive()) {
                        logger.warn("Thread: " + thread + " is still alive");
                        // do not continue unless thread terminates to avoid getting other confusing test errors. Please kill me...
                        thread.join();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // hack to ensure all search contexts are removed, seems we risk leaked search contexts when coordinator dies.
        client().admin().indices().prepareDelete("test").get();
    }


}
