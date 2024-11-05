/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.basic;

import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.formatShardStatus;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SearchWhileRelocatingIT extends ESIntegTestCase {

    public void testSearchAndRelocateConcurrentlyRandomReplicas() throws Exception {
        testSearchAndRelocateConcurrently(randomIntBetween(0, 1));
    }

    private void testSearchAndRelocateConcurrently(final int numberOfReplicas) throws Exception {
        final int numShards = between(1, 20);
        indicesAdmin().prepareCreate("test")
            .setSettings(indexSettings(numShards, numberOfReplicas))
            .setMapping("loc", "type=geo_point", "test", "type=text")
            .get();
        ensureGreen();
        List<IndexRequestBuilder> indexBuilders = new ArrayList<>();
        final int numDocs = between(10, 20);
        for (int i = 0; i < numDocs; i++) {
            indexBuilders.add(
                prepareIndex("test").setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field("test", "value")
                            .startObject("loc")
                            .field("lat", 11)
                            .field("lon", 21)
                            .endObject()
                            .endObject()
                    )
            );
        }
        indexRandom(true, indexBuilders.toArray(new IndexRequestBuilder[indexBuilders.size()]));
        assertHitCount(prepareSearch(), (numDocs));
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
                            while (stop.get() == false) {
                                assertResponse(prepareSearch().setSize(numDocs), response -> {
                                    if (response.getHits().getTotalHits().value() != numDocs) {
                                        // if we did not search all shards but had no serious failures that is potentially fine
                                        // if only the hit-count is wrong. this can happen if the cluster-state is behind when the
                                        // request comes in. It's a small window but a known limitation.
                                        if (response.getTotalShards() != response.getSuccessfulShards()
                                            && Stream.of(response.getShardFailures())
                                                .allMatch(ssf -> ssf.getCause() instanceof NoShardAvailableActionException)) {
                                            nonCriticalExceptions.add(
                                                "Count is "
                                                    + response.getHits().getTotalHits().value()
                                                    + " but "
                                                    + numDocs
                                                    + " was expected. "
                                                    + formatShardStatus(response)
                                            );
                                        } else {
                                            assertHitCount(response, numDocs);
                                        }
                                    }

                                    final SearchHits sh = response.getHits();
                                    assertThat(
                                        "Expected hits to be the same size the actual hits array",
                                        sh.getTotalHits().value(),
                                        equalTo((long) (sh.getHits().length))
                                    );
                                });
                                // this is the more critical but that we hit the actual hit array has a different size than the
                                // actual number of hits.
                            }
                        } catch (SearchPhaseExecutionException ex) {
                            // it's possible that all shards fail if we have a small number of shards.
                            if (ex.getMessage().contains("all shards failed") == false) {
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
            ClusterRerouteUtils.reroute(client());
            stop.set(true);
            for (int j = 0; j < threads.length; j++) {
                threads[j].join();
            }
            // this might time out on some machines if they are really busy and you hit lots of throttling
            ClusterHealthResponse resp = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                .setWaitForYellowStatus()
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .setTimeout(TimeValue.timeValueMinutes(5))
                .get();
            assertNoTimeout(resp);
            // if we hit only non-critical exceptions we make sure that the post search works
            if (nonCriticalExceptions.isEmpty() == false) {
                logger.info("non-critical exceptions: {}", nonCriticalExceptions);
                for (int j = 0; j < 10; j++) {
                    assertHitCount(prepareSearch(), numDocs);
                }
            }
        }
    }
}
