/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public class RecoveryRequestTrackerTests extends ESTestCase {


    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testIdempotencyIsEnforced() {
        Set<Long> seqNosReturned = ConcurrentCollections.newConcurrentSet();
        ConcurrentMap<Long, Set<PlainActionFuture<Void>>> seqToResult = ConcurrentCollections.newConcurrentMap();

        RecoveryRequestTracker requestTracker = new RecoveryRequestTracker();

        int numberOfRequests = randomIntBetween(100, 200);
        for (int j = 0; j < numberOfRequests; ++j) {
            final long seqNo = j;
            int iterations = randomIntBetween(2, 5);
            for (int i = 0; i < iterations; ++i) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                Set<PlainActionFuture<Void>> set = seqToResult.computeIfAbsent(seqNo, (k) -> ConcurrentCollections.newConcurrentSet());
                set.add(future);
                threadPool.generic().execute(() -> {
                    ActionListener<Void> listener = requestTracker.markReceivedAndCreateListener(seqNo, future);
                    if (listener != null) {
                        boolean added = seqNosReturned.add(seqNo);
                        // Ensure that we only return 1 future per sequence number
                        assertTrue(added);
                        if (rarely()) {
                            listener.onFailure(new ElasticsearchException(randomAlphaOfLength(10)));
                        } else {
                            listener.onResponse(null);
                        }
                    }
                });
            }
        }

        seqToResult.values().stream().flatMap(Collection::stream).forEach(f -> {
            try {
                f.actionGet();
            } catch (Exception e) {
                // Ignore for now. We will assert later.
            }
        });

        for (Set<PlainActionFuture<Void>> value : seqToResult.values()) {
            Optional<PlainActionFuture<Void>> first = value.stream().findFirst();
            assertTrue(first.isPresent());
            Exception expectedException = null;
            try {
                first.get().actionGet();
            } catch (Exception e) {
                expectedException = e;
            }
            for (PlainActionFuture<Void> future : value) {
                assertTrue(future.isDone());
                if (expectedException == null) {
                    future.actionGet();
                } else {
                    try {
                        future.actionGet();
                        fail("expected exception");
                    } catch (Exception e) {
                        assertEquals(expectedException.getMessage(), e.getMessage());
                    }
                }
            }
        }
    }
}
