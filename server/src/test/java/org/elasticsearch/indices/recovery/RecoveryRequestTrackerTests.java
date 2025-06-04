/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.exception.ElasticsearchException;
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
                PlainActionFuture<Void> future = new PlainActionFuture<>();
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
