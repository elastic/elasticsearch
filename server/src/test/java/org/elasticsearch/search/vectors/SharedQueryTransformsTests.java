/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedQueryTransformsTests extends ESTestCase {

    private record Key(String name) implements SharedQueryTransforms.Key<String> {}

    private record ObjectKey(String name) implements SharedQueryTransforms.Key<Object> {}

    public void testComputesOncePerKey() {
        SharedQueryTransforms transforms = new SharedQueryTransforms();
        float[] query = VectorTestUtils.randomFloatVector(randomIntBetween(1, 16));
        AtomicInteger computed = new AtomicInteger();
        String a = transforms.computeIfAbsent(query, new Key("a"), () -> "a" + computed.incrementAndGet());
        assertEquals("a1", a);
        assertSame(a, transforms.computeIfAbsent(query, new Key("a"), () -> "a" + computed.incrementAndGet()));
        assertEquals("b2", transforms.computeIfAbsent(query, new Key("b"), () -> "b" + computed.incrementAndGet()));
        assertEquals(2, computed.get());
        assertEquals(2, transforms.size());
    }

    public void testConcurrentLeavesComputeOnce() {
        SharedQueryTransforms transforms = new SharedQueryTransforms();
        float[] query = VectorTestUtils.randomFloatVector(randomIntBetween(1, 16));
        int threads = randomIntBetween(2, 8);
        AtomicInteger computed = new AtomicInteger();
        Object[] results = new Object[threads];
        startInParallel(threads, i -> results[i] = transforms.computeIfAbsent(query, new ObjectKey("key"), () -> {
            computed.incrementAndGet();
            return new Object();
        }));
        assertEquals(1, computed.get());
        for (Object result : results) {
            assertSame(results[0], result);
        }
    }

    public void testComputingOneKeyDoesNotHoldUpAnother() {
        SharedQueryTransforms transforms = new SharedQueryTransforms();
        float[] query = VectorTestUtils.randomFloatVector(randomIntBetween(1, 16));
        CountDownLatch computing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        runInParallel(2, i -> {
            if (i == 0) {
                transforms.computeIfAbsent(query, new Key("slow"), () -> {
                    computing.countDown();
                    safeAwait(release);
                    return "slow";
                });
            } else {
                try {
                    safeAwait(computing);
                    // whatever bin of the map they share with "slow", other keys are computed while "slow" still is
                    for (int k = 0; k < 64; k++) {
                        String name = "other" + k;
                        assertEquals(name, transforms.computeIfAbsent(query, new Key(name), () -> name));
                    }
                } finally {
                    release.countDown();
                }
            }
        });
        assertEquals("slow", transforms.computeIfAbsent(query, new Key("slow"), () -> "again"));
    }

    public void testFailedComputationIsRetried() {
        SharedQueryTransforms transforms = new SharedQueryTransforms();
        float[] query = VectorTestUtils.randomFloatVector(randomIntBetween(1, 16));
        AtomicInteger calls = new AtomicInteger();
        expectThrows(IllegalStateException.class, () -> transforms.computeIfAbsent(query, new Key("key"), () -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("boom");
            }
            return "value";
        }));
        // the first caller's supplier is retried, so all suppliers for a key must compute the same value
        assertEquals("value", transforms.computeIfAbsent(query, new Key("key"), () -> "other"));
        assertEquals(2, calls.get());
    }

    public void testUseWithAnotherVectorTripsAssertion() {
        assumeTrue("checked by assertions only", Assertions.ENABLED);
        SharedQueryTransforms transforms = new SharedQueryTransforms();
        float[] query = VectorTestUtils.randomFloatVector(randomIntBetween(1, 16));
        transforms.computeIfAbsent(query, new Key("key"), () -> "value");
        // an equal copy is still another vector, as a preconditioned one would be
        float[] other = randomBoolean() ? query.clone() : VectorTestUtils.randomFloatVector(query.length);
        AssertionError e = expectThrows(AssertionError.class, () -> transforms.computeIfAbsent(other, new Key("key"), () -> "other"));
        assertEquals("query transformations shared with a search that uses another vector", e.getMessage());
    }
}
