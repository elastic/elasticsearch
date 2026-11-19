/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

/**
 * Computing each transformation once is {@link org.elasticsearch.common.util.CachedSupplier}'s job, tested with it; this
 * tests what {@link SharedQueryTransforms} adds on top.
 */
public class SharedQueryTransformsTests extends ESTestCase {

    private record Key(String name) implements SharedQueryTransforms.Key<String> {}

    public void testPerQuerySharesByKey() {
        SharedQueryTransforms transforms = SharedQueryTransforms.perQuery();
        assertThat(transforms.computeIfAbsent(new Key("a"), () -> "a"), equalTo("a"));
        assertThat("computed once for an equal key", transforms.computeIfAbsent(new Key("a"), () -> "other"), equalTo("a"));
        assertThat(transforms.computeIfAbsent(new Key("b"), () -> "b"), equalTo("b"));
    }

    public void testNoneSharesNothing() {
        AtomicInteger computed = new AtomicInteger();
        for (int i = 0; i < 3; i++) {
            SharedQueryTransforms.NONE.computeIfAbsent(new Key("a"), () -> "a" + computed.incrementAndGet());
        }
        assertThat(computed.get(), equalTo(3));
    }

    public void testComputingOneKeyDoesNotHoldUpAnother() {
        SharedQueryTransforms transforms = SharedQueryTransforms.perQuery();
        CountDownLatch computing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        runInParallel(2, i -> {
            if (i == 0) {
                transforms.computeIfAbsent(new Key("slow"), () -> {
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
                        assertThat(transforms.computeIfAbsent(new Key(name), () -> name), equalTo(name));
                    }
                } finally {
                    release.countDown();
                }
            }
        });
        assertThat(transforms.computeIfAbsent(new Key("slow"), () -> "again"), equalTo("slow"));
    }
}
