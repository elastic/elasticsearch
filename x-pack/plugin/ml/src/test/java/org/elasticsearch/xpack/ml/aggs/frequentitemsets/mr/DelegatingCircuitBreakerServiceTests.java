/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets.mr;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.PreallocatedCircuitBreakerService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING;

public class DelegatingCircuitBreakerServiceTests extends ESTestCase {

    /**
     * the purpose of the test is not hitting the `IllegalStateException("already closed")` in
     * PreallocatedCircuitBreaker#addEstimateBytesAndMaybeBreak in {@link PreallocatedCircuitBreakerService}
     */
    public void testThreadedExecution() {

        try (
            HierarchyCircuitBreakerService topBreaker = new HierarchyCircuitBreakerService(
                Settings.builder()
                    .put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "10mb")
                    // Disable the real memory checking because it causes other tests to interfere with this one.
                    .put(USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                    .build(),
                List.of(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            )
        ) {
            PreallocatedCircuitBreakerService preallocated = new PreallocatedCircuitBreakerService(
                topBreaker,
                CircuitBreaker.REQUEST,
                10_000,
                "test"
            );

            CircuitBreaker breaker = preallocated.getBreaker(CircuitBreaker.REQUEST);

            DelegatingCircuitBreakerService delegatingCircuitBreakerService = new DelegatingCircuitBreakerService(breaker, (bytes -> {
                breaker.addEstimateBytesAndMaybeBreak(bytes, "test");
            }));

            Thread consumerThread = new Thread(() -> {
                for (int i = 0; i < 100; ++i) {
                    delegatingCircuitBreakerService.getBreaker("ignored").addEstimateBytesAndMaybeBreak(i % 2 == 0 ? 10 : -10, "ignored");
                }
            });

            final Thread producerThread = new Thread(() -> {
                delegatingCircuitBreakerService.disconnect();
                preallocated.close();
            });
            consumerThread.start();
            producerThread.start();
            consumerThread.join();
            producerThread.join();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
    }
}
