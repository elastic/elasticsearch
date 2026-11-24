/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BreakerTestUtil {
    private static final Logger logger = LogManager.getLogger(BreakerTestUtil.class);

    /**
     * Performs a binary search between 0 and {@code tooBigToBreak} bytes for the largest memory size
     * that'll cause the closure parameter to throw a {@link CircuitBreakingException}.
     */
    public static ByteSizeValue findBreakerLimit(ByteSizeValue tooBigToBreak, CheckedConsumer<ByteSizeValue, Exception> c)
        throws Exception {
        // Validate arguments: we don't throw for tooBigToBreak and we *do* throw for 0.
        try {
            c.accept(tooBigToBreak);
        } catch (CircuitBreakingException e) {
            throw new IllegalArgumentException("expected runnable *not* to break under tooBigToBreak", e);
        }
        try {
            c.accept(ByteSizeValue.ofBytes(0));
            throw new IllegalArgumentException("expected runnable to break under a limit of 0 bytes");
        } catch (CircuitBreakingException e) {
            // desired
        }

        // Perform the actual binary search
        long l = findBreakerLimit(0, tooBigToBreak.getBytes(), c);

        // Validate results: we *do* throw for limit, we don't throw for limit + 1
        ByteSizeValue limit = ByteSizeValue.ofBytes(l);
        ByteSizeValue onePastLimit = ByteSizeValue.ofBytes(l + 1);
        try {
            c.accept(limit);
            throw new IllegalArgumentException("expected runnable to break under a limit of " + limit + " bytes");
        } catch (CircuitBreakingException e) {
            // desired
        }
        try {
            c.accept(onePastLimit);
        } catch (CircuitBreakingException e) {
            throw new IllegalArgumentException("expected runnable to *not* break under a limit of " + onePastLimit + " bytes");
        }
        return limit;
    }

    /**
     * The bytes the request breakers of all nodes hold for parsed requests, from the held memory metric. Every node must
     * run {@link TestTelemetryPlugin} and a real request breaker, since the test cluster can make it a noop one that
     * charges nothing.
     */
    public static long parsedRequestHeldBytes(InternalTestCluster cluster) {
        return parsedRequestMeasurements(cluster).stream().mapToLong(Measurement::getLong).sum();
    }

    /**
     * The held memory measurements of the request breakers of all nodes for parsed requests. A new measurement is
     * recorded each time a charge is added or released, so a growing count shows a request was charged even when the
     * held bytes are back to zero. Has the same needs as {@link #parsedRequestHeldBytes}.
     */
    public static List<Measurement> parsedRequestMeasurements(InternalTestCluster cluster) {
        List<Measurement> measurements = new ArrayList<>();
        for (String node : cluster.getNodeNames()) {
            TestTelemetryPlugin telemetry = cluster.getInstance(PluginsService.class, node)
                .filterPlugins(TestTelemetryPlugin.class)
                .findFirst()
                .orElseThrow();
            for (Measurement measurement : telemetry.getLongUpDownCounterMeasurement(CircuitBreakerMetrics.ES_BREAKER_MEMORY_HELD)) {
                Map<String, Object> attributes = measurement.attributes();
                if (CircuitBreaker.REQUEST.equals(attributes.get(ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE))
                    && ChildMemoryCircuitBreaker.CATEGORY_PARSED_REQUEST.equals(
                        attributes.get(ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE)
                    )) {
                    measurements.add(measurement);
                }
            }
        }
        return measurements;
    }

    /**
     * A binary search of memory limits, looking for the lowest limit that'll break.
     */
    private static <E extends Exception> long findBreakerLimit(long min, long max, CheckedConsumer<ByteSizeValue, E> c) throws E {
        // max is an amount of memory that doesn't break
        // min is an amount of memory that *does* break
        while (max - min > 1) {
            assert max > min;
            long diff = max - min;
            logger.info(
                "Between {} and {}. {} bytes remaining.",
                ByteSizeValue.ofBytes(min),
                ByteSizeValue.ofBytes(max),
                ByteSizeValue.ofBytes(diff)
            );
            long mid = min + diff / 2;
            try {
                c.accept(ByteSizeValue.ofBytes(mid));
                max = mid;
            } catch (CircuitBreakingException e) {
                min = mid;
            }
        }
        return min;
    }
}
