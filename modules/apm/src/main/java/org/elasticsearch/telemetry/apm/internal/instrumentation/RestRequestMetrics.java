/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import io.opentelemetry.context.ContextKey;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.telemetry.apm.internal.OtelContext;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST-level request metrics for AutoOps.
 *
 * <p><b>These metrics constitute a contract between ES and AutoOps, never modify them without making sure these changes are agreed upon by
 * both sides.</b>
 */
public class RestRequestMetrics {

    private static final double NANOS_PER_MS = MILLISECONDS.toNanos(1);

    // 1 ms to 30 sec
    private static final List<Double> BUCKET_BOUNDARIES = List.of(
        1.,
        5.,
        10.,
        25.,
        50.,
        100.,
        250.,
        500.,
        1_000.,
        2_500.,
        5_000.,
        10_000.,
        30_000.
    );

    private final Map<RouteKey, DoubleHistogram> measuredRoutes;

    public RestRequestMetrics(MeterRegistry meterRegistry) {
        var search = meterRegistry.registerDoubleHistogram(
            "es.rest.search.duration",
            "Durations of search requests",
            "ms",
            BUCKET_BOUNDARIES
        );
        this.measuredRoutes = Map.ofEntries(
            entry(new RouteKey(GET, "_search"), search),
            entry(new RouteKey(POST, "_search"), search),
            entry(new RouteKey(GET, "{index}/_search"), search),
            entry(new RouteKey(POST, "{index}/_search"), search),
            entry(new RouteKey(GET, "{index}/_knn_search"), search),
            entry(new RouteKey(POST, "{index}/_knn_search"), search)
        );
    }

    void start(ThreadContext threadContext, RestRequest request, @Nullable String route) {
        if (route == null) {
            return;
        }
        if (route.startsWith("/")) {
            route = route.substring(1);
        }

        var durationHistogram = measuredRoutes.get(new RouteKey(request.method(), route));
        if (durationHistogram == null) {
            return;
        }

        OtelContext.updateAndGet(threadContext, context -> context.with(State.KEY, new State(durationHistogram, System.nanoTime())));
    }

    Releasable prepareEnd(ThreadContext threadContext, RestResponse response) {
        State state = OtelContext.getValueOrNullFromContext(threadContext, State.KEY);
        if (state == null) {
            return () -> {};
        }

        Map<String, Object> attributes = Map.of("http.response.status_code", response.status().getStatus());
        return () -> {
            long endNanos = System.nanoTime();
            state.requestDuration.record((endNanos - state.startNanos) / NANOS_PER_MS, attributes);
        };
    }

    private record RouteKey(RestRequest.Method method, String route) {}

    private record State(DoubleHistogram requestDuration, long startNanos) {

        private static final ContextKey<State> KEY = ContextKey.named("elasticsearch-rest-request-metrics-state");
    }
}
