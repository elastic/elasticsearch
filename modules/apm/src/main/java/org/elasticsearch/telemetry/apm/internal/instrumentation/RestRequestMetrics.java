/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.instrumentation;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.telemetry.instrumentation.HttpServerInstrumentation;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * REST-level request metrics for AutoOps.
 *
 * <p><b>These metrics constitute a contract between ES and AutoOps, never modify them without making sure these changes are agreed upon by
 * both sides.</b>
 */
public class RestRequestMetrics implements HttpServerInstrumentation {

    private static final String STATE_KEY = State.class.getName();
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
        var multiSearch = meterRegistry.registerDoubleHistogram(
            "es.rest.msearch.duration",
            "Durations of multi search requests",
            "ms",
            BUCKET_BOUNDARIES
        );
        var esqlSyncQuery = meterRegistry.registerDoubleHistogram(
            "es.rest.esql.duration",
            "Durations of synchronous ESQL query requests",
            "ms",
            BUCKET_BOUNDARIES
        );
        var index = meterRegistry.registerDoubleHistogram(
            "es.rest.index.duration",
            "Durations of document index requests",
            "ms",
            BUCKET_BOUNDARIES
        );
        var update = meterRegistry.registerDoubleHistogram(
            "es.rest.update.duration",
            "Durations of document update requests",
            "ms",
            BUCKET_BOUNDARIES
        );
        var bulk = meterRegistry.registerDoubleHistogram("es.rest.bulk.duration", "Durations of bulk requests", "ms", BUCKET_BOUNDARIES);
        var count = meterRegistry.registerDoubleHistogram("es.rest.count.duration", "Durations of count requests", "ms", BUCKET_BOUNDARIES);
        var searchScroll = meterRegistry.registerDoubleHistogram(
            "es.rest.search_scroll.duration",
            "Durations of search scroll requests",
            "ms",
            BUCKET_BOUNDARIES
        );
        var prometheusQuery = meterRegistry.registerDoubleHistogram(
            "es.rest.prometheus_query.duration",
            "Durations of Prometheus query requests",
            "ms",
            BUCKET_BOUNDARIES
        );
        var prometheusWrite = meterRegistry.registerDoubleHistogram(
            "es.rest.prometheus_write.duration",
            "Durations of Prometheus write requests",
            "ms",
            BUCKET_BOUNDARIES
        );
        this.measuredRoutes = Map.<RouteKey, DoubleHistogram>ofEntries(
            // search
            entry(new RouteKey(GET, "_search"), search),
            entry(new RouteKey(POST, "_search"), search),
            entry(new RouteKey(GET, "{index}/_search"), search),
            entry(new RouteKey(POST, "{index}/_search"), search),
            entry(new RouteKey(GET, "{index}/_knn_search"), search),
            entry(new RouteKey(POST, "{index}/_knn_search"), search),

            // multi search
            entry(new RouteKey(GET, "_msearch"), multiSearch),
            entry(new RouteKey(POST, "_msearch"), multiSearch),
            entry(new RouteKey(GET, "{index}/_msearch"), multiSearch),
            entry(new RouteKey(POST, "{index}/_msearch"), multiSearch),

            // ESQL sync query
            entry(new RouteKey(POST, "_query"), esqlSyncQuery),

            // index
            entry(new RouteKey(POST, "{index}/_doc/{id}"), index),
            entry(new RouteKey(PUT, "{index}/_doc/{id}"), index),
            entry(new RouteKey(POST, "{index}/_create/{id}"), index),
            entry(new RouteKey(PUT, "{index}/_create/{id}"), index),
            entry(new RouteKey(POST, "{index}/_doc"), index),

            // update
            entry(new RouteKey(POST, "{index}/_update/{id}"), update),

            // bulk
            entry(new RouteKey(POST, "_bulk"), bulk),
            entry(new RouteKey(PUT, "_bulk"), bulk),
            entry(new RouteKey(POST, "{index}/_bulk"), bulk),
            entry(new RouteKey(PUT, "{index}/_bulk"), bulk),

            // count
            entry(new RouteKey(GET, "_count"), count),
            entry(new RouteKey(POST, "_count"), count),
            entry(new RouteKey(GET, "{index}/_count"), count),
            entry(new RouteKey(POST, "{index}/_count"), count),

            // search scroll
            entry(new RouteKey(GET, "_search/scroll"), searchScroll),
            entry(new RouteKey(POST, "_search/scroll"), searchScroll),
            entry(new RouteKey(GET, "_search/scroll/{scroll_id}"), searchScroll),
            entry(new RouteKey(POST, "_search/scroll/{scroll_id}"), searchScroll),

            // prometheus query
            entry(new RouteKey(GET, "_prometheus/api/v1/series"), prometheusQuery),
            entry(new RouteKey(POST, "_prometheus/api/v1/series"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/{index}/api/v1/series"), prometheusQuery),
            entry(new RouteKey(POST, "_prometheus/{index}/api/v1/series"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/api/v1/query_range"), prometheusQuery),
            entry(new RouteKey(POST, "_prometheus/api/v1/query_range"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/{index}/api/v1/query_range"), prometheusQuery),
            entry(new RouteKey(POST, "_prometheus/{index}/api/v1/query_range"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/api/v1/query"), prometheusQuery),
            entry(new RouteKey(POST, "_prometheus/api/v1/query"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/{index}/api/v1/query"), prometheusQuery),
            entry(new RouteKey(POST, "_prometheus/{index}/api/v1/query"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/api/v1/labels"), prometheusQuery),
            entry(new RouteKey(POST, "_prometheus/api/v1/labels"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/{index}/api/v1/labels"), prometheusQuery),
            entry(new RouteKey(POST, "_prometheus/{index}/api/v1/labels"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/api/v1/label/{name}/values"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/{index}/api/v1/label/{name}/values"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/api/v1/metadata"), prometheusQuery),
            entry(new RouteKey(GET, "_prometheus/{index}/api/v1/metadata"), prometheusQuery),

            // prometheus write
            entry(new RouteKey(POST, "_prometheus/api/v1/write"), prometheusWrite),
            entry(new RouteKey(POST, "_prometheus/metrics/{dataset}/api/v1/write"), prometheusWrite),
            entry(new RouteKey(POST, "_prometheus/metrics/{dataset}/{namespace}/api/v1/write"), prometheusWrite)
        );
    }

    @Override
    public void start(ThreadContext threadContext, RestRequest request, @Nullable String route) {
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

        threadContext.putTransient(STATE_KEY, new State(durationHistogram, System.nanoTime()));
    }

    @Override
    public void recordException(RestRequest request, Throwable t) {}

    @Override
    public Releasable prepareEnd(ThreadContext threadContext, RestRequest request, RestResponse response) {
        State state = threadContext.getTransient(STATE_KEY);
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

    private record State(DoubleHistogram requestDuration, long startNanos) {}
}
