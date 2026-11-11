/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.metric.MetricAttributes;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * The one place that turns a request's headers into a {@link QueryCaller}. It reads the headers from the {@link ThreadContext}
 * of the node that received the query, where {@link Task#HEADERS_TO_COPY} has put them, and folds each one into a closed set of
 * labels so no raw header value reaches a counter. Classifiers for more headers plug in here, so the {@code _xpack/usage}
 * counters, the APM query counter and the external-source counters all see the same caller.
 */
public final class QueryCallerClassifier {

    private static final List<QueryMetric> PRODUCT_CLIENTS = Arrays.stream(QueryMetric.values())
        .filter(client -> client != QueryMetric.OTHER && client != QueryMetric.REST)
        .toList();

    private QueryCallerClassifier() {}

    /**
     * The caller of the request whose context is {@code threadContext}. A {@code null} context, which some callers and mocks
     * supply, carries no headers.
     */
    public static QueryCaller classify(@Nullable ThreadContext threadContext) {
        String origin = threadContext == null ? null : threadContext.getHeader(Task.X_ELASTIC_PRODUCT_ORIGIN_HTTP_HEADER);
        return new QueryCaller(clientFromOrigin(origin));
    }

    /**
     * Folds the {@code X-elastic-product-origin} header into a client: absent or blank is {@link QueryMetric#REST}; a product in
     * {@link MetricAttributes#KNOWN_PRODUCT_ORIGINS}, or that product followed by {@code -} and anything (for example
     * {@code kibana-fleet} or {@code logstash-output-elasticsearch}), compared case-insensitively, is that product's client; any
     * other value is {@link QueryMetric#OTHER}.
     */
    public static QueryMetric clientFromOrigin(@Nullable String origin) {
        if (origin == null || origin.isBlank()) {
            return QueryMetric.REST;
        }
        String normalized = origin.strip().toLowerCase(Locale.ROOT);
        for (QueryMetric client : PRODUCT_CLIENTS) {
            String product = client.toString();
            if (normalized.equals(product) || normalized.startsWith(product + "-")) {
                return client;
            }
        }
        return QueryMetric.OTHER;
    }
}
