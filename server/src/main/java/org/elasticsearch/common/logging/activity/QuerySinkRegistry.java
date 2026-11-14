/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import java.util.ArrayList;
import java.util.List;

/** Node-scoped plugin sinks, shared by the interceptors for each query type. */
public final class QuerySinkRegistry {
    private final List<QuerySink<QueryCaptureContext>> sinks;

    /** Creates the plugin sinks once, before transport actions are constructed. */
    public QuerySinkRegistry(List<? extends QuerySinkProvider> providers) {
        var sinks = new ArrayList<QuerySink<QueryCaptureContext>>();
        for (var provider : providers) {
            sinks.addAll(provider.getQuerySinks());
        }
        this.sinks = List.copyOf(sinks);
    }

    /** Keeps the built-in query logger alongside the independently enabled plugin sinks. */
    public <C extends QueryCaptureContext> QueryInterceptor<C> createInterceptor(QuerySink<C> queryLogger) {
        var allSinks = new ArrayList<QuerySink<? super C>>(sinks.size() + 1);
        allSinks.add(queryLogger);
        allSinks.addAll(sinks);
        return new QueryInterceptor<>(allSinks);
    }
}
