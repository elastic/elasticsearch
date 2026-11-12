/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.logging.activity.QueryCaptureContext;
import org.elasticsearch.common.logging.activity.QuerySink;
import org.elasticsearch.common.logging.activity.QuerySinkProvider;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureSink;

import java.util.List;

/**
 * Gives the query interceptors the capture sink, which the plugin builds with its other components. The server loads this
 * class through SPI, and hands it the plugin, which is how it reaches the sink; it does so after the plugin's components
 * have been created. Without the feature flag the plugin builds nothing, so there is no sink and the settings have no effect.
 */
public class QuerySamplingSinkProvider implements QuerySinkProvider {

    private final QuerySamplingPlugin plugin;

    public QuerySamplingSinkProvider(QuerySamplingPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public List<QuerySink<QueryCaptureContext>> getQuerySinks() {
        QueryCaptureSink sink = plugin.captureSink();
        return sink == null ? List.of() : List.of(sink);
    }
}
