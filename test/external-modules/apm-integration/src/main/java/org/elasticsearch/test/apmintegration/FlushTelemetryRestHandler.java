/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * REST handler for tests that triggers a flush of all telemetry (traces, metrics) so tests can await export.
 */
public class FlushTelemetryRestHandler extends BaseRestHandler {

    private final SetOnce<TelemetryProvider> telemetryProvider = new SetOnce<>();

    FlushTelemetryRestHandler() {}

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_flush_telemetry"));
    }

    @Override
    public String getName() {
        return "flush_telemetry_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            telemetryProvider.get().attemptFlushMetrics();
            telemetryProvider.get().attemptFlushTraces();
            try (XContentBuilder builder = channel.newBuilder()) {
                channel.sendResponse(new RestResponse(RestStatus.OK, builder));
            }
        };
    }

    void setTelemetryProvider(TelemetryProvider provider) {
        this.telemetryProvider.set(provider);
    }
}
