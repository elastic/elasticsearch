/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class OTLPMetricsRestAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "otlp_metrics_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_otlp/v1/metrics"));
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return request.getXContentType() == null
            && request.getParsedContentType().mediaTypeWithoutParameters().equals("application/x-protobuf");
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.hasContent()) {
            var transportRequest = new OTLPMetricsTransportAction.MetricsRequest(request.content().retain());
            return channel -> client.execute(
                OTLPMetricsTransportAction.TYPE,
                transportRequest,
                ActionListener.releaseBefore(request.content(), new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(OTLPMetricsTransportAction.MetricsResponse r) {
                        return new RestResponse(r.getStatus(), "application/x-protobuf", r.getResponse());
                    }
                })
            );
        }

        // If the server receives an empty request
        // (a request that does not carry any telemetry data)
        // the server SHOULD respond with success.
        // https://opentelemetry.io/docs/specs/otlp/#full-success-1
        return channel -> channel.sendResponse(
            new RestResponse(
                RestStatus.OK,
                "application/x-protobuf",
                new BytesArray(ExportMetricsServiceResponse.newBuilder().build().toByteArray())
            )
        );
    }

}
