/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.metrics;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class MetricsDBRestAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "metrics_action";
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
            var transportRequest = new MetricsDBTransportAction.MetricsRequest(
                Boolean.parseBoolean(request.header("X-MetricsDB-Normalized")),
                Boolean.parseBoolean(request.header("X-MetricsDB-Noop")),
                request.content().retain()
            );
            return channel -> client.execute(
                MetricsDBTransportAction.TYPE,
                transportRequest,
                ActionListener.releaseBefore(request.content(), new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(MetricsDBTransportAction.MetricsResponse r) throws Exception {
                        return successResponse(r.getResponse());
                    }
                })
            );
        }

        // according to spec empty requests are successful
        return channel -> channel.sendResponse(successResponse(ExportMetricsServiceResponse.newBuilder().build()));
    }

    private RestResponse successResponse(MessageLite response) throws IOException {
        var responseBytes = ByteBuffer.allocate(response.getSerializedSize());
        response.writeTo(CodedOutputStream.newInstance(responseBytes));

        return new RestResponse(RestStatus.OK, "application/x-protobuf", BytesReference.fromByteBuffer(responseBytes));
    }
}
