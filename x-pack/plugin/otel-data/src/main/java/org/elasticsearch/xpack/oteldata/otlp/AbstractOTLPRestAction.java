/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.google.protobuf.GeneratedMessage;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;

public abstract class AbstractOTLPRestAction extends BaseRestHandler {

    private final ActionType<OTLPActionResponse> type;
    private final BytesArray emptyResponse;

    public AbstractOTLPRestAction(ActionType<OTLPActionResponse> type, GeneratedMessage emptyResponse) {
        this.type = type;
        this.emptyResponse = new BytesArray(emptyResponse.toByteArray());
    }

    @Override
    public final boolean mediaTypesValid(RestRequest request) {
        // we expect OTLP payloads to be sent with a content type of "application/x-protobuf" without any additional parameters,
        // so the presence of an XContentType indicates an invalid media type
        return request.getXContentType() == null
            && request.getParsedContentType().mediaTypeWithoutParameters().equals("application/x-protobuf");
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.hasContent()) {
            var transportRequest = new OTLPActionRequest(request.content().retain());
            return channel -> client.execute(
                type,
                transportRequest,
                ActionListener.releaseBefore(request.content(), new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(OTLPActionResponse r) {
                        return new RestResponse(r.getStatus(), "application/x-protobuf", r.getResponse());
                    }
                })
            );
        }

        // If the server receives an empty request
        // (a request that does not carry any telemetry data)
        // the server SHOULD respond with success.
        // https://opentelemetry.io/docs/specs/otlp/#full-success-1
        return channel -> channel.sendResponse(new RestResponse(RestStatus.OK, "application/x-protobuf", emptyResponse));
    }

}
