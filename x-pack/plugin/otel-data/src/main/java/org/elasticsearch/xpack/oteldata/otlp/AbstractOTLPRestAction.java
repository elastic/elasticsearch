/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.google.protobuf.GeneratedMessage;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.IndexingPressureAwareContentAggregator;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;

import java.io.IOException;

/**
 * Base REST handler for OTLP endpoints. Accumulates the protobuf request body
 * while tracking memory usage via {@link IndexingPressure}, then dispatches to
 * the appropriate transport action.
 */
public abstract class AbstractOTLPRestAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(AbstractOTLPRestAction.class);
    public static final String CONTENT_TYPE_PROTOBUF = "application/x-protobuf";

    private final ActionType<OTLPActionResponse> type;
    private final BytesArray emptyResponse;
    private final IndexingPressure indexingPressure;
    private final long maxRequestSizeBytes;

    public AbstractOTLPRestAction(
        ActionType<OTLPActionResponse> type,
        GeneratedMessage emptyResponse,
        IndexingPressure indexingPressure,
        long maxRequestSizeBytes
    ) {
        this.type = type;
        this.emptyResponse = new BytesArray(emptyResponse.toByteArray());
        this.indexingPressure = indexingPressure;
        this.maxRequestSizeBytes = maxRequestSizeBytes;
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    public final boolean mediaTypesValid(RestRequest request) {
        // we expect OTLP payloads to be sent with a content type of "application/x-protobuf" without any additional parameters,
        // so the presence of an XContentType indicates an invalid media type
        return request.getXContentType() == null
            && request.getParsedContentType().mediaTypeWithoutParameters().equals(CONTENT_TYPE_PROTOBUF);
    }

    @Override
    protected final RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return new IndexingPressureAwareContentAggregator(
            request,
            indexingPressure,
            maxRequestSizeBytes,
            new IndexingPressureAwareContentAggregator.CompletionHandler() {
                @Override
                public void onComplete(RestChannel channel, ReleasableBytesReference content, Releasable indexingPressureRelease) {
                    if (content.length() == 0) {
                        // If the server receives an empty request
                        // (a request that does not carry any telemetry data)
                        // the server SHOULD respond with success.
                        // https://opentelemetry.io/docs/specs/otlp/#full-success-1
                        Releasables.closeExpectNoException(content, indexingPressureRelease);
                        channel.sendResponse(new RestResponse(RestStatus.OK, CONTENT_TYPE_PROTOBUF, emptyResponse));
                        return;
                    }
                    var transportRequest = new OTLPActionRequest(content);
                    var release = Releasables.wrap(content, indexingPressureRelease);
                    client.execute(type, transportRequest, ActionListener.releaseBefore(release, new RestResponseListener<>(channel) {
                        @Override
                        public RestResponse buildResponse(OTLPActionResponse r) {
                            return new RestResponse(r.getStatus(), CONTENT_TYPE_PROTOBUF, r.getResponse());
                        }
                    }));
                }

                @Override
                public void onFailure(RestChannel channel, Exception e) {
                    logger.debug("OTLP request failed during content aggregation", e);
                    channel.sendResponse(new RestResponse(ExceptionsHelper.status(e), CONTENT_TYPE_PROTOBUF, BytesArray.EMPTY));
                }
            },
            IndexingPressureAwareContentAggregator.BodyPostProcessor.NOOP
        );
    }

}
