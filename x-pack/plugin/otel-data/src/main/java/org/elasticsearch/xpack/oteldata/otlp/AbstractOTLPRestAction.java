/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp;

import com.google.protobuf.GeneratedMessage;
import com.google.rpc.Status;

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
        final MappingMode requestMappingMode = MappingMode.parse(request.header(MappingMode.HEADER));
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
                    var transportRequest = new OTLPActionRequest(content, requestMappingMode);
                    var release = Releasables.wrap(content, indexingPressureRelease);
                    client.execute(
                        type,
                        transportRequest,
                        ActionListener.releaseBefore(
                            release,
                            ActionListener.wrap(
                                r -> channel.sendResponse(new RestResponse(RestStatus.OK, CONTENT_TYPE_PROTOBUF, r.getResponse())),
                                e -> sendFailureResponse(channel, e)
                            )
                        )
                    );
                }

                @Override
                public void onFailure(RestChannel channel, Exception e) {
                    sendFailureResponse(channel, e);
                }
            },
            IndexingPressureAwareContentAggregator.BodyPostProcessor.NOOP
        );
    }

    /**
     * Sends a failure response using a Protobuf-encoded {@link Status google.rpc.Status} message.
     * <p>
     * From the OTLP spec:
     * "The response body for all HTTP 4xx and HTTP 5xx responses MUST be a Protobuf-encoded Status message
     * that describes the problem."
     *
     * @see <a href="https://opentelemetry.io/docs/specs/otlp/#failures-1">OTLP Failures</a>
     */
    private static void sendFailureResponse(RestChannel channel, Exception e) {
        logger.debug("OTLP request failed", e);
        try {
            // Per the OTLP spec, Status.code is not used over HTTP: "the server MAY omit Status.code field.
            // The clients are not expected to alter their behavior based on Status.code field".
            // The HTTP status code in the response is what drives client retry behavior.
            String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            Status status = Status.newBuilder().setMessage(message).build();
            channel.sendResponse(new RestResponse(ExceptionsHelper.status(e), CONTENT_TYPE_PROTOBUF, new BytesArray(status.toByteArray())));
        } catch (Exception sendException) {
            sendException.addSuppressed(e);
            logger.warn("failed to send failure response", sendException);
        }
    }

}
