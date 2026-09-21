/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.apache.http.HttpHeaders;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.xcontent.ParsedMediaType;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for Prometheus Remote Write 1.0 requests. Accumulates the protobuf request body
 * while tracking memory usage via {@link IndexingPressure}, then dispatches to
 * {@link PrometheusRemoteWriteTransportAction}.
 * <p>
 * Remote Write 2.0 ({@code io.prometheus.write.v2.Request}) is rejected with HTTP 415. The 2.0 proto
 * reserves fields 1-3 so that decoding a 2.0 payload as {@code prometheus.WriteRequest} yields an empty
 * message and a success status; senders then treat the write as accepted. The 2.0 spec requires 415 so
 * they can fall back to 1.0.
 * <p>
 * Path {@code {dataset}} and {@code {namespace}} segments are sanitized with {@link DataStream#sanitizeDataset} and
 * {@link DataStream#sanitizeNamespace} respectively, like OTLP attributes, rather than rejected when they contain
 * disallowed characters.
 *
 * @see <a href="https://prometheus.io/docs/specs/prw/remote_write_spec_2_0/#unsupported-request-content">PRW 2.0 unsupported content</a>
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusRemoteWriteRestAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(PrometheusRemoteWriteRestAction.class);

    /**
     * Remote Write 1.0 senders omit {@code proto} or set {@code proto=prometheus.WriteRequest}.
     * {@link ParsedMediaType} lowercases parameter values, so comparisons use that form.
     */
    private static final String REMOTE_WRITE_V1_PROTO = "prometheus.writerequest";

    private final IndexingPressure indexingPressure;
    private final long maxRequestSizeBytes;
    private final Recycler<BytesRef> recycler;

    public PrometheusRemoteWriteRestAction(IndexingPressure indexingPressure, long maxRequestSizeBytes, Recycler<BytesRef> recycler) {
        this.indexingPressure = indexingPressure;
        this.maxRequestSizeBytes = maxRequestSizeBytes;
        this.recycler = recycler;
    }

    @Override
    public String getName() {
        return "prometheus_remote_write_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_prometheus/api/v1/write"),
            new Route(POST, "/_prometheus/metrics/{dataset}/api/v1/write"),
            new Route(POST, "/_prometheus/metrics/{dataset}/{namespace}/api/v1/write")
        );
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        // Accept any application/x-protobuf, including proto=io.prometheus.write.v2.Request.
        // Unsupported proto parameters are rejected with 415 in prepareRequest so Prometheus
        // senders can fall back to remote write 1.0. Rejecting here would yield 406 instead.
        return request.getXContentType() == null
            && request.getParsedContentType().mediaTypeWithoutParameters().equals("application/x-protobuf");
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String dataset = DataStream.sanitizeDataset(request.param(DataStream.DATASET, "generic"));
        String namespace = DataStream.sanitizeNamespace(request.param(DataStream.NAMESPACE, "default"));

        // while the remote write spec mandates snappy, we intentionally want to allow additional compression formats
        var bodyPostProcessor = "snappy".equals(request.header(HttpHeaders.CONTENT_ENCODING))
            ? new SnappyBlockDecoder(recycler)
            : IndexingPressureAwareContentAggregator.BodyPostProcessor.NOOP;

        String unsupportedProto = unsupportedRemoteWriteProto(request);

        return new IndexingPressureAwareContentAggregator(
            request,
            indexingPressure,
            maxRequestSizeBytes,
            new IndexingPressureAwareContentAggregator.CompletionHandler() {
                @Override
                public void onComplete(RestChannel channel, ReleasableBytesReference content, Releasable indexingPressureRelease) {
                    if (unsupportedProto != null) {
                        Releasables.closeExpectNoException(content, indexingPressureRelease);
                        channel.sendResponse(unsupportedRemoteWriteResponse(unsupportedProto));
                        return;
                    }
                    var transportRequest = new PrometheusRemoteWriteTransportAction.RemoteWriteRequest(
                        content,
                        dataset,
                        namespace,
                        indexingPressureRelease
                    );
                    client.execute(
                        PrometheusRemoteWriteTransportAction.TYPE,
                        transportRequest,
                        ActionListener.releaseBefore(
                            transportRequest,
                            ActionListener.wrap(
                                r -> channel.sendResponse(
                                    new RestResponse(RestStatus.NO_CONTENT, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY)
                                ),
                                e -> {
                                    logger.debug("Remote write transport action failed", e);
                                    try {
                                        channel.sendResponse(
                                            new RestResponse(
                                                ExceptionsHelper.status(e),
                                                RestResponse.TEXT_CONTENT_TYPE,
                                                new BytesArray(e.getMessage())
                                            )
                                        );
                                    } catch (Exception sendException) {
                                        sendException.addSuppressed(e);
                                        logger.warn("failed to send failure response", sendException);
                                    }
                                }
                            )
                        )
                    );
                }

                @Override
                public void onFailure(RestChannel channel, Exception e) {
                    logger.debug("Remote write request failed during content aggregation", e);
                    channel.sendResponse(
                        new RestResponse(ExceptionsHelper.status(e), RestResponse.TEXT_CONTENT_TYPE, new BytesArray(e.getMessage()))
                    );
                }
            },
            bodyPostProcessor
        );
    }

    /**
     * Returns the Content-Type {@code proto} parameter when it is not a Remote Write 1.0 request,
     * otherwise {@code null}.
     */
    @Nullable
    private static String unsupportedRemoteWriteProto(RestRequest request) {
        ParsedMediaType parsed = request.getParsedContentType();
        if (parsed == null) {
            return null;
        }
        String proto = parsed.getParameters().get("proto");
        if (proto == null || proto.equals(REMOTE_WRITE_V1_PROTO)) {
            return null;
        }
        return proto;
    }

    private static RestResponse unsupportedRemoteWriteResponse(String proto) {
        String message = "Unsupported Prometheus remote write protobuf ["
            + proto
            + "]; this endpoint only supports prometheus.WriteRequest (remote write 1.0)";
        return new RestResponse(RestStatus.UNSUPPORTED_MEDIA_TYPE, RestResponse.TEXT_CONTENT_TYPE, new BytesArray(message));
    }
}
