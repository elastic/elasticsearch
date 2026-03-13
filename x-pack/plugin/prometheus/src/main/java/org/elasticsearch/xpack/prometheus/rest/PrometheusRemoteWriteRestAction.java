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
import org.elasticsearch.core.Releasable;
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

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST handler for Prometheus Remote Write requests. Accumulates the protobuf request body
 * while tracking memory usage via {@link IndexingPressure}, then dispatches to
 * {@link PrometheusRemoteWriteTransportAction}.
 */
@ServerlessScope(Scope.PUBLIC)
public class PrometheusRemoteWriteRestAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(PrometheusRemoteWriteRestAction.class);

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
            new Route(POST, "/_prometheus/{dataset}/api/v1/write"),
            new Route(POST, "/_prometheus/{dataset}/{namespace}/api/v1/write")
        );
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return request.getXContentType() == null
            && request.getParsedContentType().mediaTypeWithoutParameters().equals("application/x-protobuf");
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String dataset = request.param(DataStream.DATASET, "generic");
        String namespace = request.param(DataStream.NAMESPACE, "default");
        DataStream.validateDataset(dataset);
        DataStream.validateNamespace(namespace);

        // while the remote write spec mandates snappy, we intentionally want to allow additional compression formats
        var bodyPostProcessor = "snappy".equals(request.header(HttpHeaders.CONTENT_ENCODING))
            ? new SnappyBlockDecoder(recycler)
            : IndexingPressureAwareContentAggregator.BodyPostProcessor.NOOP;

        return new IndexingPressureAwareContentAggregator(
            request,
            indexingPressure,
            maxRequestSizeBytes,
            new IndexingPressureAwareContentAggregator.CompletionHandler() {
                @Override
                public void onComplete(RestChannel channel, ReleasableBytesReference content, Releasable indexingPressureRelease) {
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
}
