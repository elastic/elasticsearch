/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestResponseListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class PrometheusRemoteWriteRestAction extends BaseRestHandler {
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
    public boolean mediaTypesValid(RestRequest request) {
        return request.getXContentType() == null
            && request.getParsedContentType().mediaTypeWithoutParameters().equals("application/x-protobuf");
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (request.hasContent()) {
            String dataset = request.param(DataStream.DATASET, "generic");
            String namespace = request.param(DataStream.NAMESPACE, "default");
            DataStream.validateDataset(dataset);
            DataStream.validateNamespace(namespace);
            var transportRequest = new PrometheusRemoteWriteTransportAction.RemoteWriteRequest(
                request.content().retain(),
                dataset,
                namespace
            );
            return channel -> client.execute(
                PrometheusRemoteWriteTransportAction.TYPE,
                transportRequest,
                ActionListener.releaseBefore(request.content(), new RestResponseListener<>(channel) {
                    @Override
                    public RestResponse buildResponse(PrometheusRemoteWriteTransportAction.RemoteWriteResponse r) {
                        if (r.getMessage() != null) {
                            return new RestResponse(r.getStatus(), r.getMessage());
                        }
                        return new RestResponse(r.getStatus(), RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY);
                    }
                })
            );
        }

        return channel -> channel.sendResponse(new RestResponse(RestStatus.NO_CONTENT, RestResponse.TEXT_CONTENT_TYPE, BytesArray.EMPTY));
    }
}
