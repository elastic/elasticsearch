/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.lifecycle.rest;

import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.rest.RestUtils.getAckTimeout;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestPutDataStreamLifecycleAction extends BaseRestHandler {

    private static final String SUPPORTS_DOWNSAMPLING_METHOD = "dlm.downsampling_method";
    private static final Set<String> CAPABILITIES = Set.of(SUPPORTS_DOWNSAMPLING_METHOD);

    @Override
    public String getName() {
        return "put_data_lifecycles_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "/_data_stream/{name}/_lifecycle"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            PutDataStreamLifecycleAction.Request putLifecycleRequest = PutDataStreamLifecycleAction.Request.parseRequest(
                parser,
                (dataRetention, enabled, downsamplingRounds, downsamplingMethod) -> new PutDataStreamLifecycleAction.Request(
                    getMasterNodeTimeout(request),
                    getAckTimeout(request),
                    Strings.splitStringByCommaToArray(request.param("name")),
                    dataRetention,
                    enabled,
                    downsamplingRounds,
                    downsamplingMethod
                )
            );
            putLifecycleRequest.indicesOptions(IndicesOptions.fromRequest(request, putLifecycleRequest.indicesOptions()));
            return channel -> client.execute(
                PutDataStreamLifecycleAction.INSTANCE,
                putLifecycleRequest,
                new RestToXContentListener<>(channel)
            );
        }
    }

    @Override
    public Set<String> supportedCapabilities() {
        return CAPABILITIES;
    }
}
