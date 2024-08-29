/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.rest;

import org.elasticsearch.action.datastreams.lifecycle.ExplainDataStreamLifecycleAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

@ServerlessScope(Scope.PUBLIC)
public class RestExplainDataStreamLifecycleAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "data_stream_lifecycle_explain_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_lifecycle/explain"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(restRequest.param("index"));
        ExplainDataStreamLifecycleAction.Request explainRequest = new ExplainDataStreamLifecycleAction.Request(
            getMasterNodeTimeout(restRequest),
            indices
        );
        explainRequest.includeDefaults(restRequest.paramAsBoolean("include_defaults", false));
        explainRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, IndicesOptions.strictExpandOpen()));
        return channel -> client.execute(
            ExplainDataStreamLifecycleAction.INSTANCE,
            explainRequest,
            new RestRefCountedChunkedToXContentListener<>(channel)
        );
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return Set.of(DataStreamLifecycle.EFFECTIVE_RETENTION_REST_API_CAPABILITY);
    }
}
