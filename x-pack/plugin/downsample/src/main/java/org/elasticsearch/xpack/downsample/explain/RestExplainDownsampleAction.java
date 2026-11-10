/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.explain;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_downsample/explain} and {@code GET /<index>/_downsample/explain}. The {@code source_index} parameter, a comma-separated
 * list of index names, keeps only the downsamples of those source indices.
 */
@ServerlessScope(Scope.INTERNAL)
public class RestExplainDownsampleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_downsample/explain"), new Route(GET, "/{index}/_downsample/explain"));
    }

    @Override
    public String getName() {
        return "downsample_explain_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        ExplainDownsampleAction.Request request = new ExplainDownsampleAction.Request(RestUtils.getMasterNodeTimeout(restRequest));
        request.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
        request.indicesOptions(IndicesOptions.fromRequest(restRequest, request.indicesOptions()));
        request.sourceIndices(Strings.splitStringByCommaToArray(restRequest.param("source_index")));
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            ExplainDownsampleAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel)
        );
    }
}
