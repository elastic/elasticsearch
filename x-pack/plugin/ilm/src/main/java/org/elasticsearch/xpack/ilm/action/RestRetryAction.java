/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.action.ILMActions;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestRetryAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_ilm/retry"));
    }

    @Override
    public String getName() {
        return "ilm_retry_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String[] indices = Strings.splitStringByCommaToArray(restRequest.param("index"));
        TransportRetryAction.Request request = new TransportRetryAction.Request(indices);
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        request.masterNodeTimeout(restRequest.paramAsTime("master_timeout", request.masterNodeTimeout()));
        request.indices(indices);
        request.indicesOptions(IndicesOptions.fromRequest(restRequest, IndicesOptions.strictExpandOpen()));
        return channel -> client.execute(ILMActions.RETRY, request, new RestToXContentListener<>(channel));
    }
}
