/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getMasterNodeTimeout;

public class RestExplainLifecycleAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/{index}/_ilm/explain"));
    }

    @Override
    public String getName() {
        return "ilm_explain_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String[] indexes = Strings.splitStringByCommaToArray(restRequest.param("index"));
        ExplainLifecycleRequest explainLifecycleRequest = new ExplainLifecycleRequest(getMasterNodeTimeout(restRequest));
        explainLifecycleRequest.indices(indexes);
        explainLifecycleRequest.indicesOptions(IndicesOptions.fromRequest(restRequest, IndicesOptions.strictExpandOpen()));
        explainLifecycleRequest.onlyManaged(restRequest.paramAsBoolean("only_managed", false));
        explainLifecycleRequest.onlyErrors(restRequest.paramAsBoolean("only_errors", false));
        return channel -> new RestCancellableNodeClient(client, restRequest.getHttpChannel()).execute(
            ExplainLifecycleAction.INSTANCE,
            explainLifecycleRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
