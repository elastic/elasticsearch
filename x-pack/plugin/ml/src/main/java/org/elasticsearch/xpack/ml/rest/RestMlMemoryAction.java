/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestActions.NodesResponseRestListener;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.xpack.ml.MachineLearning.BASE_PATH;

@ServerlessScope(Scope.INTERNAL)
public class RestMlMemoryAction extends BaseRestHandler {

    public static final String NODE_ID = "nodeId";
    public static final String MASTER_TIMEOUT = "master_timeout";
    public static final String TIMEOUT = "timeout";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, BASE_PATH + "memory/{" + NODE_ID + "}/_stats"), new Route(GET, BASE_PATH + "memory/_stats"));
    }

    @Override
    public String getName() {
        return "ml_memory_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String nodeId = restRequest.param(NODE_ID);
        if (Strings.isNullOrEmpty(nodeId)) {
            nodeId = Metadata.ALL;
        }
        MlMemoryAction.Request request = new MlMemoryAction.Request(nodeId);
        request.masterNodeTimeout(restRequest.paramAsTime(MASTER_TIMEOUT, request.masterNodeTimeout()));
        request.timeout(restRequest.paramAsTime(TIMEOUT, request.timeout()));
        return channel -> client.execute(MlMemoryAction.INSTANCE, request, new NodesResponseRestListener<>(channel));
    }
}
