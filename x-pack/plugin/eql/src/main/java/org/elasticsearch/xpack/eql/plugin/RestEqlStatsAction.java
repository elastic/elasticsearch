/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestActions;

import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestEqlStatsAction extends BaseRestHandler {

    protected RestEqlStatsAction(RestController controller) {
        controller.registerHandler(GET, "/_eql/stats", this);
    }

    @Override
    public String getName() {
        return "eql_stats";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        EqlStatsRequest request = new EqlStatsRequest();
        return channel -> client.execute(EqlStatsAction.INSTANCE, request, new RestActions.NodesResponseRestListener<>(channel));
    }

}
