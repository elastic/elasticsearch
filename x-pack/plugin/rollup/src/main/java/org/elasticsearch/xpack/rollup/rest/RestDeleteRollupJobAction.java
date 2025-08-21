/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteRollupJobAction extends BaseRestHandler {

    public static final ParseField ID = new ParseField("id");

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_rollup/job/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(ID.getPreferredName());
        DeleteRollupJobAction.Request request = new DeleteRollupJobAction.Request(id);

        return channel -> client.execute(DeleteRollupJobAction.INSTANCE, request, new RestToXContentListener<>(channel, r -> {
            if (r.getNodeFailures().size() > 0 || r.getTaskFailures().size() > 0) {
                return RestStatus.INTERNAL_SERVER_ERROR;
            }
            return RestStatus.OK;
        }));
    }

    @Override
    public String getName() {
        return "delete_rollup_job";
    }

}
