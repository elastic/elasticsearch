/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;

import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteRollupJobAction extends BaseRestHandler {

    public static final ParseField ID = new ParseField("id");

    public RestDeleteRollupJobAction(RestController controller) {
        controller.registerHandler(DELETE, "/_rollup/job/{id}", this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        String id = restRequest.param(ID.getPreferredName());
        DeleteRollupJobAction.Request request = new DeleteRollupJobAction.Request(id);

        return channel -> client.execute(DeleteRollupJobAction.INSTANCE, request,
            new RestToXContentListener<DeleteRollupJobAction.Response>(channel) {
            @Override
            protected RestStatus getStatus(DeleteRollupJobAction.Response response) {
                if (response.getNodeFailures().size() > 0 || response.getTaskFailures().size() > 0) {
                    return RestStatus.INTERNAL_SERVER_ERROR;
                }
                return RestStatus.OK;
            }
        });
    }

    @Override
    public String getName() {
        return "delete_rollup_job";
    }

}
