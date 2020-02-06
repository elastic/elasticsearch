/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.rest;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.rollup.action.DeleteRollupJobAction;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.rest.RestRequest.Method.DELETE;

public class RestDeleteRollupJobAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(RestDeleteRollupJobAction.class));

    public static final ParseField ID = new ParseField("id");

    @Override
    public List<Route> routes() {
        return emptyList();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return singletonList(new ReplacedRoute(DELETE, "/_rollup/job/{id}", DELETE, "/_xpack/rollup/job/{id}/", deprecationLogger));
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
