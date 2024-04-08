/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

@ServerlessScope(Scope.PUBLIC)
public class RestPostConnectorSyncJobAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "connector_sync_job_post_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/" + EnterpriseSearch.CONNECTOR_SYNC_JOB_API_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        PostConnectorSyncJobAction.Request request = PostConnectorSyncJobAction.Request.fromXContentBytes(
            restRequest.content(),
            restRequest.getXContentType()
        );

        return channel -> client.execute(
            PostConnectorSyncJobAction.INSTANCE,
            request,
            new RestToXContentListener<>(channel, r -> RestStatus.CREATED, r -> null)
        );
    }
}
