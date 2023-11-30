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
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.application.connector.syncjob.action.DeleteConnectorSyncJobAction.Request.CONNECTOR_SYNC_JOB_ID_FIELD;

public class RestDeleteConnectorSyncJobAction extends BaseRestHandler {

    private static final String CONNECTOR_SYNC_JOB_ID_PARAM = CONNECTOR_SYNC_JOB_ID_FIELD.getPreferredName();

    @Override
    public String getName() {
        return "connector_sync_job_delete_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(
                RestRequest.Method.DELETE,
                "/" + EnterpriseSearch.CONNECTOR_API_ENDPOINT + "/_sync_job/{" + CONNECTOR_SYNC_JOB_ID_PARAM + "}"
            )
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteConnectorSyncJobAction.Request request = new DeleteConnectorSyncJobAction.Request(
            restRequest.param(CONNECTOR_SYNC_JOB_ID_PARAM)
        );
        return restChannel -> client.execute(DeleteConnectorSyncJobAction.INSTANCE, request, new RestToXContentListener<>(restChannel));
    }
}
