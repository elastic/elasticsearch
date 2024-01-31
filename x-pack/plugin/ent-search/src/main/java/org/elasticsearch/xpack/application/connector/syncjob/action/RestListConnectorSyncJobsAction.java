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
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.application.EnterpriseSearch;
import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobType;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@ServerlessScope(Scope.PUBLIC)
public class RestListConnectorSyncJobsAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "connector_sync_jobs_list_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/" + EnterpriseSearch.CONNECTOR_SYNC_JOB_API_ENDPOINT));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        int from = restRequest.paramAsInt("from", PageParams.DEFAULT_FROM);
        int size = restRequest.paramAsInt("size", PageParams.DEFAULT_SIZE);
        String connectorId = restRequest.param(ListConnectorSyncJobsAction.Request.CONNECTOR_ID_FIELD.getPreferredName());
        String statusString = restRequest.param(ConnectorSyncJob.STATUS_FIELD.getPreferredName());
        ConnectorSyncStatus status = statusString != null ? ConnectorSyncStatus.fromString(statusString) : null;
        String[] jobTypeStringArray = restRequest.paramAsStringArray(ConnectorSyncJob.JOB_TYPE_FIELD.getPreferredName(), null);
        List<ConnectorSyncJobType> jobTypeList = jobTypeStringArray != null
            ? Arrays.stream(jobTypeStringArray).map(ConnectorSyncJobType::fromString).toList()
            : null;

        ListConnectorSyncJobsAction.Request request = new ListConnectorSyncJobsAction.Request(
            new PageParams(from, size),
            connectorId,
            status,
            jobTypeList
        );

        return channel -> client.execute(ListConnectorSyncJobsAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
