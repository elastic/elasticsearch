/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.syncjob.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.ConnectorSyncStatus;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobIndexService;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobType;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.util.List;

public class TransportListConnectorSyncJobsAction extends HandledTransportAction<
    ListConnectorSyncJobsAction.Request,
    ListConnectorSyncJobsAction.Response> {
    protected final ConnectorSyncJobIndexService connectorSyncJobIndexService;

    @Inject
    public TransportListConnectorSyncJobsAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            ListConnectorSyncJobsAction.NAME,
            transportService,
            actionFilters,
            ListConnectorSyncJobsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorSyncJobIndexService = new ConnectorSyncJobIndexService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        ListConnectorSyncJobsAction.Request request,
        ActionListener<ListConnectorSyncJobsAction.Response> listener
    ) {
        final PageParams pageParams = request.getPageParams();
        final String connectorId = request.getConnectorId();
        final ConnectorSyncStatus syncStatus = request.getConnectorSyncStatus();
        final List<ConnectorSyncJobType> jobTypeList = request.getConnectorSyncJobTypeList();

        connectorSyncJobIndexService.listConnectorSyncJobs(
            pageParams.getFrom(),
            pageParams.getSize(),
            connectorId,
            syncStatus,
            jobTypeList,
            listener.map(r -> new ListConnectorSyncJobsAction.Response(r.connectorSyncJobs(), r.totalResults()))
        );
    }
}
