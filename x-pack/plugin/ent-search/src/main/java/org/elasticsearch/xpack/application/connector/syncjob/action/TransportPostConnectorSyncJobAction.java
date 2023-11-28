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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJob;
import org.elasticsearch.xpack.application.connector.syncjob.ConnectorSyncJobIndexService;

import java.util.Objects;

public class TransportPostConnectorSyncJobAction extends HandledTransportAction<
    PostConnectorSyncJobAction.Request,
    PostConnectorSyncJobAction.Response> {

    protected final ConnectorSyncJobIndexService syncJobIndexService;

    @Inject
    public TransportPostConnectorSyncJobAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            PostConnectorSyncJobAction.NAME,
            transportService,
            actionFilters,
            PostConnectorSyncJobAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.syncJobIndexService = new ConnectorSyncJobIndexService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        PostConnectorSyncJobAction.Request request,
        ActionListener<PostConnectorSyncJobAction.Response> listener
    ) {
        if (Objects.isNull(request.getJobType())) {
            HeaderWarning.addWarning(ConnectorSyncJob.DEFAULT_JOB_TYPE_USED_WARNING);
        }

        if (Objects.isNull(request.getTriggerMethod())) {
            HeaderWarning.addWarning(ConnectorSyncJob.DEFAULT_TRIGGER_METHOD_USED_WARNING);
        }

        syncJobIndexService.createConnectorSyncJob(request, listener.map(response -> response));
    }
}
