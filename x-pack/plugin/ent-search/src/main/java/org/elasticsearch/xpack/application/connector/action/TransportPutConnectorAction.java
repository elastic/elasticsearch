/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.ConnectorIndexService;

public class TransportPutConnectorAction extends HandledTransportAction<PutConnectorAction.Request, PutConnectorAction.Response> {

    protected final ConnectorIndexService connectorIndexService;

    @Inject
    public TransportPutConnectorAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            PutConnectorAction.NAME,
            transportService,
            actionFilters,
            PutConnectorAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorIndexService = new ConnectorIndexService(client);
    }

    @Override
    protected void doExecute(Task task, PutConnectorAction.Request request, ActionListener<PutConnectorAction.Response> listener) {
        connectorIndexService.createConnectorWithDocId(request, listener.map(r -> new PutConnectorAction.Response(r.getResult())));
    }
}
