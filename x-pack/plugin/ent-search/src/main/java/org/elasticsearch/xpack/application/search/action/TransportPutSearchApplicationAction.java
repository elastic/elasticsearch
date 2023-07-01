/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;

public class TransportPutSearchApplicationAction extends HandledTransportAction<
    PutSearchApplicationAction.Request,
    PutSearchApplicationAction.Response> {
    protected final SearchApplicationIndexService systemIndexService;

    @Inject
    public TransportPutSearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays
    ) {
        super(PutSearchApplicationAction.NAME, transportService, actionFilters, PutSearchApplicationAction.Request::new);
        this.systemIndexService = new SearchApplicationIndexService(client, clusterService, namedWriteableRegistry, bigArrays);
    }

    @Override
    protected void doExecute(
        Task task,
        PutSearchApplicationAction.Request request,
        ActionListener<PutSearchApplicationAction.Response> listener
    ) {
        SearchApplication app = request.getSearchApplication();
        boolean create = request.create();
        systemIndexService.putSearchApplication(app, create, listener.map(r -> new PutSearchApplicationAction.Response(r.getResult())));

    }
}
