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
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;
import org.elasticsearch.xpack.core.action.util.PageParams;

public class TransportListSearchApplicationAction extends HandledTransportAction<
    ListSearchApplicationAction.Request,
    ListSearchApplicationAction.Response> {
    protected final SearchApplicationIndexService systemIndexService;

    @Inject
    public TransportListSearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays
    ) {
        super(ListSearchApplicationAction.NAME, transportService, actionFilters, ListSearchApplicationAction.Request::new);
        this.systemIndexService = new SearchApplicationIndexService(client, clusterService, namedWriteableRegistry, bigArrays);
    }

    @Override
    protected void doExecute(
        Task task,
        ListSearchApplicationAction.Request request,
        ActionListener<ListSearchApplicationAction.Response> listener
    ) {
        final PageParams pageParams = request.pageParams();
        systemIndexService.listSearchApplication(
            request.query(),
            pageParams.getFrom(),
            pageParams.getSize(),
            listener.map(r -> new ListSearchApplicationAction.Response(r.items(), r.totalResults()))
        );
    }
}
