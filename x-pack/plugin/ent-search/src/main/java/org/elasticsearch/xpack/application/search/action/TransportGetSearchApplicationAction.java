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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;

public class TransportGetSearchApplicationAction extends HandledTransportAction<
    GetSearchApplicationAction.Request,
    GetSearchApplicationAction.Response> {

    protected final SearchApplicationIndexService systemIndexService;

    @Inject
    public TransportGetSearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays
    ) {
        super(
            GetSearchApplicationAction.NAME,
            transportService,
            actionFilters,
            GetSearchApplicationAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndexService = new SearchApplicationIndexService(client, clusterService, namedWriteableRegistry, bigArrays);
    }

    @Override
    protected void doExecute(
        Task task,
        GetSearchApplicationAction.Request request,
        ActionListener<GetSearchApplicationAction.Response> listener
    ) {
        systemIndexService.getSearchApplication(request.getName(), listener.map(searchApplication -> {
            if (searchApplication.hasStoredTemplate() == false) {
                HeaderWarning.addWarning(SearchApplication.NO_TEMPLATE_STORED_WARNING);
            }
            if (searchApplication.indices() == null || searchApplication.indices().length == 0) {
                HeaderWarning.addWarning(SearchApplication.NO_ALIAS_WARNING);
            }
            // Construct a new object to ensure we backfill the stored application with the default template
            return new GetSearchApplicationAction.Response(
                searchApplication.name(),
                searchApplication.indices(),
                searchApplication.analyticsCollectionName(),
                searchApplication.updatedAtMillis(),
                searchApplication.searchApplicationTemplateOrDefault()
            );
        }));
    }
}
