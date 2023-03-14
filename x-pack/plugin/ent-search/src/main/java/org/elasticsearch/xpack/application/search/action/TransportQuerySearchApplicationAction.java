/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.search.SearchApplication;

import java.util.Map;

public class TransportQuerySearchApplicationAction extends SearchApplicationTransportAction<
    QuerySearchApplicationAction.Request,
    QuerySearchApplicationAction.Response> {

    @Inject
    public TransportQuerySearchApplicationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays,
        XPackLicenseState licenseState
    ) {
        super(
            QuerySearchApplicationAction.NAME,
            transportService,
            actionFilters,
            QuerySearchApplicationAction.Request::new,
            client,
            clusterService,
            namedWriteableRegistry,
            bigArrays,
            licenseState
        );
    }

    @Override
    protected void doExecute(QuerySearchApplicationAction.Request request, ActionListener<QuerySearchApplicationAction.Response> listener) {
        systemIndexService.getSearchApplication(request.getName(), new ActionListener<SearchApplication>() {
            @Override
            public void onResponse(SearchApplication searchApplication) {
                Script script = searchApplication.searchApplicationTemplate().script();
                String source = script.getIdOrCode();
                Map<String, Object> defaultParams = script.getParams();

                Client client = systemIndexService.getClient();

                SearchTemplateRequestBuilder requestBuilder = new SearchTemplateRequestBuilder(client).setScript(source)
                    .setScriptParams((defaultParams))
                    .setRequest(new SearchRequest(searchApplication.name()));

                requestBuilder.execute(new ActionListener<SearchTemplateResponse>() {
                    @Override
                    public void onResponse(SearchTemplateResponse searchTemplateResponse) {
                        listener.onResponse(new QuerySearchApplicationAction.Response("okay"));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
