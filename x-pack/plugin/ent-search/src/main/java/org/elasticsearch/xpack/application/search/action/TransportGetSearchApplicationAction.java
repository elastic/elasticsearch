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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;

import java.util.Map;

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
        super(GetSearchApplicationAction.NAME, transportService, actionFilters, GetSearchApplicationAction.Request::new);
        this.systemIndexService = new SearchApplicationIndexService(client, clusterService, namedWriteableRegistry, bigArrays);
    }

    @Override
    protected void doExecute(
        Task task,
        GetSearchApplicationAction.Request request,
        ActionListener<GetSearchApplicationAction.Response> listener
    ) {
        systemIndexService.getSearchApplication(request.getName(), new ActionListener<>() {
                @Override
                public void onResponse(SearchApplication searchApplication) {
                    systemIndexService.checkAliasConsistency(searchApplication, new ActionListener<>() {
                        @Override
                        public void onResponse(Map<String, String> inconsistentIndices) {
                            for (String key : inconsistentIndices.keySet()) {
                                HeaderWarning.addWarning(key + " " + inconsistentIndices.get(key));
                            }
                            listener.onResponse(new GetSearchApplicationAction.Response(searchApplication));
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
            }
        );
    }
}
