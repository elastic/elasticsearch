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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.application.search.SearchApplicationIndexService;
import org.elasticsearch.xpack.application.search.SearchApplicationTemplateService;

import java.util.Map;
import java.util.function.Predicate;

public class TransportRenderSearchApplicationQueryAction extends HandledTransportAction<
    SearchApplicationSearchRequest,
    RenderSearchApplicationQueryAction.Response> {

    protected final SearchApplicationIndexService systemIndexService;

    private final SearchApplicationTemplateService templateService;

    @Inject
    public TransportRenderSearchApplicationQueryAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        NamedWriteableRegistry namedWriteableRegistry,
        BigArrays bigArrays,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        FeatureService featureService
    ) {
        super(
            RenderSearchApplicationQueryAction.NAME,
            transportService,
            actionFilters,
            SearchApplicationSearchRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndexService = new SearchApplicationIndexService(client, clusterService, namedWriteableRegistry, bigArrays);
        Predicate<NodeFeature> clusterSupportsFeature = f -> {
            ClusterState state = clusterService.state();
            return state.clusterRecovered() && featureService.clusterHasFeature(state, f);
        };
        this.templateService = new SearchApplicationTemplateService(scriptService, xContentRegistry, clusterSupportsFeature);
    }

    @Override
    protected void doExecute(
        Task task,
        SearchApplicationSearchRequest request,
        ActionListener<RenderSearchApplicationQueryAction.Response> listener
    ) {
        systemIndexService.getSearchApplication(request.name(), listener.delegateFailureAndWrap((delegate, searchApplication) -> {
            final Map<String, Object> renderedMetadata = templateService.renderTemplate(searchApplication, request.queryParams());
            final SearchSourceBuilder sourceBuilder = templateService.renderQuery(searchApplication, renderedMetadata);
            delegate.onResponse(new RenderSearchApplicationQueryAction.Response(request.name(), sourceBuilder));
        }));
    }
}
