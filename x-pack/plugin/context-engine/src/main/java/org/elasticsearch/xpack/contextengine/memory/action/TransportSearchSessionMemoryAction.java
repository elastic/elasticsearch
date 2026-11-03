/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.memory.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.contextengine.ContextEnginePlugin;
import org.elasticsearch.xpack.contextengine.memory.SessionMemoryIndexService;

public class TransportSearchSessionMemoryAction extends HandledTransportAction<
    SearchSessionMemoryAction.Request,
    SearchSessionMemoryAction.Response> {

    private final SessionMemoryIndexService sessionMemoryIndexService;
    private final String inferenceId;
    private final String rerankInferenceId;

    @Inject
    public TransportSearchSessionMemoryAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ClusterService clusterService,
        FeatureService featureService
    ) {
        super(
            SearchSessionMemoryAction.NAME,
            transportService,
            actionFilters,
            SearchSessionMemoryAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.sessionMemoryIndexService = new SessionMemoryIndexService(
            client,
            xContentRegistry,
            feature -> featureService.clusterHasFeature(clusterService.state(), feature)
        );
        this.inferenceId = ContextEnginePlugin.SESSION_MEMORY_INFERENCE_ID.get(settings);
        this.rerankInferenceId = ContextEnginePlugin.SESSION_MEMORY_RERANK_INFERENCE_ID.get(settings);
    }

    @Override
    protected void doExecute(
        Task task,
        SearchSessionMemoryAction.Request request,
        ActionListener<SearchSessionMemoryAction.Response> listener
    ) {
        sessionMemoryIndexService.search(request, inferenceId, rerankInferenceId, listener.map(SearchSessionMemoryAction.Response::new));
    }
}
