/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.entsearch.engine.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.entsearch.engine.Engine;
import org.elasticsearch.xpack.entsearch.engine.EngineIndexService;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ENGINE_ORIGIN;

public class TransportGetEngineAction extends HandledTransportAction<GetEngineAction.Request, GetEngineAction.Response> {

    private final Client client;

    private final EngineIndexService engineIndexService;

    @Inject
    public TransportGetEngineAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        EngineIndexService engineIndexService
    ) {
        super(GetEngineAction.NAME, transportService, actionFilters, GetEngineAction.Request::new);
        this.client = new OriginSettingClient(client, ENT_SEARCH_ENGINE_ORIGIN);
        this.engineIndexService = engineIndexService;
    }

    @Override
    protected void doExecute(Task task, GetEngineAction.Request request, ActionListener<GetEngineAction.Response> listener) {
        engineIndexService.getEngine(request.getEngineId(), new ActionListener<>() {
            // TODO - Create/update dates
            @Override
            public void onResponse(Engine engine) {
                String engineName = engine.name();
                String[] indices = engine.indices();
                String analyticsCollectionName = engine.analyticsCollectionName();

                listener.onResponse(new GetEngineAction.Response(engineName, indices, analyticsCollectionName));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

}
