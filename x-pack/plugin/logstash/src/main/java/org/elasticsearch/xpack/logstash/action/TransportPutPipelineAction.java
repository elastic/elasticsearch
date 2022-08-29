/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.ReservedStateAwareHandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.logstash.Logstash;

import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.xpack.core.ClientHelper.LOGSTASH_MANAGEMENT_ORIGIN;

public class TransportPutPipelineAction extends ReservedStateAwareHandledTransportAction<PutPipelineRequest, PutPipelineResponse> {

    private final Client client;

    @Inject
    public TransportPutPipelineAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(PutPipelineAction.NAME, clusterService, transportService, actionFilters, PutPipelineRequest::new);
        this.client = new OriginSettingClient(client, LOGSTASH_MANAGEMENT_ORIGIN);
    }

    static void createOrUpdatePipeline(Client client, PutPipelineRequest request, ActionListener<PutPipelineResponse> listener) {
        client.prepareIndex(Logstash.LOGSTASH_CONCRETE_INDEX_NAME)
            .setId(request.id())
            .setSource(request.source(), request.xContentType())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(
                ActionListener.wrap(
                    indexResponse -> listener.onResponse(new PutPipelineResponse(indexResponse.status())),
                    listener::onFailure
                )
            );
    }

    @Override
    protected void doExecuteProtected(Task task, PutPipelineRequest request, ActionListener<PutPipelineResponse> listener) {
        createOrUpdatePipeline(client, request, listener);
    }

    @Override
    public Optional<String> reservedStateHandlerName() {
        return super.reservedStateHandlerName();
    }

    @Override
    public Set<String> modifiedKeys(PutPipelineRequest request) {
        return super.modifiedKeys(request);
    }
}
