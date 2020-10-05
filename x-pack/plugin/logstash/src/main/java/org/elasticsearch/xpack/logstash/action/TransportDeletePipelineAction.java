/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.logstash.Logstash;

public class TransportDeletePipelineAction extends HandledTransportAction<DeletePipelineRequest, DeletePipelineResponse> {

    private final Client client;

    @Inject
    public TransportDeletePipelineAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(DeletePipelineAction.NAME, transportService, actionFilters, DeletePipelineRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, DeletePipelineRequest request, ActionListener<DeletePipelineResponse> listener) {
        client.prepareDelete(Logstash.LOGSTASH_CONCRETE_INDEX_NAME, request.id())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .execute(
                ActionListener.wrap(
                    deleteResponse -> listener.onResponse(new DeletePipelineResponse(deleteResponse.getResult() == Result.DELETED)),
                    listener::onFailure
                )
            );
    }
}
