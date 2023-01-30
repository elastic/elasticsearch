/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.QueryRulesDeleteAction;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_RULES_MANAGEMENT_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.searchbusinessrules.SearchBusinessRules.QUERY_RULES_CONCRETE_INDEX_NAME;

public class TransportQueryRulesDeleteAction extends HandledTransportAction<QueryRulesDeleteAction.Request, AcknowledgedResponse> {

    private final Client client;

    @Inject
    public TransportQueryRulesDeleteAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(QueryRulesDeleteAction.NAME, transportService, actionFilters, QueryRulesDeleteAction.Request::new);
        this.client = new OriginSettingClient(client, QUERY_RULES_MANAGEMENT_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, QueryRulesDeleteAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(QUERY_RULES_CONCRETE_INDEX_NAME).id(request.getRuleSetId());

        executeAsyncWithOrigin(client, QUERY_RULES_MANAGEMENT_ORIGIN, DeleteAction.INSTANCE, deleteRequest, new ActionListener<>() {

            @Override
            public void onResponse(DeleteResponse indexResponse) {
                listener.onResponse(
                    indexResponse.getResult().equals(DocWriteResponse.Result.DELETED)
                        ? AcknowledgedResponse.TRUE
                        : AcknowledgedResponse.FALSE
                );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
