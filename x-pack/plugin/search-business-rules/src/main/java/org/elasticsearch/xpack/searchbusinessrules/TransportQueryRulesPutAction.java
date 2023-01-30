/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.QueryRulesPutAction;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_RULES_MANAGEMENT_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.searchbusinessrules.SearchBusinessRules.QUERY_RULES_CONCRETE_INDEX_NAME;

public class TransportQueryRulesPutAction extends HandledTransportAction<QueryRulesPutAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportQueryRulesPutAction.class);

    private final Client client;

    @Inject
    public TransportQueryRulesPutAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(QueryRulesPutAction.NAME, transportService, actionFilters, QueryRulesPutAction.Request::new);
        this.client = new OriginSettingClient(client, QUERY_RULES_MANAGEMENT_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, QueryRulesPutAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        IndexRequest indexRequest = new IndexRequest(QUERY_RULES_CONCRETE_INDEX_NAME).id(request.getRuleSetId())
            .source(request.getContent(), request.getContentType());

        executeAsyncWithOrigin(client, QUERY_RULES_MANAGEMENT_ORIGIN, IndexAction.INSTANCE, indexRequest, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
