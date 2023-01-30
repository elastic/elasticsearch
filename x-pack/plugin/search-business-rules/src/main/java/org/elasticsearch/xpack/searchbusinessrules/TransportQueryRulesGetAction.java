/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.search.action.QueryRulesGetAction;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_RULES_MANAGEMENT_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.searchbusinessrules.SearchBusinessRules.QUERY_RULES_CONCRETE_INDEX_NAME;

public class TransportQueryRulesGetAction extends HandledTransportAction<QueryRulesGetAction.Request, QueryRulesGetAction.Response> {

    private final Client client;

    @Inject
    public TransportQueryRulesGetAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(QueryRulesGetAction.NAME, transportService, actionFilters, QueryRulesGetAction.Request::new);
        this.client = new OriginSettingClient(client, QUERY_RULES_MANAGEMENT_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, QueryRulesGetAction.Request request, ActionListener<QueryRulesGetAction.Response> listener) {
        GetRequest getRequest = new GetRequest(QUERY_RULES_CONCRETE_INDEX_NAME).id(request.getRuleSetId());

        executeAsyncWithOrigin(client, QUERY_RULES_MANAGEMENT_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                BytesReference source = null;
                if (getResponse.isExists()) {
                    source = getResponse.getSourceAsBytesRef();
                }
                listener.onResponse(new QueryRulesGetAction.Response(request.getRuleSetId(), getResponse.isExists(), source));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
