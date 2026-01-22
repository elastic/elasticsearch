/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.connector.ConnectorFiltering;
import org.elasticsearch.xpack.application.connector.ConnectorIndexService;
import org.elasticsearch.xpack.application.connector.filtering.FilteringAdvancedSnippet;
import org.elasticsearch.xpack.application.connector.filtering.FilteringRule;

import java.util.List;

public class TransportUpdateConnectorFilteringAction extends HandledTransportAction<
    UpdateConnectorFilteringAction.Request,
    ConnectorUpdateActionResponse> {

    protected final ConnectorIndexService connectorIndexService;

    @Inject
    public TransportUpdateConnectorFilteringAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            UpdateConnectorFilteringAction.NAME,
            transportService,
            actionFilters,
            UpdateConnectorFilteringAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.connectorIndexService = new ConnectorIndexService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        UpdateConnectorFilteringAction.Request request,
        ActionListener<ConnectorUpdateActionResponse> listener
    ) {
        String connectorId = request.getConnectorId();
        List<ConnectorFiltering> filtering = request.getFiltering();
        FilteringAdvancedSnippet advancedSnippet = request.getAdvancedSnippet();
        List<FilteringRule> rules = request.getRules();
        // If [filtering] is not present in request body, it means that user's intention is to
        // update draft's rules or advanced snippet
        if (request.getFiltering() == null) {
            connectorIndexService.updateConnectorFilteringDraft(
                connectorId,
                advancedSnippet,
                rules,
                listener.map(r -> new ConnectorUpdateActionResponse(r.getResult()))
            );
        }
        // Otherwise override the whole filtering object (discouraged in docs)
        else {
            connectorIndexService.updateConnectorFiltering(
                connectorId,
                filtering,
                listener.map(r -> new ConnectorUpdateActionResponse(r.getResult()))
            );
        }
    }
}
