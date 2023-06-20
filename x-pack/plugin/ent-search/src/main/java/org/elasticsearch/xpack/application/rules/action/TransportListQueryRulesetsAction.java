/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.rules.QueryRulesIndexService;
import org.elasticsearch.xpack.core.action.util.PageParams;

public class TransportListQueryRulesetsAction extends HandledTransportAction<
    ListQueryRulesetsAction.Request,
    ListQueryRulesetsAction.Response> {
    protected final QueryRulesIndexService systemIndexService;

    @Inject
    public TransportListQueryRulesetsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(ListQueryRulesetsAction.NAME, transportService, actionFilters, ListQueryRulesetsAction.Request::new);
        this.systemIndexService = new QueryRulesIndexService(client);
    }

    @Override
    protected void doExecute(
        Task task,
        ListQueryRulesetsAction.Request request,
        ActionListener<ListQueryRulesetsAction.Response> listener
    ) {
        final PageParams pageParams = request.pageParams();
        systemIndexService.listQueryRulesets(
            pageParams.getFrom(),
            pageParams.getSize(),
            listener.map(r -> new ListQueryRulesetsAction.Response(r.rulesets(), r.totalResults()))
        );
    }
}
