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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.rules.QueryRulesIndexService;

public class TransportGetQueryRulesetAction extends HandledTransportAction<GetQueryRulesetAction.Request, GetQueryRulesetAction.Response> {

    protected final QueryRulesIndexService systemIndexService;

    @Inject
    public TransportGetQueryRulesetAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            GetQueryRulesetAction.NAME,
            transportService,
            actionFilters,
            GetQueryRulesetAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndexService = new QueryRulesIndexService(client, clusterService.getClusterSettings());
    }

    @Override
    protected void doExecute(Task task, GetQueryRulesetAction.Request request, ActionListener<GetQueryRulesetAction.Response> listener) {
        systemIndexService.getQueryRuleset(request.rulesetId(), listener.map(GetQueryRulesetAction.Response::new));
    }
}
