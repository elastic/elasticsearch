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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.rules.QueryRulesIndexService;

public class TransportDeleteQueryRuleAction extends HandledTransportAction<DeleteQueryRuleAction.Request, AcknowledgedResponse> {
    protected final QueryRulesIndexService systemIndexService;

    @Inject
    public TransportDeleteQueryRuleAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            DeleteQueryRuleAction.NAME,
            transportService,
            actionFilters,
            DeleteQueryRuleAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.systemIndexService = new QueryRulesIndexService(client, clusterService.getClusterSettings());
    }

    @Override
    protected void doExecute(Task task, DeleteQueryRuleAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        String rulesetId = request.rulesetId();
        String ruleId = request.ruleId();
        systemIndexService.deleteQueryRule(rulesetId, ruleId, listener.map(v -> AcknowledgedResponse.TRUE));
    }
}
