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
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.rules.QueryRule;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;

public class TransportTestQueryRulesetAction extends HandledTransportAction<
    TestQueryRulesetAction.Request,
    TestQueryRulesetAction.Response> {

    private final Client originSettingClient;

    @Inject
    public TransportTestQueryRulesetAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(
            TestQueryRulesetAction.NAME,
            transportService,
            actionFilters,
            TestQueryRulesetAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.originSettingClient = new OriginSettingClient(client, ENT_SEARCH_ORIGIN);
    }

    @Override
    protected void doExecute(Task task, TestQueryRulesetAction.Request request, ActionListener<TestQueryRulesetAction.Response> listener) {
        GetQueryRulesetAction.Request getQueryRulesetRequest = new GetQueryRulesetAction.Request(request.rulesetId());
        originSettingClient.execute(GetQueryRulesetAction.INSTANCE, getQueryRulesetRequest, new ActionListener<>() {
            @Override
            public void onResponse(GetQueryRulesetAction.Response response) {
                List<TestQueryRulesetAction.MatchedRule> matchedRules = new ArrayList<>();
                for (QueryRule rule : response.queryRuleset().rules()) {
                    if (rule.isRuleMatch(request.matchCriteria())) {
                        matchedRules.add(new TestQueryRulesetAction.MatchedRule(request.rulesetId(), rule.id()));
                    }
                }
                listener.onResponse(new TestQueryRulesetAction.Response(matchedRules.size(), matchedRules));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
