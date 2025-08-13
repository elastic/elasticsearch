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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.rules.QueryRule;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ENT_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class TransportTestQueryRulesetAction extends HandledTransportAction<
    TestQueryRulesetAction.Request,
    TestQueryRulesetAction.Response> {

    private final Client client;

    @Inject
    public TransportTestQueryRulesetAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(
            TestQueryRulesetAction.NAME,
            transportService,
            actionFilters,
            TestQueryRulesetAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, TestQueryRulesetAction.Request request, ActionListener<TestQueryRulesetAction.Response> listener) {
        GetQueryRulesetAction.Request getQueryRulesetRequest = new GetQueryRulesetAction.Request(request.rulesetId());
        executeAsyncWithOrigin(
            client,
            ENT_SEARCH_ORIGIN,
            GetQueryRulesetAction.TYPE,
            getQueryRulesetRequest,
            ActionListener.wrap(getQueryRulesetResponse -> {
                List<TestQueryRulesetAction.MatchedRule> matchedRules = new ArrayList<>();
                for (QueryRule rule : getQueryRulesetResponse.queryRuleset().rules()) {
                    if (rule.isRuleMatch(request.matchCriteria())) {
                        matchedRules.add(new TestQueryRulesetAction.MatchedRule(request.rulesetId(), rule.id()));
                    }
                }
                listener.onResponse(new TestQueryRulesetAction.Response(matchedRules.size(), matchedRules));
            }, listener::onFailure)
        );
    }

}
