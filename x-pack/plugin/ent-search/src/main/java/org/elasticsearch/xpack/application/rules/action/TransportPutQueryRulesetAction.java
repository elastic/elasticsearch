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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.application.rules.QueryRulesIndexService;
import org.elasticsearch.xpack.application.rules.QueryRuleset;
import org.elasticsearch.xpack.application.rules.QueryRulesetConfig;

public class TransportPutQueryRulesetAction extends HandledTransportAction<PutQueryRulesetAction.Request, PutQueryRulesetAction.Response> {
    protected final QueryRulesIndexService systemIndexService;
    private final QueryRulesetConfig config;

    @Inject
    public TransportPutQueryRulesetAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        Settings settings
    ) {
        super(PutQueryRulesetAction.NAME, transportService, actionFilters, PutQueryRulesetAction.Request::new);
        this.systemIndexService = new QueryRulesIndexService(client);
        this.config = new QueryRulesetConfig(settings);
    }

    @Override
    protected void doExecute(Task task, PutQueryRulesetAction.Request request, ActionListener<PutQueryRulesetAction.Response> listener) {
        QueryRuleset queryRuleset = request.queryRuleset();

        if (queryRuleset.rules().size() > config.maxRulesPerRuleset()) {
            throw new IllegalArgumentException(
                "The number of rules in a ruleset cannot exceed [" + config.maxRulesPerRuleset() + "]."
                    + "This maximum can be set by changing the [" + QueryRulesetConfig.MAX_RULE_LIMIT_SETTING.getKey() + "] setting."
            );
        }

        systemIndexService.putQueryRuleset(queryRuleset, listener.map(r -> new PutQueryRulesetAction.Response(r.getResult())));

    }
}
