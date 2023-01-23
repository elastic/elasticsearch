/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.search.action.QueryRulesPutAction;

import java.io.IOException;

public class TransportQueryRulesPutAction extends HandledTransportAction<QueryRulesPutAction.Request, AcknowledgedResponse> {

    @Inject
    public TransportQueryRulesPutAction(TransportService transportService, ActionFilters actionFilters) {
        super(QueryRulesPutAction.NAME, transportService, actionFilters, QueryRulesPutAction.Request::new);
    }

    @Override
    protected void doExecute(Task task, QueryRulesPutAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        listener.onResponse(new AcknowledgedResponse(true) {
            @Override
            protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
                builder.field("rules_id", request.getRuleSetId());
            }
        });
    }
}
