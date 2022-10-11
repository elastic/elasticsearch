/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.session.EsqlSession;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.session.Configuration;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;

public class TransportEsqlQueryAction extends HandledTransportAction<EsqlQueryRequest, EsqlQueryResponse> {

    private final PlanExecutor planExecutor;

    @Inject
    public TransportEsqlQueryAction(TransportService transportService, ActionFilters actionFilters, PlanExecutor planExecutor) {
        super(EsqlQueryAction.NAME, transportService, actionFilters, EsqlQueryRequest::new);
        this.planExecutor = planExecutor;
    }

    @Override
    protected void doExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        FunctionRegistry functionRegistry = new FunctionRegistry();
        Configuration configuration = new Configuration(
            request.zoneId() != null ? request.zoneId() : ZoneOffset.UTC,
            null,
            null,
            x -> Collections.emptySet()
        );
        new EsqlSession(planExecutor.indexResolver(), functionRegistry, configuration).execute(request.query(), listener.map(r -> {
            List<ColumnInfo> columns = r.columns().stream().map(c -> new ColumnInfo(c.qualifiedName(), c.dataType().esType())).toList();
            return new EsqlQueryResponse(columns, r.values());
        }));
    }
}
