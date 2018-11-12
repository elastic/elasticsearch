/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.action.SqlTranslateAction;
import org.elasticsearch.xpack.sql.action.SqlTranslateRequest;
import org.elasticsearch.xpack.sql.action.SqlTranslateResponse;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.stats.Counters;

/**
 * Transport action for translating SQL queries into ES requests
 */
public class TransportSqlTranslateAction extends HandledTransportAction<SqlTranslateRequest, SqlTranslateResponse> {
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;
    private final CounterMetric counter = new CounterMetric();

    @Inject
    public TransportSqlTranslateAction(TransportService transportService, ActionFilters actionFilters,
                                       PlanExecutor planExecutor, SqlLicenseChecker sqlLicenseChecker) {
        super(SqlTranslateAction.NAME, transportService, actionFilters, (Writeable.Reader<SqlTranslateRequest>) SqlTranslateRequest::new);

        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    @Override
    protected void doExecute(Task task, SqlTranslateRequest request, ActionListener<SqlTranslateResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());

        counter.inc();
        Configuration cfg = new Configuration(request.timeZone(), request.fetchSize(),
                request.requestTimeout(), request.pageTimeout(), request.filter());

        planExecutor.searchSource(cfg, request.query(), request.params(), ActionListener.wrap(
                searchSourceBuilder -> listener.onResponse(new SqlTranslateResponse(searchSourceBuilder)), listener::onFailure));
    }
    
    public Counters stats() {
        Counters counters = new Counters();
        counters.inc("queries.translate.count", counter.count());
        
        return counters;
    }
}
