/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.SqlLicenseChecker;
import org.elasticsearch.xpack.sql.session.SqlSettings;

public class TransportSqlTranslateAction extends HandledTransportAction<SqlTranslateRequest, SqlTranslateResponse> {

    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;

    @Inject
    public TransportSqlTranslateAction(Settings settings, ThreadPool threadPool,
                              TransportService transportService, ActionFilters actionFilters,
                              IndexNameExpressionResolver indexNameExpressionResolver,
                              PlanExecutor planExecutor,
                              SqlLicenseChecker sqlLicenseChecker) {
        super(settings, SqlTranslateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SqlTranslateRequest::new);

        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    @Override
    protected void doExecute(SqlTranslateRequest request, ActionListener<SqlTranslateResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed();
        String query = request.query();

        SqlSettings sqlSettings = new SqlSettings(Settings.builder()
            .put(SqlSettings.PAGE_SIZE, request.fetchSize())
            .put(SqlSettings.TIMEZONE_ID, request.timeZone().getID()).build());
    
        listener.onResponse(new SqlTranslateResponse(planExecutor.searchSource(query, sqlSettings)));
    }
}