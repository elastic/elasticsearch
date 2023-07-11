/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.action.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.action.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;

import static org.elasticsearch.xpack.sql.action.SqlClearCursorAction.NAME;

public class TransportSqlClearCursorAction extends HandledTransportAction<SqlClearCursorRequest, SqlClearCursorResponse> {
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;

    @Inject
    public TransportSqlClearCursorAction(
        TransportService transportService,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        SqlLicenseChecker sqlLicenseChecker
    ) {
        super(NAME, transportService, actionFilters, SqlClearCursorRequest::new);
        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    @Override
    protected void doExecute(Task task, SqlClearCursorRequest request, ActionListener<SqlClearCursorResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());
        operation(planExecutor, request, listener);
    }

    public static void operation(
        PlanExecutor planExecutor,
        SqlClearCursorRequest request,
        ActionListener<SqlClearCursorResponse> listener
    ) {
        Cursor cursor = Cursors.decodeFromStringWithZone(request.getCursor(), planExecutor.writeableRegistry()).v1();
        planExecutor.cleanCursor(
            cursor,
            listener.delegateFailureAndWrap((l, success) -> l.onResponse(new SqlClearCursorResponse(success)))
        );
    }
}
