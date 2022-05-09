/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ql.QlVersionMismatchException;
import org.elasticsearch.xpack.sql.action.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.action.SqlClearCursorRequest;
import org.elasticsearch.xpack.sql.action.SqlClearCursorResponse;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursors;

import java.time.ZoneId;

import static org.elasticsearch.xpack.ql.plugin.TransportActionUtils.retryOnNodeWithMatchingVersion;
import static org.elasticsearch.xpack.sql.action.SqlClearCursorAction.NAME;

public class TransportSqlClearCursorAction extends HandledTransportAction<SqlClearCursorRequest, SqlClearCursorResponse> {
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;
    private final TransportService transportService;
    private final ClusterService clusterService;

    private static final Logger log = LogManager.getLogger(TransportSqlClearCursorAction.class);

    @Inject
    public TransportSqlClearCursorAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        SqlLicenseChecker sqlLicenseChecker
    ) {
        super(NAME, transportService, actionFilters, SqlClearCursorRequest::new);
        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
        this.transportService = transportService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, SqlClearCursorRequest request, ActionListener<SqlClearCursorResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());
        operation(planExecutor, request, listener, transportService, clusterService);
    }

    public static void operation(
        PlanExecutor planExecutor,
        SqlClearCursorRequest request,
        ActionListener<SqlClearCursorResponse> listener,
        TransportService transportService,
        ClusterService clusterService
    ) {
        try {
            Tuple<Cursor, ZoneId> decoded = Cursors.decodeFromStringWithZone(request.getCursor(), planExecutor.writeableRegistry());
            planExecutor.cleanCursor(
                decoded.v1(),
                ActionListener.wrap(success -> listener.onResponse(new SqlClearCursorResponse(success)), listener::onFailure)
            );
        } catch (QlVersionMismatchException e) {
            retryOnNodeWithMatchingVersion(
                clusterService,
                e,
                node -> transportService.sendRequest(
                    node,
                    SqlClearCursorAction.NAME,
                    request,
                    new ActionListenerResponseHandler<>(listener, SqlClearCursorResponse::new, ThreadPool.Names.SAME)
                ),
                listener::onFailure,
                log
            );
        }
    }
}
