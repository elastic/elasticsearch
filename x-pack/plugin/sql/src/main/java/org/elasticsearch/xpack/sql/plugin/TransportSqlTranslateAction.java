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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.sql.action.Protocol;
import org.elasticsearch.xpack.sql.action.SqlTranslateAction;
import org.elasticsearch.xpack.sql.action.SqlTranslateRequest;
import org.elasticsearch.xpack.sql.action.SqlTranslateResponse;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;

import static org.elasticsearch.xpack.sql.plugin.Transports.clusterName;
import static org.elasticsearch.xpack.sql.plugin.Transports.username;

/**
 * Transport action for translating SQL queries into ES requests
 */
public class TransportSqlTranslateAction extends HandledTransportAction<SqlTranslateRequest, SqlTranslateResponse> {
    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;

    @Inject
    public TransportSqlTranslateAction(
        Settings settings,
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        SqlLicenseChecker sqlLicenseChecker
    ) {
        super(SqlTranslateAction.NAME, transportService, actionFilters, SqlTranslateRequest::new);

        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.clusterService = clusterService;
        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    @Override
    protected void doExecute(Task task, SqlTranslateRequest request, ActionListener<SqlTranslateResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());

        SqlConfiguration cfg = new SqlConfiguration(
            request.zoneId(),
            request.catalog(),
            request.fetchSize(),
            request.requestTimeout(),
            request.pageTimeout(),
            request.filter(),
            request.runtimeMappings(),
            request.mode(),
            request.clientId(),
            request.version(),
            username(securityContext),
            clusterName(clusterService),
            Protocol.FIELD_MULTI_VALUE_LENIENCY,
            Protocol.INDEX_INCLUDE_FROZEN,
            null,
            null,
            Protocol.ALLOW_PARTIAL_SEARCH_RESULTS
        );

        planExecutor.searchSource(
            cfg,
            request.query(),
            request.params(),
            listener.delegateFailureAndWrap((l, searchSourceBuilder) -> l.onResponse(new SqlTranslateResponse(searchSourceBuilder)))
        );
    }
}
