/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.sql.plugin.Transports.clusterName;
import static org.elasticsearch.xpack.sql.plugin.Transports.username;

public class TransportSqlQueryAction extends HandledTransportAction<SqlQueryRequest, SqlQueryResponse> {
    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;

    @Inject
    public TransportSqlQueryAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                   ThreadPool threadPool, ActionFilters actionFilters, PlanExecutor planExecutor,
                                   SqlLicenseChecker sqlLicenseChecker) {
        super(SqlQueryAction.NAME, transportService, actionFilters, SqlQueryRequest::new);

        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
                new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.clusterService = clusterService;
        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    @Override
    protected void doExecute(Task task, SqlQueryRequest request, ActionListener<SqlQueryResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());
        operation(planExecutor, request, listener, username(securityContext), clusterName(clusterService));
    }

    /**
     * Actual implementation of the action. Statically available to support embedded mode.
     */
    public static void operation(PlanExecutor planExecutor, SqlQueryRequest request, ActionListener<SqlQueryResponse> listener,
                                 String username, String clusterName) {
        // The configuration is always created however when dealing with the next page, only the timeouts are relevant
        // the rest having default values (since the query is already created)
        Configuration cfg = new Configuration(request.zoneId(), request.fetchSize(), request.requestTimeout(), request.pageTimeout(),
                request.filter(), request.mode(), request.clientId(), username, clusterName, request.fieldMultiValueLeniency(),
                request.indexIncludeFrozen());

        if (Strings.hasText(request.cursor()) == false) {
            planExecutor.sql(cfg, request.query(), request.params(),
                    wrap(p -> listener.onResponse(createResponseWithSchema(request, p)), listener::onFailure));
        } else {
            planExecutor.nextPage(cfg, Cursors.decodeFromString(request.cursor()),
                    wrap(p -> listener.onResponse(createResponse(request, null, p)),
                            listener::onFailure));
        }
    }

    static SqlQueryResponse createResponseWithSchema(SqlQueryRequest request, Page page) {
        RowSet rset = page.rowSet();
        if ((rset instanceof SchemaRowSet) == false) {
            throw new SqlIllegalArgumentException("No schema found inside {}", rset.getClass());
        }
        SchemaRowSet rowSet = (SchemaRowSet) rset;

        List<ColumnInfo> columns = new ArrayList<>(rowSet.columnCount());
        for (Schema.Entry entry : rowSet.schema()) {
            if (Mode.isDriver(request.mode())) {
                columns.add(new ColumnInfo("", entry.name(), entry.type().typeName(), entry.type().displaySize));
            } else {
                columns.add(new ColumnInfo("", entry.name(), entry.type().typeName()));
            }
        }
        columns = unmodifiableList(columns);
        return createResponse(request, columns, page);
    }

    static SqlQueryResponse createResponse(SqlQueryRequest request, List<ColumnInfo> header, Page page) {
        List<List<Object>> rows = new ArrayList<>();
        page.rowSet().forEachRow(rowView -> {
            List<Object> row = new ArrayList<>(rowView.columnCount());
            rowView.forEachColumn(row::add);
            rows.add(unmodifiableList(row));
        });

        return new SqlQueryResponse(
                Cursors.encodeToString(page.next(), request.zoneId()),
                request.mode(),
                request.columnar(),
                header,
                rows);
    }
}
