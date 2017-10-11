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
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse.ColumnInfo;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.SqlSettings;
import org.elasticsearch.xpack.sql.type.Schema;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class TransportSqlAction extends HandledTransportAction<SqlRequest, SqlResponse> {
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;

    @Inject
    public TransportSqlAction(Settings settings, ThreadPool threadPool,
                              TransportService transportService, ActionFilters actionFilters,
                              IndexNameExpressionResolver indexNameExpressionResolver,
                              PlanExecutor planExecutor,
                              SqlLicenseChecker sqlLicenseChecker) {
        super(settings, SqlAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, SqlRequest::new);

        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
    }

    @Override
    protected void doExecute(SqlRequest request, ActionListener<SqlResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed();
        operation(planExecutor, request, listener);
    }

    /**
     * Actual implementation of the action. Statically available to support embedded mode.
     */
    public static void operation(PlanExecutor planExecutor, SqlRequest request, ActionListener<SqlResponse> listener) {
        if (request.cursor() == Cursor.EMPTY) {
            SqlSettings sqlSettings = new SqlSettings(Settings.builder()
                    .put(SqlSettings.PAGE_SIZE, request.fetchSize())
                    .put(SqlSettings.TIMEZONE_ID, request.timeZone().getID()).build());
            planExecutor.sql(sqlSettings, request.query(),
                    ActionListener.wrap(cursor -> listener.onResponse(createResponse(true, cursor)), listener::onFailure));
        } else {
            planExecutor.nextPage(request.cursor(),
                    ActionListener.wrap(cursor -> listener.onResponse(createResponse(false, cursor)), listener::onFailure));
        }
    }

    static SqlResponse createResponse(boolean includeColumnMetadata, RowSet cursor) {
        List<ColumnInfo> columns = null;
        if (includeColumnMetadata) {
            columns = new ArrayList<>(cursor.schema().types().size());
            for (Schema.Entry entry : cursor.schema()) {
                columns.add(new ColumnInfo(entry.name(), entry.type().esName(), entry.type().sqlType(), entry.type().displaySize()));
            }
            columns = unmodifiableList(columns);
        }

        List<List<Object>> rows = new ArrayList<>();
        cursor.forEachRow(rowView -> {
            List<Object> row = new ArrayList<>(rowView.rowSize());
            rowView.forEachColumn(row::add);
            rows.add(unmodifiableList(row));
        });

        return new SqlResponse(
                cursor.nextPageCursor(),
                cursor.size(),
                cursor.rowSize(),
                columns,
                rows);
    }
}