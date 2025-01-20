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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ql.async.AsyncTaskManagementService;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.action.SqlQueryTask;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.expression.literal.geo.GeoShape;
import org.elasticsearch.xpack.sql.expression.literal.interval.Interval;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Cursor.Page;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.sql.plugin.Transports.clusterName;
import static org.elasticsearch.xpack.sql.plugin.Transports.username;
import static org.elasticsearch.xpack.sql.proto.Mode.CLI;

public final class TransportSqlQueryAction extends HandledTransportAction<SqlQueryRequest, SqlQueryResponse>
    implements
        AsyncTaskManagementService.AsyncOperation<SqlQueryRequest, SqlQueryResponse, SqlQueryTask> {

    private static final Logger log = LogManager.getLogger(TransportSqlQueryAction.class);
    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;
    private final TransportService transportService;
    private final AsyncTaskManagementService<SqlQueryRequest, SqlQueryResponse, SqlQueryTask> asyncTaskManagementService;

    @Inject
    public TransportSqlQueryAction(
        Settings settings,
        ClusterService clusterService,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        SqlLicenseChecker sqlLicenseChecker,
        BigArrays bigArrays
    ) {
        super(SqlQueryAction.NAME, transportService, actionFilters, SqlQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.clusterService = clusterService;
        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
        this.transportService = transportService;

        asyncTaskManagementService = new AsyncTaskManagementService<>(
            XPackPlugin.ASYNC_RESULTS_INDEX,
            planExecutor.client(),
            ASYNC_SEARCH_ORIGIN,
            planExecutor.writeableRegistry(),
            taskManager,
            SqlQueryAction.INSTANCE.name(),
            this,
            SqlQueryTask.class,
            clusterService,
            threadPool,
            bigArrays
        );
    }

    @Override
    protected void doExecute(Task task, SqlQueryRequest request, ActionListener<SqlQueryResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());
        if (request.waitForCompletionTimeout() != null && request.waitForCompletionTimeout().getMillis() >= 0) {
            asyncTaskManagementService.asyncExecute(
                request,
                request.waitForCompletionTimeout(),
                request.keepAlive(),
                request.keepOnCompletion(),
                listener
            );
        } else {
            operation(planExecutor, (SqlQueryTask) task, request, listener, username(securityContext), transportService, clusterService);
        }
    }

    /**
     * Actual implementation of the action. Statically available to support embedded mode.
     */
    public static void operation(
        PlanExecutor planExecutor,
        SqlQueryTask task,
        SqlQueryRequest request,
        ActionListener<SqlQueryResponse> listener,
        String username,
        TransportService transportService,
        ClusterService clusterService
    ) {
        // The configuration is always created however when dealing with the next page, only the timeouts are relevant
        // the rest having default values (since the query is already created)
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
            username,
            clusterName(clusterService),
            request.fieldMultiValueLeniency(),
            request.indexIncludeFrozen(),
            new TaskId(clusterService.localNode().getId(), task.getId()),
            task,
            request.allowPartialSearchResults()
        );

        if (Strings.hasText(request.cursor()) == false) {
            planExecutor.sql(
                cfg,
                request.query(),
                request.params(),
                wrap(p -> listener.onResponse(createResponseWithSchema(request, p, task)), listener::onFailure)
            );
        } else {
            Tuple<Cursor, ZoneId> decoded = Cursors.decodeFromStringWithZone(request.cursor(), planExecutor.writeableRegistry());
            planExecutor.nextPage(
                cfg,
                decoded.v1(),
                listener.delegateFailureAndWrap((l, p) -> l.onResponse(createResponse(request, decoded.v2(), null, p, task)))
            );
        }
    }

    private static SqlQueryResponse createResponseWithSchema(SqlQueryRequest request, Page page, SqlQueryTask task) {
        RowSet rset = page.rowSet();
        if ((rset instanceof SchemaRowSet) == false) {
            throw new SqlIllegalArgumentException("No schema found inside {}", rset.getClass());
        }
        SchemaRowSet rowSet = (SchemaRowSet) rset;

        List<ColumnInfo> columns = new ArrayList<>(rowSet.columnCount());
        for (Schema.Entry entry : rowSet.schema()) {
            if (Mode.isDriver(request.mode())) {
                columns.add(new ColumnInfo("", entry.name(), entry.type().typeName(), SqlDataTypes.displaySize(entry.type())));
            } else {
                columns.add(new ColumnInfo("", entry.name(), entry.type().typeName()));
            }
        }
        columns = unmodifiableList(columns);
        return createResponse(request, request.zoneId(), columns, page, task);
    }

    private static SqlQueryResponse createResponse(
        SqlQueryRequest request,
        ZoneId zoneId,
        List<ColumnInfo> header,
        Page page,
        SqlQueryTask task
    ) {
        List<List<Object>> rows = new ArrayList<>();
        page.rowSet().forEachRow(rowView -> {
            List<Object> row = new ArrayList<>(rowView.columnCount());
            rowView.forEachColumn(r -> row.add(value(r, request.mode())));
            rows.add(unmodifiableList(row));
        });

        AsyncExecutionId executionId = task.getExecutionId();
        return new SqlQueryResponse(
            Cursors.encodeToString(page.next(), zoneId),
            request.mode(),
            request.version(),
            request.columnar(),
            header,
            rows,
            executionId == null ? null : executionId.getEncoded(),
            false,
            false
        );
    }

    @SuppressWarnings("rawtypes")
    private static Object value(Object r, Mode mode) {
        /*
         * Intervals and GeoShape instances need to be serialized (as in StreamInput/Ouput serialization) as Strings
         * since SqlQueryResponse creation doesn't have access to GeoShape nor Interval classes to make the decision
         * so, we flatten them as Strings before being serialized.
         * CLI gets a special treatment see {@link org.elasticsearch.xpack.sql.action.SqlQueryResponse#value()}
         */
        if (r instanceof GeoShape) {
            r = r.toString();
        } else if (r instanceof Interval) {
            if (mode == CLI) {
                r = r.toString();
            } else {
                r = ((Interval) r).value();
            }
        }

        return r;
    }

    @Override
    public SqlQueryTask createTask(
        SqlQueryRequest request,
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        Map<String, String> headers,
        Map<String, String> originHeaders,
        AsyncExecutionId asyncExecutionId
    ) {
        return new SqlQueryTask(
            id,
            type,
            action,
            request.getDescription(),
            parentTaskId,
            headers,
            originHeaders,
            asyncExecutionId,
            request.keepAlive(),
            request.mode(),
            request.version(),
            request.columnar()
        );
    }

    @Override
    public void execute(SqlQueryRequest request, SqlQueryTask task, ActionListener<SqlQueryResponse> listener) {
        operation(planExecutor, task, request, listener, username(securityContext), transportService, clusterService);
    }

    @Override
    public SqlQueryResponse initialResponse(SqlQueryTask task) {
        return task.getCurrentResult();
    }

    @Override
    public SqlQueryResponse readResponse(StreamInput inputStream) throws IOException {
        return new SqlQueryResponse(inputStream);
    }
}
