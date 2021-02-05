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
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.VersionMismatchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.sql.plugin.Transports.clusterName;
import static org.elasticsearch.xpack.sql.plugin.Transports.username;
import static org.elasticsearch.xpack.sql.proto.Mode.CLI;

public class TransportSqlQueryAction extends HandledTransportAction<SqlQueryRequest, SqlQueryResponse> {
    private static final Logger log = LogManager.getLogger(TransportSqlQueryAction.class);
    private final SecurityContext securityContext;
    private final ClusterService clusterService;
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;
    private final TransportService transportService;

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
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, SqlQueryRequest request, ActionListener<SqlQueryResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());
        operation(planExecutor, request, listener, username(securityContext), clusterName(clusterService), transportService,
            clusterService);
    }

    /**
     * Actual implementation of the action. Statically available to support embedded mode.
     */
    static void operation(PlanExecutor planExecutor, SqlQueryRequest request, ActionListener<SqlQueryResponse> listener,
                                 String username, String clusterName, TransportService transportService, ClusterService clusterService) {
        // The configuration is always created however when dealing with the next page, only the timeouts are relevant
        // the rest having default values (since the query is already created)
        SqlConfiguration cfg = new SqlConfiguration(request.zoneId(), request.fetchSize(), request.requestTimeout(), request.pageTimeout(),
                request.filter(), request.mode(), request.clientId(), request.version(), username, clusterName,
                request.fieldMultiValueLeniency(), request.indexIncludeFrozen());

        if (Strings.hasText(request.cursor()) == false) {
            Holder<Boolean> retrySecondTime = new Holder<Boolean>(false);
            planExecutor.sql(cfg, request.query(), request.params(),
                wrap(p -> listener.onResponse(createResponseWithSchema(request, p)), e -> {
                    // the search request will likely run on nodes with different versions of ES
                    // we will retry on a node with an older version that should generate a backwards compatible _search request
                    if (e instanceof SearchPhaseExecutionException
                        && ((SearchPhaseExecutionException) e).getCause() instanceof VersionMismatchException) {

                        SearchPhaseExecutionException spee = (SearchPhaseExecutionException) e;
                        if (log.isTraceEnabled()) {
                            log.trace("Caught exception type [{}] with cause [{}].", e.getClass().getName(), e.getCause());
                        }
                        DiscoveryNode localNode = clusterService.state().nodes().getLocalNode();
                        DiscoveryNode candidateNode = null;
                        for (DiscoveryNode node : clusterService.state().nodes()) {
                            // find the first node that's older than the current node
                            if (node != localNode && node.getVersion().before(localNode.getVersion())) {
                                candidateNode = node;
                                break;
                            }
                        }
                        if (candidateNode != null) {
                            if (log.isTraceEnabled()) {
                                log.trace("Candidate node to resend the request to: address [{}], id [{}], name [{}], version [{}]",
                                    candidateNode.getAddress(), candidateNode.getId(), candidateNode.getName(), candidateNode.getVersion());
                            }
                            // re-send the request to the older node
                            transportService.sendRequest(candidateNode, SqlQueryAction.NAME, request,
                                new ActionListenerResponseHandler<>(listener, SqlQueryResponse::new, ThreadPool.Names.SAME));
                        } else {
                            if (log.isTraceEnabled()) {
                                log.trace("No candidate node found, likely all were upgraded in the meantime");
                            }
                            retrySecondTime.set(true);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }));
            if (retrySecondTime.get()) {
                if (log.isTraceEnabled()) {
                    log.trace("No candidate node found, likely all were upgraded in the meantime. Re-trying the original request.");
                }
                planExecutor.sql(cfg, request.query(), request.params(),
                    wrap(p -> listener.onResponse(createResponseWithSchema(request, p)), listener::onFailure));
            }
        } else {
            Tuple<Cursor, ZoneId> decoded = Cursors.decodeFromStringWithZone(request.cursor());
            planExecutor.nextPage(cfg, decoded.v1(),
                    wrap(p -> listener.onResponse(createResponse(request, decoded.v2(), null, p)),
                            listener::onFailure));
        }
    }

    private static SqlQueryResponse createResponseWithSchema(SqlQueryRequest request, Page page) {
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
        return createResponse(request, request.zoneId(), columns, page);
    }

    private static SqlQueryResponse createResponse(SqlQueryRequest request, ZoneId zoneId, List<ColumnInfo> header, Page page) {
        List<List<Object>> rows = new ArrayList<>();
        page.rowSet().forEachRow(rowView -> {
            List<Object> row = new ArrayList<>(rowView.columnCount());
            rowView.forEachColumn(r -> row.add(value(r, request.mode())));
            rows.add(unmodifiableList(row));
        });

        return new SqlQueryResponse(
                Cursors.encodeToString(page.next(), zoneId),
                request.mode(),
                request.version(),
                request.columnar(),
                header,
                rows);
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
}
