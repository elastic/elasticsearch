/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.action.SqlQueryAction;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.elasticsearch.xpack.sql.session.RowSet;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.stats.Counters;
import org.elasticsearch.xpack.sql.stats.Metric;
import org.elasticsearch.xpack.sql.type.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.unmodifiableList;

public class TransportSqlQueryAction extends HandledTransportAction<SqlQueryRequest, SqlQueryResponse> {
    private final PlanExecutor planExecutor;
    private final SqlLicenseChecker sqlLicenseChecker;
    private static final Map<Metric, Map<String, CounterMetric>> opsByTypeMetrics = new ConcurrentHashMap<>();
    private final Map<String, CounterMetric> featuresMetrics = new ConcurrentHashMap<>();

    @Inject
    public TransportSqlQueryAction(TransportService transportService, ActionFilters actionFilters,
                                   PlanExecutor planExecutor, SqlLicenseChecker sqlLicenseChecker) {
        super(SqlQueryAction.NAME, transportService, actionFilters, (Writeable.Reader<SqlQueryRequest>) SqlQueryRequest::new);

        this.planExecutor = planExecutor;
        this.sqlLicenseChecker = sqlLicenseChecker;
        
        for (Metric metric : Metric.values()) {
            ConcurrentHashMap<String, CounterMetric> metricsMap = new ConcurrentHashMap<>(3);
            metricsMap.put("total",  new CounterMetric());
            metricsMap.put("failed", new CounterMetric());
            metricsMap.put("paging", new CounterMetric());
            opsByTypeMetrics.put(metric, metricsMap);
        }
        
        featuresMetrics.put("having",    new CounterMetric());
        featuresMetrics.put("groupby",   new CounterMetric());
        featuresMetrics.put("where",     new CounterMetric());
        featuresMetrics.put("orderby",   new CounterMetric());
        featuresMetrics.put("limit",     new CounterMetric());
        featuresMetrics.put("local",     new CounterMetric());
        featuresMetrics.put("command",   new CounterMetric());
        featuresMetrics.put("join",      new CounterMetric());
        featuresMetrics.put("subselect", new CounterMetric());
        this.planExecutor.setFeaturesMetrics(featuresMetrics);
    }

    @Override
    protected void doExecute(Task task, SqlQueryRequest request, ActionListener<SqlQueryResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());
        operation(planExecutor, request, listener);
    }

    /**
     * Actual implementation of the action. Statically available to support embedded mode.
     */
    public static void operation(PlanExecutor planExecutor, SqlQueryRequest request, ActionListener<SqlQueryResponse> listener) {
        // The configuration is always created however when dealing with the next page, only the timeouts are relevant
        // the rest having default values (since the query is already created)
        Configuration cfg = new Configuration(request.timeZone(), request.fetchSize(), request.requestTimeout(), request.pageTimeout(),
                request.filter());
        
        Metric metric = Metric.fromString(request.mode() == null ? null : request.mode().toString());
        opsByTypeMetrics.get(metric).get("total").inc();

        if (Strings.hasText(request.cursor()) == false) {
            planExecutor.sql(cfg, request.query(), request.params(),
                    ActionListener.wrap(rowSet -> listener.onResponse(createResponse(request, rowSet)), listener::onFailure));
        } else {
            opsByTypeMetrics.get(metric).get("paging").inc();
            planExecutor.nextPage(cfg, Cursors.decodeFromString(request.cursor()),
                    ActionListener.wrap(rowSet -> listener.onResponse(createResponse(rowSet, null)), listener::onFailure));
        }
    }

    static SqlQueryResponse createResponse(SqlQueryRequest request, SchemaRowSet rowSet) {
        List<ColumnInfo> columns = new ArrayList<>(rowSet.columnCount());
        for (Schema.Entry entry : rowSet.schema()) {
            if (Mode.isDriver(request.mode())) {
                columns.add(new ColumnInfo("", entry.name(), entry.type().esType, entry.type().jdbcType,
                        entry.type().displaySize));
            } else {
                columns.add(new ColumnInfo("", entry.name(), entry.type().esType));
            }
        }
        columns = unmodifiableList(columns);
        return createResponse(rowSet, columns);
    }

    static SqlQueryResponse createResponse(RowSet rowSet, List<ColumnInfo> columns) {
        List<List<Object>> rows = new ArrayList<>();
        rowSet.forEachRow(rowView -> {
            List<Object> row = new ArrayList<>(rowView.columnCount());
            rowView.forEachColumn(row::add);
            rows.add(unmodifiableList(row));
        });

        return new SqlQueryResponse(
                Cursors.encodeToString(Version.CURRENT, rowSet.nextPageCursor()),
                columns,
                rows);
    }
    
    public Counters stats() {
        Counters counters = new Counters();
        for (Entry<Metric, Map<String, CounterMetric>> entry : opsByTypeMetrics.entrySet()) {
            counters.inc("queries." + entry.getKey().toString() + ".total", entry.getValue().get("total").count());
            counters.inc("queries." + entry.getKey().toString() + ".failed", entry.getValue().get("failed").count());
            counters.inc("queries." + entry.getKey().toString() + ".paging", entry.getValue().get("paging").count());
            counters.inc("queries._all.total", entry.getValue().get("total").count());
            counters.inc("queries._all.failed", entry.getValue().get("failed").count());
            counters.inc("queries._all.paging", entry.getValue().get("paging").count());
        }
        
        counters.inc("features.having",    featuresMetrics.get("having").count());
        counters.inc("features.groupby",   featuresMetrics.get("groupby").count());
        counters.inc("features.where",     featuresMetrics.get("where").count());
        counters.inc("features.orderby",   featuresMetrics.get("orderby").count());
        counters.inc("features.limit",     featuresMetrics.get("limit").count());
        counters.inc("features.local",     featuresMetrics.get("local").count());
        counters.inc("features.command",   featuresMetrics.get("command").count());
        counters.inc("features.join",      featuresMetrics.get("join").count());
        counters.inc("features.subselect", featuresMetrics.get("subselect").count());
        
        return counters;
    }
}
