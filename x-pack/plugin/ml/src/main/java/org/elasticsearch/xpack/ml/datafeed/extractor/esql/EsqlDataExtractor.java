/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;

public class EsqlDataExtractor implements DataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(EsqlDataExtractor.class);

    private static final String EPOCH_MILLIS = "epoch_millis";

    /**
     * The row limit this extractor injects into a user's ES|QL query when it has no explicit outer LIMIT
     * of its own. Once an explicit LIMIT is present in the pipeline (whether the user's or this injected
     * one), ES|QL applies that LIMIT as its row cap instead of its no-limit default
     * ({@code esql.query.result_truncation_default_size}, 1000 rows). Callers that need to detect genuine
     * row-count truncation (e.g. {@code ChunkedDataExtractor}) must compare against this constant, not the
     * unrelated no-limit default, so the two can never drift apart.
     */
    public static final long INJECTED_ROW_LIMIT = 10_000L;
    private static final String DEFAULT_LIMIT = " | LIMIT " + INJECTED_ROW_LIMIT;

    private final Client client;
    private final EsqlDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private boolean hasNext = true;
    private boolean isCancelled = false;
    private final Object cancellationLock = new Object();
    private Task inFlightQueryTask;

    public EsqlDataExtractor(Client client, EsqlDataExtractorContext context, DatafeedTimingStatsReporter timingStatsReporter) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(context);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public void cancel() {
        LOGGER.trace("[{}] Data extractor received cancel request", context.jobId());
        isCancelled = true;
        Task query;
        synchronized (cancellationLock) {
            query = inFlightQueryTask;
            inFlightQueryTask = null;
        }
        cancelQueryTask(query);
    }

    @Override
    public void destroy() {
        cancel();
    }

    @Override
    public long getEndTime() {
        return context.end();
    }

    EsqlDataExtractorContext getContext() {
        return context;
    }

    @Override
    public DataSummary getSummary() {
        String summaryQuery = context.esqlQuery()
            + " | STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)";
        QueryBuilder timeFilter = buildTimeFilter();
        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runEsqlQueryWithSingleRetry(summaryQuery, timeFilter, timeFieldParam())) {
            long durationMs = client.threadPool().relativeTimeInMillis() - startMs;
            timingStatsReporter.reportSearchDuration(TimeValue.timeValueMillis(durationMs));
            return parseSummaryResponse(response.response());
        }
    }

    private QueryBuilder buildTimeFilter() {
        return new RangeQueryBuilder(context.sourceTimeField()).gte(context.start()).lt(context.end()).format(EPOCH_MILLIS);
    }

    private static DataSummary parseSummaryResponse(EsqlResponse response) {
        List<? extends ColumnInfo> columns = response.columns();
        boolean earliestIsDate = isDateType(columns, 0);
        boolean latestIsDate = isDateType(columns, 1);
        for (Iterable<Object> row : response.rows()) {
            List<Object> values = new ArrayList<>();
            for (Object v : row) {
                values.add(v);
            }
            Long earliestTime = toEpochMillisOrNull(values.get(0), earliestIsDate);
            Long latestTime = toEpochMillisOrNull(values.get(1), latestIsDate);
            long totalHits = values.get(2) instanceof Number n ? n.longValue() : 0L;
            return new DataSummary(earliestTime, latestTime, totalHits);
        }
        return new DataSummary(null, null, 0L);
    }

    private static boolean isDateType(List<? extends ColumnInfo> columns, int index) {
        if (index >= columns.size()) {
            return false;
        }
        String type = columns.get(index).outputType();
        return "date".equals(type) || "date_nanos".equals(type);
    }

    private static Long toEpochMillisOrNull(Object value, boolean isDate) {
        if (value == null) {
            return null;
        }
        return isDate ? toEpochMillis((String) value) : ((Number) value).longValue();
    }

    @Override
    public Result next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        hasNext = false;
        SearchInterval searchInterval = new SearchInterval(context.start(), context.end());
        if (isCancelled) {
            return new Result(searchInterval, Optional.empty(), List.of());
        }

        QueryBuilder timeFilter = buildTimeFilter();

        // Anomaly detection drops records that arrive out of time order
        // We add a SORT on the time field to ensure that the data is returned in time order
        String orderedQuery = appendGeneratedPipeline(maybeInjectLimit(context.esqlQuery()), " | SORT ??timeField ASC");

        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runEsqlQueryWithSingleRetry(orderedQuery, timeFilter, timeFieldParam())) {
            long durationMs = client.threadPool().relativeTimeInMillis() - startMs;
            timingStatsReporter.reportSearchDuration(TimeValue.timeValueMillis(durationMs));
            ExtractedData extractedData = toNdjson(
                response.response(),
                context.jobId(),
                context.emittedTimeField(),
                context.start(),
                context.end(),
                context.requiredSummaryCountField()
            );
            return new Result(searchInterval, extractedData.data(), List.of(), extractedData.rowCount());
        }
    }

    protected EsqlQueryResponse runEsqlQuery(String orderedQuery, QueryBuilder timeFilter, List<EsqlQueryParam> params) {
        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request = EsqlQueryRequestBuilder
            .newRequestBuilder(client)
            .query(orderedQuery)
            .filter(timeFilter)
            .params(params)
            .allowPartialResults(false);
        if (context.projectRouting() != null) {
            request.projectRouting(context.projectRouting());
        }
        return execute(request);
    }

    private EsqlQueryResponse runEsqlQueryWithSingleRetry(String query, QueryBuilder timeFilter, List<EsqlQueryParam> params) {
        for (int attempt = 0; attempt < 2; attempt++) {
            if (isCancelled) {
                throw new IllegalStateException("ES|QL query was cancelled");
            }
            try {
                return runEsqlQuery(query, timeFilter, params);
            } catch (RuntimeException e) {
                if (attempt == 0 && isCancelled == false && isNodeChurnFailure(e)) {
                    LOGGER.debug(() -> "[" + context.jobId() + "] ES|QL query failed due to node churn; retrying the same range", e);
                    continue;
                }
                throw e;
            }
        }
        throw new AssertionError("unreachable");
    }

    public static boolean isNodeChurnFailure(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof NodeClosedException
            || cause instanceof NodeDisconnectedException
            || cause instanceof NodeNotConnectedException
            || cause instanceof UnavailableShardsException;
    }

    static String maybeInjectLimit(String query) {
        LimitScan limitScan = scanForOuterLimit(query);
        return limitScan.hasOuterLimit() ? query : appendGeneratedPipeline(query, DEFAULT_LIMIT);
    }

    private static String appendGeneratedPipeline(String query, String pipeline) {
        return query + (endsInLineComment(query) ? "\n" : "") + pipeline;
    }

    /**
     * Identifies an outer LIMIT command without interpreting LIMIT-like text in strings, comments, or identifiers.
     * The full ES|QL parser belongs to the ES|QL plugin, so this deliberately narrow scan only recognizes
     * a depth-zero pipeline command that determines whether the datafeed needs its safety limit.
     */
    private static LimitScan scanForOuterLimit(String query) {
        return new LimitScan(EsqlQueryClauseScanner.scan(query, "").hasOuterLimit());
    }

    private static boolean endsInLineComment(String query) {
        return EsqlQueryClauseScanner.endsInLineComment(query);
    }

    private record LimitScan(boolean hasOuterLimit) {}

    private List<EsqlQueryParam> timeFieldParam() {
        return List.of(new EsqlQueryParam("timeField", context.emittedTimeField(), IDENTIFIER));
    }

    EsqlQueryResponse execute(EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request) {
        NodeClient nodeClient = nodeClient();
        if (nodeClient != null) {
            if (client instanceof ParentTaskAssigningClient parentTaskClient) {
                request.request().setParentTask(parentTaskClient.getParentTask());
            }
            return executeTracked(nodeClient, request);
        }
        return ClientHelper.executeWithHeaders(context.headers(), ClientHelper.ML_ORIGIN, client, () -> request.execute().actionGet());
    }

    private EsqlQueryResponse executeTracked(
        NodeClient nodeClient,
        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request
    ) {
        // Stored datafeed headers must reach _query for the same security behavior as classic extractors.
        return ClientHelper.executeWithHeaders(context.headers(), ClientHelper.ML_ORIGIN, client, () -> {
            var future = new org.elasticsearch.action.support.PlainActionFuture<EsqlQueryResponse>();
            // TransportEsqlQueryAction#doExecute wraps the terminal listener with ActionListener::respondAndRelease,
            // which decRefs the response as soon as onResponse() returns, on the assumption that a synchronous
            // consumer already read it (or took its own reference) before returning. PlainActionFuture#onResponse
            // only stores the reference for a later actionGet() call and does not take a reference of its own, so
            // without incRef-ing here the response is already closed (ref count 0) by the time future.actionGet()
            // hands it back below, and response.response()/response() throws IllegalStateException("closed").
            ActionListener<EsqlQueryResponse> retainingListener = ActionListener.wrap(response -> {
                response.incRef();
                future.onResponse(response);
            }, future::onFailure);
            Task task = executeAndReturnTask(nodeClient, request, retainingListener);
            synchronized (cancellationLock) {
                if (isCancelled) {
                    cancelQueryTask(task);
                } else {
                    inFlightQueryTask = task;
                }
            }
            try {
                return future.actionGet();
            } finally {
                synchronized (cancellationLock) {
                    if (inFlightQueryTask == task) {
                        inFlightQueryTask = null;
                    }
                }
            }
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Task executeAndReturnTask(
        NodeClient client,
        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request,
        ActionListener<EsqlQueryResponse> listener
    ) {
        return client.executeAndReturnTask(request.action(), request.request(), (ActionListener) listener);
    }

    private void cancelQueryTask(Task task) {
        NodeClient nodeClient = nodeClient();
        if (task == null || nodeClient == null) {
            return;
        }
        CancelTasksRequest request = new CancelTasksRequest().setTargetTaskId(new TaskId(nodeClient.getLocalNodeId(), task.getId()));
        request.setReason("datafeed stopped");
        new OriginSettingClient(nodeClient, TASKS_ORIGIN).admin().cluster().cancelTasks(request, ActionListener.noop());
    }

    private NodeClient nodeClient() {
        if (client instanceof NodeClient nodeClient) {
            return nodeClient;
        }
        if (client instanceof ParentTaskAssigningClient parentTaskClient && parentTaskClient.unwrap() instanceof NodeClient nodeClient) {
            return nodeClient;
        }
        return null;
    }

    private static ExtractedData toNdjson(
        EsqlResponse response,
        String jobId,
        String emittedTimeField,
        long sourceWindowStart,
        long sourceWindowEnd,
        String requiredSummaryCountField
    ) throws IOException {
        List<? extends ColumnInfo> columns = response.columns();
        // datafeedId isn't threaded through EsqlDataExtractorContext (extraction runtime path); the message
        // degrades gracefully to the datafeed-agnostic wording in that case.
        EsqlDatafeedQueryValidator.checkRequiredColumns(columns, emittedTimeField, requiredSummaryCountField, null);
        List<List<Object>> materializedRows = new ArrayList<>();
        for (Iterable<Object> row : response.rows()) {
            List<Object> values = new ArrayList<>();
            for (Object value : row) {
                values.add(value);
            }
            materializedRows.add(values);
        }
        EsqlDatafeedQueryValidator.validateEmittedTimesInSourceWindow(
            columns,
            materializedRows,
            jobId,
            emittedTimeField,
            sourceWindowStart,
            sourceWindowEnd
        );
        boolean[] isDateColumn = new boolean[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            String type = columns.get(i).outputType();
            isDateColumn[i] = "date".equals(type) || "date_nanos".equals(type);
        }

        BytesStreamOutput out = new BytesStreamOutput();
        boolean hasRows = false;
        for (List<Object> row : materializedRows) {
            hasRows = true;
            try (XContentBuilder b = XContentFactory.jsonBuilder(out)) {
                writeRow(b, row, columns, isDateColumn);
            }
            out.write('\n');
        }
        return new ExtractedData(hasRows ? Optional.of(out.bytes().streamInput()) : Optional.empty(), materializedRows.size());
    }

    private record ExtractedData(Optional<InputStream> data, long rowCount) {}

    private static void writeRow(XContentBuilder b, Iterable<Object> row, List<? extends ColumnInfo> columns, boolean[] isDateColumn)
        throws IOException {
        b.startObject();
        int columnIndex = 0;
        for (Object value : row) {
            if (value != null) {
                writeField(b, columns.get(columnIndex).name(), value, isDateColumn[columnIndex]);
            }
            columnIndex++;
        }
        b.endObject();
    }

    private static void writeField(XContentBuilder b, String name, Object value, boolean isDate) throws IOException {
        if (isDate) {
            if (value instanceof List<?> list) {
                List<Long> epochMillis = new ArrayList<>(list.size());
                for (Object element : list) {
                    epochMillis.add(toEpochMillis((String) element));
                }
                b.field(name, epochMillis);
            } else {
                b.field(name, toEpochMillis((String) value));
            }
        } else if (value instanceof List<?> list) {
            b.field(name, list);
        } else {
            b.field(name, value);
        }
    }

    private static long toEpochMillis(String isoDate) {
        return Instant.parse(isoDate).toEpochMilli();
    }
}
