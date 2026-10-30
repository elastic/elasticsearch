/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
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
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;

public class EsqlDataExtractor implements DataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(EsqlDataExtractor.class);

    /**
     * Injected row cap when the user query has no outer LIMIT; chunking truncation checks must use this value,
     * not {@code esql.query.result_truncation_default_size}.
     */
    public static final long INJECTED_ROW_LIMIT = EsqlDataExtractorQueryUtils.INJECTED_ROW_LIMIT;

    /** Delegates to {@link EsqlDataExtractorQueryUtils#injectsRowLimit(String)}. */
    public static boolean injectsRowLimit(String esqlQuery) {
        return EsqlDataExtractorQueryUtils.injectsRowLimit(esqlQuery);
    }

    /** Whether the user's pipeline contains a depth-zero {@code STATS} command (aggregating query). */
    public static boolean hasAggregation(String esqlQuery) {
        return EsqlQueryClauseScanner.hasAggregation(esqlQuery);
    }

    private final Client client;
    private final EsqlDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private final EsqlSyncQueryRunner syncQueryRunner;
    private boolean hasNext = true;
    private volatile boolean isCancelled = false;
    private final AtomicReference<Task> inFlightQueryTask = new AtomicReference<>();

    public EsqlDataExtractor(Client client, EsqlDataExtractorContext context, DatafeedTimingStatsReporter timingStatsReporter) {
        this(client, context, timingStatsReporter, null);
    }

    EsqlDataExtractor(
        Client client,
        EsqlDataExtractorContext context,
        DatafeedTimingStatsReporter timingStatsReporter,
        @Nullable EsqlSyncQueryRunner syncQueryRunner
    ) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(context);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
        this.syncQueryRunner = syncQueryRunner != null ? syncQueryRunner : this::executeDefaultSyncQuery;
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
        cancelQueryTask(inFlightQueryTask.getAndSet(null));
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

    /** Chunking summary from a leading FROM/TS probe; aggregating queries use {@link #estimateAggregatingOutputRows}. */
    @Override
    public DataSummary getSummary() {
        QueryBuilder timeFilter = buildTimeFilter();
        SourceRangeSummary sourceSummary = fetchSourceRangeSummary(timeFilter);
        if (sourceSummary.hasData() == false || EsqlQueryClauseScanner.hasAggregation(context.esqlQuery()) == false) {
            return new DataSummary(sourceSummary.earliestTime(), sourceSummary.latestTime(), sourceSummary.totalHits());
        }
        if (sourceSummary.earliestTime() == null) {
            // COUNT(*) can be positive while MIN/MAX(??timeField) are null when every doc lacks the source time field.
            return new DataSummary(null, null, sourceSummary.totalHits());
        }
        long estimatedOutputRows = estimateAggregatingOutputRows(sourceSummary);
        return new DataSummary(sourceSummary.earliestTime(), sourceSummary.latestTime(), estimatedOutputRows);
    }

    /** Source-level MIN/MAX/COUNT probe on {@link EsqlQueryClauseScanner#extractLeadingCommand}, not the user pipeline. */
    private SourceRangeSummary fetchSourceRangeSummary(QueryBuilder timeFilter) {
        String sourceQuery = EsqlDataExtractorQueryUtils.sourceRangeSummaryQuery(context.esqlQuery());
        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runQueryWithSingleRetry(sourceQuery, timeFilter, sourceTimeFieldParam())) {
            long durationMs = client.threadPool().relativeTimeInMillis() - startMs;
            timingStatsReporter.reportSearchDuration(TimeValue.timeValueMillis(durationMs));
            DataSummary summary = parseSummaryResponse(response.response());
            return new SourceRangeSummary(summary.earliestTime(), summary.latestTime(), summary.totalHits());
        }
    }

    /** Bounded pipeline probe after {@code earliestTime}; extrapolates row density unless the probe spans the range. */
    private long estimateAggregatingOutputRows(SourceRangeSummary sourceSummary) {
        long earliest = sourceSummary.earliestTime();
        long latest = sourceSummary.latestTime();
        long timeSpread = latest - earliest;
        assert timeSpread >= 0 : "latest_time must not precede earliest_time";
        if (timeSpread == 0) {
            // All matching data falls at a single instant; the probe below already covers everything.
            return Math.max(1L, runBoundedAggregationProbe(earliest, latest + 1));
        }
        assert context.groupingIntervalMillis() > 0 : "grouping_interval must be positive";
        long probeSpan = Math.min(timeSpread, context.groupingIntervalMillis());
        long probeEnd = Math.min(earliest + probeSpan, latest + 1);
        long probeOutputRows = runBoundedAggregationProbe(earliest, probeEnd);
        long actualProbeSpan = probeEnd - earliest;
        if (probeOutputRows == 0 || actualProbeSpan >= timeSpread) {
            // Do not extrapolate zero density (sparse data) or when the probe already covers the range.
            return Math.max(1L, probeOutputRows);
        }
        double density = (double) probeOutputRows / actualProbeSpan;
        return Math.max(1L, Math.round(density * timeSpread));
    }

    /**
     * Runs the user's actual pipeline, bounded to [probeStart, probeEnd), appending a trailing row-count
     * STATS. Used only to measure output-row density over a small window, not over the full extraction range.
     */
    private long runBoundedAggregationProbe(long probeStart, long probeEnd) {
        QueryBuilder probeFilter = EsqlDataExtractorQueryUtils.timeRangeFilter(context.sourceTimeField(), probeStart, probeEnd);
        String probeQuery = EsqlDataExtractorQueryUtils.boundedProbeQuery(context.esqlQuery());
        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runQueryWithSingleRetry(probeQuery, probeFilter, List.of())) {
            long durationMs = client.threadPool().relativeTimeInMillis() - startMs;
            timingStatsReporter.reportSearchDuration(TimeValue.timeValueMillis(durationMs));
            for (Iterable<Object> row : response.response().rows()) {
                for (Object value : row) {
                    return value instanceof Number n ? n.longValue() : 0L;
                }
            }
            return 0L;
        }
    }

    private record SourceRangeSummary(Long earliestTime, Long latestTime, long totalHits) {
        boolean hasData() {
            return totalHits > 0;
        }
    }

    private QueryBuilder buildTimeFilter() {
        return EsqlDataExtractorQueryUtils.timeRangeFilter(context.sourceTimeField(), context.start(), context.end());
    }

    private static DataSummary parseSummaryResponse(EsqlResponse response) {
        List<? extends ColumnInfo> columns = response.columns();
        boolean earliestIsDate = columns.isEmpty() == false && EsqlDatafeedColumnValues.isDateColumnType(columns.get(0).outputType());
        boolean latestIsDate = columns.size() > 1 && EsqlDatafeedColumnValues.isDateColumnType(columns.get(1).outputType());
        var rowIterator = response.rows().iterator();
        if (rowIterator.hasNext() == false) {
            return new DataSummary(null, null, 0L);
        }
        Iterable<Object> row = rowIterator.next();
        assert rowIterator.hasNext() == false : "source range summary probe must return exactly one row";
        List<Object> values = new ArrayList<>();
        for (Object v : row) {
            values.add(v);
        }
        Long earliestTime = EsqlDatafeedColumnValues.toEpochMillisOrNull(values.get(0), earliestIsDate);
        Long latestTime = EsqlDatafeedColumnValues.toEpochMillisOrNull(values.get(1), latestIsDate);
        long totalHits = values.get(2) instanceof Number n ? n.longValue() : 0L;
        return new DataSummary(earliestTime, latestTime, totalHits);
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

        String orderedQuery = EsqlDataExtractorQueryUtils.buildOrderedQuery(context.esqlQuery());

        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runQueryWithSingleRetry(orderedQuery, timeFilter, timeFieldParam())) {
            long durationMs = client.threadPool().relativeTimeInMillis() - startMs;
            timingStatsReporter.reportSearchDuration(TimeValue.timeValueMillis(durationMs));
            Optional<InputStream> data = Optional.empty();
            long rowCount = 0L;
            EsqlResponse esqlResponse = response.response();
            List<? extends ColumnInfo> columns = esqlResponse.columns();
            // Runtime extraction has no datafeed id for column-check messages.
            EsqlDatafeedQueryValidator.checkRequiredColumns(columns, context.emittedTimeField(), context.requiredSummaryCountField(), null);
            List<List<Object>> materializedRows = new ArrayList<>();
            for (Iterable<Object> row : esqlResponse.rows()) {
                List<Object> values = new ArrayList<>();
                for (Object value : row) {
                    values.add(value);
                }
                materializedRows.add(values);
            }
            EsqlDatafeedQueryValidator.validateEmittedTimesInSourceWindow(
                columns,
                materializedRows,
                context.jobId(),
                context.emittedTimeField(),
                context.start(),
                context.end()
            );
            BytesStreamOutput out = new BytesStreamOutput();
            try (EsqlToJsonProcessor processor = new EsqlToJsonProcessor(columns, out)) {
                for (List<Object> row : materializedRows) {
                    processor.process(row);
                }
                rowCount = processor.getWrittenDocumentCount();
            }
            if (rowCount > 0) {
                data = Optional.of(out.bytes().streamInput());
            }
            return new Result(searchInterval, data, List.of(), rowCount);
        }
    }

    private EsqlQueryResponse runQueryWithSingleRetry(String query, QueryBuilder timeFilter, List<EsqlQueryParam> params) {
        return EsqlQueryRunner.runWithSingleRetry(syncQueryRunner, query, timeFilter, params, () -> isCancelled, context.jobId());
    }

    private EsqlQueryResponse executeDefaultSyncQuery(String orderedQuery, QueryBuilder timeFilter, List<EsqlQueryParam> params) {
        return execute(EsqlQueryExecutor.newRequest(client, orderedQuery, timeFilter, params, context.projectRouting()));
    }

    private List<EsqlQueryParam> timeFieldParam() {
        return List.of(new EsqlQueryParam("timeField", context.emittedTimeField(), IDENTIFIER));
    }

    /** Binds {@code ??timeField} to {@link EsqlDataExtractorContext#sourceTimeField()} for pre-pipeline probes. */
    private List<EsqlQueryParam> sourceTimeFieldParam() {
        return List.of(new EsqlQueryParam("timeField", context.sourceTimeField(), IDENTIFIER));
    }

    EsqlQueryResponse execute(EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request) {
        NodeClient nodeClient = nodeClient();
        if (nodeClient != null) {
            if (client instanceof ParentTaskAssigningClient parentTaskClient) {
                request.request().setParentTask(parentTaskClient.getParentTask());
            }
            return executeTracked(nodeClient, request);
        }
        return EsqlQueryExecutor.executeSync(client, request, context.headers());
    }

    private EsqlQueryResponse executeTracked(
        NodeClient nodeClient,
        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request
    ) {
        // Stored datafeed headers must reach _query for the same security behavior as classic extractors.
        return ClientHelper.executeWithHeaders(context.headers(), ClientHelper.ML_ORIGIN, client, () -> {
            var future = new PlainActionFuture<EsqlQueryResponse>();
            // Unlike the Client#execute future used in execute(), executeAndReturnTask listeners get a response that is released
            // when the listener returns, so take a reference for the caller, who closes the response.
            ActionListener<EsqlQueryResponse> retainingListener = ActionListener.wrap(response -> {
                response.incRef();
                future.onResponse(response);
            }, future::onFailure);
            Task task = executeAndReturnTask(nodeClient, request, retainingListener);
            // Publish before re-checking isCancelled: cancel() sets the flag before draining, so one side always sees the other.
            inFlightQueryTask.set(task);
            if (isCancelled) {
                cancelQueryTask(inFlightQueryTask.getAndSet(null));
            }
            try {
                return future.actionGet();
            } finally {
                inFlightQueryTask.compareAndSet(task, null);
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
        new OriginSettingClient(nodeClient, TASKS_ORIGIN).admin()
            .cluster()
            .cancelTasks(
                request,
                ActionListener.wrap(
                    r -> {},
                    e -> LOGGER.debug(() -> "[" + context.jobId() + "] Failed to cancel in-flight ES|QL query task", e)
                )
            );
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
}
