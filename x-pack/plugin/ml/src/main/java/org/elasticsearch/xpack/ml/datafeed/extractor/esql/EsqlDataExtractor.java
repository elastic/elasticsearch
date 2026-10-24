/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
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

    private static final String EPOCH_MILLIS = "epoch_millis";

    /**
     * Injected row cap when the user query has no outer LIMIT; chunking truncation checks must use this value,
     * not {@code esql.query.result_truncation_default_size}.
     */
    public static final long INJECTED_ROW_LIMIT = 10_000L;

    private static final String DEFAULT_LIMIT = " | LIMIT " + INJECTED_ROW_LIMIT;
    private static final String TIME_SORT = " | SORT ??timeField ASC";
    private static final String SOURCE_RANGE_SUMMARY_STATS =
        " | STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)";
    private static final String PROBE_OUTPUT_ROWS_STATS = " | STATS probe_output_rows = COUNT(*)";

    private final Client client;
    private final EsqlDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private boolean hasNext = true;
    private volatile boolean isCancelled = false;
    private final AtomicReference<Task> inFlightQueryTask = new AtomicReference<>();

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
        // Key off hasData() (null MIN/MAX with positive COUNT(*) is possible); estimateAggregatingOutputRows needs times.
        if (sourceSummary.hasData() == false || EsqlQueryClauseScanner.hasAggregation(context.esqlQuery()) == false) {
            return new DataSummary(sourceSummary.earliestTime(), sourceSummary.latestTime(), sourceSummary.totalHits());
        }
        long estimatedOutputRows = estimateAggregatingOutputRows(sourceSummary);
        return new DataSummary(sourceSummary.earliestTime(), sourceSummary.latestTime(), estimatedOutputRows);
    }

    /** Source-level MIN/MAX/COUNT probe on {@link EsqlQueryClauseScanner#extractLeadingCommand}, not the user pipeline. */
    private SourceRangeSummary fetchSourceRangeSummary(QueryBuilder timeFilter) {
        String sourceQuery = EsqlQueryClauseScanner.appendGeneratedPipeline(
            EsqlQueryClauseScanner.extractLeadingCommand(context.esqlQuery()).stripTrailing(),
            SOURCE_RANGE_SUMMARY_STATS
        );
        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runEsqlQueryWithSingleRetry(sourceQuery, timeFilter, sourceTimeFieldParam())) {
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
        QueryBuilder probeFilter = new RangeQueryBuilder(context.sourceTimeField()).gte(probeStart).lt(probeEnd).format(EPOCH_MILLIS);
        String probeQuery = EsqlQueryClauseScanner.appendGeneratedPipeline(context.esqlQuery(), PROBE_OUTPUT_ROWS_STATS);
        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runEsqlQueryWithSingleRetry(probeQuery, probeFilter, List.of())) {
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
            return earliestTime != null;
        }
    }

    private QueryBuilder buildTimeFilter() {
        return new RangeQueryBuilder(context.sourceTimeField()).gte(context.start()).lt(context.end()).format(EPOCH_MILLIS);
    }

    private static DataSummary parseSummaryResponse(EsqlResponse response) {
        List<? extends ColumnInfo> columns = response.columns();
        boolean earliestIsDate = columns.isEmpty() == false && EsqlDatafeedColumnValues.isDateColumnType(columns.get(0).outputType());
        boolean latestIsDate = columns.size() > 1 && EsqlDatafeedColumnValues.isDateColumnType(columns.get(1).outputType());
        for (Iterable<Object> row : response.rows()) {
            List<Object> values = new ArrayList<>();
            for (Object v : row) {
                values.add(v);
            }
            Long earliestTime = EsqlDatafeedColumnValues.toEpochMillisOrNull(values.get(0), earliestIsDate);
            Long latestTime = EsqlDatafeedColumnValues.toEpochMillisOrNull(values.get(1), latestIsDate);
            long totalHits = values.get(2) instanceof Number n ? n.longValue() : 0L;
            return new DataSummary(earliestTime, latestTime, totalHits);
        }
        return new DataSummary(null, null, 0L);
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

        String orderedQuery = buildOrderedQuery(context.esqlQuery());

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
        return execute(EsqlQueryExecutor.newRequest(client, orderedQuery, timeFilter, params, context.projectRouting()));
    }

    private EsqlQueryResponse runEsqlQueryWithSingleRetry(String query, QueryBuilder timeFilter, List<EsqlQueryParam> params) {
        throwIfCancelled();
        try {
            return runEsqlQuery(query, timeFilter, params);
        } catch (NodeClosedException | NodeDisconnectedException | NodeNotConnectedException | UnavailableShardsException e) {
            return retryAfterNodeFailure(query, timeFilter, params, e);
        } catch (ElasticsearchException e) {
            if (isRetryableNodeFailure(e) == false) {
                throw e;
            }
            return retryAfterNodeFailure(query, timeFilter, params, e);
        }
    }

    private EsqlQueryResponse retryAfterNodeFailure(
        String query,
        QueryBuilder timeFilter,
        List<EsqlQueryParam> params,
        Exception nodeFailure
    ) {
        if (isCancelled) {
            if (nodeFailure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new ElasticsearchException(nodeFailure);
        }
        LOGGER.debug(
            () -> "[" + context.jobId() + "] ES|QL query failed due to a node or shard availability failure; retrying the same range",
            nodeFailure
        );
        throwIfCancelled();
        return runEsqlQuery(query, timeFilter, params);
    }

    private void throwIfCancelled() {
        if (isCancelled) {
            throw new IllegalStateException(Messages.getMessage(Messages.DATAFEED_ESQL_QUERY_CANCELLED));
        }
    }

    public static boolean isRetryableNodeFailure(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        return cause instanceof NodeClosedException
            || cause instanceof NodeDisconnectedException
            || cause instanceof NodeNotConnectedException
            || cause instanceof UnavailableShardsException;
    }

    /**
     * Appends {@code SORT} on the time field and, when missing, {@link #INJECTED_ROW_LIMIT} after sort so capped
     * chunks keep the earliest rows of the interval.
     */
    static String buildOrderedQuery(String query) {
        String sorted = EsqlQueryClauseScanner.appendGeneratedPipeline(query, TIME_SORT);
        return EsqlQueryClauseScanner.hasOuterLimit(query) ? sorted : sorted + DEFAULT_LIMIT;
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

    private static ExtractedData toNdjson(
        EsqlResponse response,
        String jobId,
        String emittedTimeField,
        long sourceWindowStart,
        long sourceWindowEnd,
        String requiredSummaryCountField
    ) throws IOException {
        List<? extends ColumnInfo> columns = response.columns();
        // Runtime extraction has no datafeed id for column-check messages.
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
            isDateColumn[i] = EsqlDatafeedColumnValues.isDateColumnType(columns.get(i).outputType());
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
                    epochMillis.add(EsqlDatafeedColumnValues.toEpochMillis(element, true));
                }
                b.field(name, epochMillis);
            } else {
                b.field(name, EsqlDatafeedColumnValues.toEpochMillis(value, true));
            }
        } else if (value instanceof List<?> list) {
            b.field(name, list);
        } else {
            b.field(name, value);
        }
    }

}
