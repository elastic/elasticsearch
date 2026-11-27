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
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.ExecutionMetadata;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.LinkedClusterState;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction.TASKS_ORIGIN;
import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;

public class EsqlDataExtractor implements DataExtractor {

    private static final Logger LOGGER = LogManager.getLogger(EsqlDataExtractor.class);

    /** Row cap appended to every extraction query; see {@link EsqlDataExtractorQueryUtils#INJECTED_ROW_LIMIT}. */
    public static final long INJECTED_ROW_LIMIT = EsqlDataExtractorQueryUtils.INJECTED_ROW_LIMIT;

    private final Client client;
    private final EsqlDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private final EsqlSyncQueryRunner syncQueryRunner;
    private boolean hasNext = true;
    private volatile List<LinkedClusterState> lastLinkedClusterStates = List.of();
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

    /**
     * One probe over the output of the user query: earliest and latest emitted time and the number of output rows, used to size
     * lookback chunks. The source time filter applies as for extraction; {@code ??timeField} is the time column.
     */
    @Override
    public DataSummary getSummary() {
        String summaryQuery = EsqlDataExtractorQueryUtils.summaryQuery(context.esqlQuery());
        long startMs = client.threadPool().relativeTimeInMillis();
        try (EsqlQueryResponse response = runQueryWithSingleRetry(summaryQuery, buildTimeFilter(), timeFieldParam())) {
            long durationMs = client.threadPool().relativeTimeInMillis() - startMs;
            timingStatsReporter.reportSearchDuration(TimeValue.timeValueMillis(durationMs));
            recordAndCheckClusters(response.response());
            DataSummary summary = parseSummaryResponse(response.response());
            if (summary.totalHits() > 0 && summary.earliestTime() == null) {
                // Every output row has a null time. Fail as the data query does instead of reporting an empty window.
                EsqlDatafeedQueryValidator.validateEmittedTimeValueInSourceWindow(
                    context.jobId(),
                    context.timeField(),
                    null,
                    context.start(),
                    context.end()
                );
            }
            return summary;
        }
    }

    /** Remembers the remote cluster states for {@link #getLinkedClusterStates}, then fails when a remote cluster was skipped. */
    private void recordAndCheckClusters(EsqlResponse response) {
        lastLinkedClusterStates = DataExtractorUtils.extractLinkedClusterStates(response);
        DataExtractorUtils.checkForSkippedClusters(response);
    }

    @Override
    public List<LinkedClusterState> getLinkedClusterStates() {
        return lastLinkedClusterStates;
    }

    private QueryBuilder buildTimeFilter() {
        return EsqlDataExtractorQueryUtils.timeRangeFilter(context.timeField(), context.start(), context.end());
    }

    private static DataSummary parseSummaryResponse(EsqlResponse response) {
        var rowIterator = response.rows().iterator();
        if (rowIterator.hasNext() == false) {
            return new DataSummary(null, null, 0L);
        }
        Iterable<Object> row = rowIterator.next();
        assert rowIterator.hasNext() == false : "summary probe must return exactly one row";
        List<Object> values = new ArrayList<>();
        for (Object v : row) {
            values.add(v);
        }
        Long earliestTime = EsqlDatafeedColumnValues.toEpochMillisOrNull(values.get(0));
        Long latestTime = EsqlDatafeedColumnValues.toEpochMillisOrNull(values.get(1));
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
            recordAndCheckClusters(esqlResponse);
            List<? extends ColumnInfo> columns = esqlResponse.columns();
            // Runtime extraction has no datafeed id for column-check messages.
            Map<String, String> columnTypes = columns.stream()
                .collect(Collectors.toMap(ColumnInfo::name, ColumnInfo::outputType, (first, second) -> second, LinkedHashMap::new));
            EsqlDatafeedQueryValidator.checkRequiredColumns(columnTypes, context.timeField(), context.requiredSummaryCountField(), null);
            BytesStreamOutput out = new BytesStreamOutput();
            // The processor checks each row's time against the window while it writes the row, so the rows are read only once.
            EsqlToJsonProcessor.TimeWindow timeWindow = new EsqlToJsonProcessor.TimeWindow(
                context.jobId(),
                context.timeField(),
                context.start(),
                context.end()
            );
            try (EsqlToJsonProcessor processor = new EsqlToJsonProcessor(columns, timeWindow, out)) {
                for (Iterable<Object> row : esqlResponse.rows()) {
                    processor.process(row);
                }
                rowCount = processor.getWrittenDocumentCount();
            }
            if (rowCount > 0) {
                data = Optional.of(out.bytes().streamInput());
            }
            return new Result(searchInterval, data, lastLinkedClusterStates, rowCount);
        }
    }

    private EsqlQueryResponse runQueryWithSingleRetry(String query, QueryBuilder timeFilter, List<EsqlQueryParam> params) {
        return EsqlQueryRunner.runWithSingleRetry(syncQueryRunner, query, timeFilter, params, () -> isCancelled, context.jobId());
    }

    private EsqlQueryResponse executeDefaultSyncQuery(String orderedQuery, QueryBuilder timeFilter, List<EsqlQueryParam> params) {
        return execute(
            EsqlQueryExecutor.newRequest(client, orderedQuery, timeFilter, params, context.projectRouting(), ExecutionMetadata.CCS_ONLY)
        );
    }

    private List<EsqlQueryParam> timeFieldParam() {
        return List.of(new EsqlQueryParam("timeField", context.timeField(), IDENTIFIER));
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
