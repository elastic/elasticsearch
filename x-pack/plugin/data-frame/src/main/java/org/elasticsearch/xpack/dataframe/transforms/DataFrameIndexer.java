/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpoint;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class DataFrameIndexer extends AsyncTwoPhaseIndexer<DataFrameIndexerPosition, DataFrameIndexerTransformStats> {

    /**
     * RunState is an internal (non-persisted) state that controls the internal logic
     * which query filters to run and which index requests to send
     */
    private enum RunState {
        // do a complete query/index, this is used for batch data frames and for bootstraping (1st run)
        FULL_RUN,

        // Partial run modes in 2 stages:
        // identify buckets that have changed
        PARTIAL_RUN_IDENTIFY_CHANGES,

        // recalculate buckets based on the update list
        PARTIAL_RUN_APPLY_CHANGES
    }

    public static final int MINIMUM_PAGE_SIZE = 10;
    public static final String COMPOSITE_AGGREGATION_NAME = "_data_frame";
    private static final Logger logger = LogManager.getLogger(DataFrameIndexer.class);

    protected final DataFrameAuditor auditor;

    protected volatile DataFrameTransformConfig transformConfig;
    protected volatile DataFrameTransformProgress progress;
    private final Map<String, String> fieldMappings;

    private Pivot pivot;
    private int pageSize = 0;
    protected volatile DataFrameTransformCheckpoint lastCheckpoint;
    protected volatile DataFrameTransformCheckpoint nextCheckpoint;

    private volatile RunState runState;

    // hold information for continuous mode (partial updates)
    private volatile Map<String, Set<String>> changedBuckets;
    private volatile Map<String, Object> changedBucketsAfterKey;

    public DataFrameIndexer(Executor executor,
                            DataFrameAuditor auditor,
                            DataFrameTransformConfig transformConfig,
                            Map<String, String> fieldMappings,
                            AtomicReference<IndexerState> initialState,
                            DataFrameIndexerPosition initialPosition,
                            DataFrameIndexerTransformStats jobStats,
                            DataFrameTransformProgress transformProgress,
                            DataFrameTransformCheckpoint lastCheckpoint,
                            DataFrameTransformCheckpoint nextCheckpoint) {
        super(executor, initialState, initialPosition, jobStats);
        this.auditor = Objects.requireNonNull(auditor);
        this.transformConfig = ExceptionsHelper.requireNonNull(transformConfig, "transformConfig");
        this.fieldMappings = ExceptionsHelper.requireNonNull(fieldMappings, "fieldMappings");
        this.progress = transformProgress;
        this.lastCheckpoint = lastCheckpoint;
        this.nextCheckpoint = nextCheckpoint;
        // give runState a default
        this.runState = RunState.FULL_RUN;
    }

    protected abstract void failIndexer(String message);

    public int getPageSize() {
        return pageSize;
    }

    @Override
    protected String getJobId() {
        return transformConfig.getId();
    }

    public DataFrameTransformConfig getConfig() {
        return transformConfig;
    }

    public boolean isContinuous() {
        return getConfig().getSyncConfig() != null;
    }

    public Map<String, String> getFieldMappings() {
        return fieldMappings;
    }

    public DataFrameTransformProgress getProgress() {
        return progress;
    }

    public DataFrameTransformCheckpoint getLastCheckpoint() {
        return lastCheckpoint;
    }

    public DataFrameTransformCheckpoint getNextCheckpoint() {
        return nextCheckpoint;
    }

    /**
     * Request a checkpoint
     */
    protected abstract void createCheckpoint(ActionListener<DataFrameTransformCheckpoint> listener);

    @Override
    protected void onStart(long now, ActionListener<Boolean> listener) {
        try {
            pivot = new Pivot(getConfig().getPivotConfig());

            // if we haven't set the page size yet, if it is set we might have reduced it after running into an out of memory
            if (pageSize == 0) {
                pageSize = pivot.getInitialPageSize();
            }

            runState = determineRunStateAtStart();
            listener.onResponse(true);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected boolean initialRun() {
        return getPosition() == null;
    }

    @Override
    protected void onFinish(ActionListener<Void> listener) {
        // reset the page size, so we do not memorize a low page size forever
        pageSize = pivot.getInitialPageSize();
        // reset the changed bucket to free memory
        changedBuckets = null;
    }

    @Override
    protected IterationResult<DataFrameIndexerPosition> doProcess(SearchResponse searchResponse) {
        final Aggregations aggregations = searchResponse.getAggregations();
        // Treat this as a "we reached the end".
        // This should only happen when all underlying indices have gone away. Consequently, there is no more data to read.
        if (aggregations == null) {
            logger.info("[" + getJobId() + "] unexpected null aggregations in search response. " +
                "Source indices have been deleted or closed.");
            auditor.info(getJobId(),
                "Source indices have been deleted or closed. " +
                    "Please verify that these indices exist and are open [" +
                    Strings.arrayToCommaDelimitedString(getConfig().getSource().getIndex()) +
                    "].");
            return new IterationResult<>(Collections.emptyList(), null, true);
        }
        final CompositeAggregation agg = aggregations.get(COMPOSITE_AGGREGATION_NAME);

        switch (runState) {
        case FULL_RUN:
            return processBuckets(agg);
        case PARTIAL_RUN_APPLY_CHANGES:
            return processPartialBucketUpdates(agg);
        case PARTIAL_RUN_IDENTIFY_CHANGES:
            return processChangedBuckets(agg);

        default:
            // Any other state is a bug, should not happen
            logger.warn("Encountered unexpected run state [" + runState + "]");
            throw new IllegalStateException("DataFrame indexer job encountered an illegal state [" + runState + "]");
        }
    }

    private IterationResult<DataFrameIndexerPosition> processBuckets(final CompositeAggregation agg) {
        // we reached the end
        if (agg.getBuckets().isEmpty()) {
            return new IterationResult<>(Collections.emptyList(), null, true);
        }

        long docsBeforeProcess = getStats().getNumDocuments();

        DataFrameIndexerPosition oldPosition = getPosition();
        DataFrameIndexerPosition newPosition = new DataFrameIndexerPosition(agg.afterKey(),
                oldPosition != null ? getPosition().getBucketsPosition() : null);

        IterationResult<DataFrameIndexerPosition> result = new IterationResult<>(
                processBucketsToIndexRequests(agg).collect(Collectors.toList()),
                newPosition,
                agg.getBuckets().isEmpty());

        // NOTE: progress is also mutated in ClientDataFrameIndexer#onFinished
        if (progress != null) {
            progress.incrementDocsProcessed(getStats().getNumDocuments() - docsBeforeProcess);
            progress.incrementDocsIndexed(result.getToIndex().size());
        }

        return result;
    }

    private IterationResult<DataFrameIndexerPosition> processPartialBucketUpdates(final CompositeAggregation agg) {
        // we reached the end
        if (agg.getBuckets().isEmpty()) {
            // cleanup changed Buckets
            changedBuckets = null;

            // reset the runState to fetch changed buckets
            runState = RunState.PARTIAL_RUN_IDENTIFY_CHANGES;
            // advance the cursor for changed bucket detection
            return new IterationResult<>(Collections.emptyList(),
                    new DataFrameIndexerPosition(null, changedBucketsAfterKey), false);
        }

        return processBuckets(agg);
    }


    private IterationResult<DataFrameIndexerPosition> processChangedBuckets(final CompositeAggregation agg) {
        // initialize the map of changed buckets, the map might be empty if source do not require/implement
        // changed bucket detection
        changedBuckets = pivot.initialIncrementalBucketUpdateMap();

        // reached the end?
        if (agg.getBuckets().isEmpty()) {
            // reset everything and return the end marker
            changedBuckets = null;
            changedBucketsAfterKey = null;
            return new IterationResult<>(Collections.emptyList(), null, true);
        }
        // else

        // collect all buckets that require the update
        agg.getBuckets().stream().forEach(bucket -> {
            bucket.getKey().forEach((k, v) -> {
                changedBuckets.get(k).add(v.toString());
            });
        });

        // remember the after key but do not store it in the state yet (in the failure we need to retrieve it again)
        changedBucketsAfterKey = agg.afterKey();

        // reset the runState to fetch the partial updates next
        runState = RunState.PARTIAL_RUN_APPLY_CHANGES;

        return new IterationResult<>(Collections.emptyList(), getPosition(), false);
    }

    /*
     * Parses the result and creates a stream of indexable documents
     *
     * Implementation decisions:
     *
     * Extraction uses generic maps as intermediate exchange format in order to hook in ingest pipelines/processors
     * in later versions, see {@link IngestDocument).
     */
    private Stream<IndexRequest> processBucketsToIndexRequests(CompositeAggregation agg) {
        final DataFrameTransformConfig transformConfig = getConfig();
        String indexName = transformConfig.getDestination().getIndex();

        return pivot.extractResults(agg, getFieldMappings(), getStats()).map(document -> {
            String id = (String) document.get(DataFrameField.DOCUMENT_ID_FIELD);

            if (id == null) {
                throw new RuntimeException("Expected a document id but got null.");
            }

            XContentBuilder builder;
            try {
                builder = jsonBuilder();
                builder.startObject();
                for (Map.Entry<String, ?> value : document.entrySet()) {
                    // skip all internal fields
                    if (value.getKey().startsWith("_") == false) {
                        builder.field(value.getKey(), value.getValue());
                    }
                }
                builder.endObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            IndexRequest request = new IndexRequest(indexName).source(builder).id(id);
            if (transformConfig.getDestination().getPipeline() != null) {
                request.setPipeline(transformConfig.getDestination().getPipeline());
            }
            return request;
        });
    }

    protected QueryBuilder buildFilterQuery() {
        assert nextCheckpoint != null;

        QueryBuilder pivotQueryBuilder = getConfig().getSource().getQueryConfig().getQuery();

        DataFrameTransformConfig config = getConfig();
        if (this.isContinuous()) {

            BoolQueryBuilder filteredQuery = new BoolQueryBuilder()
                .filter(pivotQueryBuilder);

            if (lastCheckpoint != null) {
                filteredQuery.filter(config.getSyncConfig().getRangeQuery(lastCheckpoint, nextCheckpoint));
            } else {
                filteredQuery.filter(config.getSyncConfig().getRangeQuery(nextCheckpoint));
            }
            return filteredQuery;
        }

        return pivotQueryBuilder;
    }

    @Override
    protected SearchRequest buildSearchRequest() {
        assert nextCheckpoint != null;

        SearchRequest searchRequest = new SearchRequest(getConfig().getSource().getIndex())
                .allowPartialSearchResults(false)
                .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .size(0);

        switch (runState) {
        case FULL_RUN:
            buildFullRunQuery(sourceBuilder);
            break;
        case PARTIAL_RUN_IDENTIFY_CHANGES:
            buildChangedBucketsQuery(sourceBuilder);
            break;
        case PARTIAL_RUN_APPLY_CHANGES:
            buildPartialUpdateQuery(sourceBuilder);
            break;
        default:
            // Any other state is a bug, should not happen
            logger.warn("Encountered unexpected run state [" + runState + "]");
            throw new IllegalStateException("DataFrame indexer job encountered an illegal state [" + runState + "]");
        }

        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    private SearchSourceBuilder buildFullRunQuery(SearchSourceBuilder sourceBuilder) {
        DataFrameIndexerPosition position = getPosition();

        sourceBuilder.aggregation(pivot.buildAggregation(position != null ? position.getIndexerPosition() : null, pageSize));
        DataFrameTransformConfig config = getConfig();

        QueryBuilder pivotQueryBuilder = config.getSource().getQueryConfig().getQuery();
        if (isContinuous()) {
            BoolQueryBuilder filteredQuery = new BoolQueryBuilder()
                    .filter(pivotQueryBuilder)
                    .filter(config.getSyncConfig()
                            .getRangeQuery(nextCheckpoint));
            sourceBuilder.query(filteredQuery);
        } else {
            sourceBuilder.query(pivotQueryBuilder);
        }

        logger.trace("running full run query: {}", sourceBuilder);

        return sourceBuilder;
    }

    private SearchSourceBuilder buildChangedBucketsQuery(SearchSourceBuilder sourceBuilder) {
        assert isContinuous();

        DataFrameIndexerPosition position = getPosition();

        CompositeAggregationBuilder changesAgg = pivot.buildIncrementalBucketUpdateAggregation(pageSize);
        changesAgg.aggregateAfter(position != null ? position.getBucketsPosition() : null);
        sourceBuilder.aggregation(changesAgg);

        QueryBuilder pivotQueryBuilder = getConfig().getSource().getQueryConfig().getQuery();

        DataFrameTransformConfig config = getConfig();
        BoolQueryBuilder filteredQuery = new BoolQueryBuilder().
                filter(pivotQueryBuilder).
                filter(config.getSyncConfig().getRangeQuery(lastCheckpoint, nextCheckpoint));

        sourceBuilder.query(filteredQuery);

        logger.trace("running changes query {}", sourceBuilder);
        return sourceBuilder;
    }

    private SearchSourceBuilder buildPartialUpdateQuery(SearchSourceBuilder sourceBuilder) {
        assert isContinuous();

        DataFrameIndexerPosition position = getPosition();

        sourceBuilder.aggregation(pivot.buildAggregation(position != null ? position.getIndexerPosition() : null, pageSize));
        DataFrameTransformConfig config = getConfig();

        QueryBuilder pivotQueryBuilder = config.getSource().getQueryConfig().getQuery();

        BoolQueryBuilder filteredQuery = new BoolQueryBuilder()
                .filter(pivotQueryBuilder)
                .filter(config.getSyncConfig()
                        .getRangeQuery(nextCheckpoint));

        if (changedBuckets != null && changedBuckets.isEmpty() == false) {
            QueryBuilder pivotFilter = pivot.filterBuckets(changedBuckets);
            if (pivotFilter != null) {
                filteredQuery.filter(pivotFilter);
            }
        }

        sourceBuilder.query(filteredQuery);
        logger.trace("running partial update query: {}", sourceBuilder);

        return sourceBuilder;
    }

    /**
     * Handle the circuit breaking case: A search consumed to much memory and got aborted.
     *
     * Going out of memory we smoothly reduce the page size which reduces memory consumption.
     *
     * Implementation details: We take the values from the circuit breaker as a hint, but
     * note that it breaks early, that's why we also reduce using
     *
     * @param e Exception thrown, only {@link CircuitBreakingException} are handled
     * @return true if exception was handled, false if not
     */
    protected boolean handleCircuitBreakingException(Exception e) {
        CircuitBreakingException circuitBreakingException = getCircuitBreakingException(e);

        if (circuitBreakingException == null) {
            return false;
        }

        double reducingFactor = Math.min((double) circuitBreakingException.getByteLimit() / circuitBreakingException.getBytesWanted(),
                1 - (Math.log10(pageSize) * 0.1));

        int newPageSize = (int) Math.round(reducingFactor * pageSize);

        if (newPageSize < MINIMUM_PAGE_SIZE) {
            String message = DataFrameMessages.getMessage(DataFrameMessages.LOG_DATA_FRAME_TRANSFORM_PIVOT_LOW_PAGE_SIZE_FAILURE, pageSize);
            failIndexer(message);
            return true;
        }

        String message = DataFrameMessages.getMessage(DataFrameMessages.LOG_DATA_FRAME_TRANSFORM_PIVOT_REDUCE_PAGE_SIZE, pageSize,
                newPageSize);
        auditor.info(getJobId(), message);
        logger.info("Data frame transform [" + getJobId() + "]:" + message);

        pageSize = newPageSize;
        return true;
    }

    private RunState determineRunStateAtStart() {
        // either 1st run or not a continuous data frame
        if (nextCheckpoint.getCheckpoint() == 1 || isContinuous() == false) {
            return RunState.FULL_RUN;
        }

        // if incremental update is not supported, do a full run
        if (pivot.supportsIncrementalBucketUpdate() == false) {
            return RunState.FULL_RUN;
        }

        // continuous mode: we need to get the changed buckets first
        return RunState.PARTIAL_RUN_IDENTIFY_CHANGES;
    }

    /**
     * Inspect exception for circuit breaking exception and return the first one it can find.
     *
     * @param e Exception
     * @return CircuitBreakingException instance if found, null otherwise
     */
    private static CircuitBreakingException getCircuitBreakingException(Exception e) {
        // circuit breaking exceptions are at the bottom
        Throwable unwrappedThrowable = org.elasticsearch.ExceptionsHelper.unwrapCause(e);

        if (unwrappedThrowable instanceof CircuitBreakingException) {
            return (CircuitBreakingException) unwrappedThrowable;
        } else if (unwrappedThrowable instanceof SearchPhaseExecutionException) {
            SearchPhaseExecutionException searchPhaseException = (SearchPhaseExecutionException) e;
            for (ShardSearchFailure shardFailure : searchPhaseException.shardFailures()) {
                Throwable unwrappedShardFailure = org.elasticsearch.ExceptionsHelper.unwrapCause(shardFailure.getCause());

                if (unwrappedShardFailure instanceof CircuitBreakingException) {
                    return (CircuitBreakingException) unwrappedShardFailure;
                }
            }
        }

        return null;
    }

    protected abstract void sourceHasChanged(ActionListener<Boolean> hasChangedListener);
}
