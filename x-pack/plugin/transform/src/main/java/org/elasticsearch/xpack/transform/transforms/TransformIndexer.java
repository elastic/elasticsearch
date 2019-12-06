/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.indexing.AsyncTwoPhaseIndexer;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.indexing.IterationResult;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.pivot.AggregationResultUtils;
import org.elasticsearch.xpack.transform.transforms.pivot.Pivot;
import org.elasticsearch.xpack.transform.utils.ExceptionRootCauseFinder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public abstract class TransformIndexer extends AsyncTwoPhaseIndexer<TransformIndexerPosition, TransformIndexerStats> {

    /**
     * RunState is an internal (non-persisted) state that controls the internal logic
     * which query filters to run and which index requests to send
     */
    private enum RunState {
        // do a complete query/index, this is used for batch transforms and for bootstrapping (1st run)
        FULL_RUN,

        // Partial run modes in 2 stages:
        // identify buckets that have changed
        PARTIAL_RUN_IDENTIFY_CHANGES,

        // recalculate buckets based on the update list
        PARTIAL_RUN_APPLY_CHANGES
    }

    public static final int MINIMUM_PAGE_SIZE = 10;
    public static final String COMPOSITE_AGGREGATION_NAME = "_transform";

    private static final Logger logger = LogManager.getLogger(TransformIndexer.class);

    // constant for checkpoint retention, static for now
    private static final long NUMBER_OF_CHECKPOINTS_TO_KEEP = 10;
    private static final long RETENTION_OF_CHECKPOINTS_MS = 864000000L; // 10 days
    private static final long CHECKPOINT_CLEANUP_INTERVAL = 100L; // every 100 checkpoints

    protected final TransformConfigManager transformsConfigManager;
    private final CheckpointProvider checkpointProvider;
    private final TransformProgressGatherer progressGatherer;

    protected final TransformAuditor auditor;
    protected final TransformContext context;

    protected volatile TransformConfig transformConfig;
    private volatile TransformProgress progress;
    protected volatile boolean auditBulkFailures = true;
    // Indicates that the source has changed for the current run
    protected volatile boolean hasSourceChanged = true;

    private final Map<String, String> fieldMappings;

    private Pivot pivot;
    private int pageSize = 0;
    private long logEvery = 1;
    private long logCount = 0;
    private volatile TransformCheckpoint lastCheckpoint;
    private volatile TransformCheckpoint nextCheckpoint;

    // Keeps track of the last exception that was written to our audit, keeps us from spamming the audit index
    private volatile String lastAuditedExceptionMessage = null;
    private volatile RunState runState;

    // hold information for continuous mode (partial updates)
    private volatile Map<String, Set<String>> changedBuckets;
    private volatile Map<String, Object> changedBucketsAfterKey;

    private volatile long lastCheckpointCleanup = 0L;

    public TransformIndexer(
        Executor executor,
        TransformConfigManager transformsConfigManager,
        CheckpointProvider checkpointProvider,
        TransformProgressGatherer progressGatherer,
        TransformAuditor auditor,
        TransformConfig transformConfig,
        Map<String, String> fieldMappings,
        AtomicReference<IndexerState> initialState,
        TransformIndexerPosition initialPosition,
        TransformIndexerStats jobStats,
        TransformProgress transformProgress,
        TransformCheckpoint lastCheckpoint,
        TransformCheckpoint nextCheckpoint,
        TransformContext context
    ) {
        super(executor, initialState, initialPosition, jobStats);
        this.transformsConfigManager = ExceptionsHelper.requireNonNull(transformsConfigManager, "transformsConfigManager");
        this.checkpointProvider = ExceptionsHelper.requireNonNull(checkpointProvider, "checkpointProvider");
        this.progressGatherer = ExceptionsHelper.requireNonNull(progressGatherer, "progressGatherer");
        this.auditor = ExceptionsHelper.requireNonNull(auditor, "auditor");
        this.transformConfig = ExceptionsHelper.requireNonNull(transformConfig, "transformConfig");
        this.fieldMappings = ExceptionsHelper.requireNonNull(fieldMappings, "fieldMappings");
        this.progress = transformProgress;
        this.lastCheckpoint = ExceptionsHelper.requireNonNull(lastCheckpoint, "lastCheckpoint");
        this.nextCheckpoint = ExceptionsHelper.requireNonNull(nextCheckpoint, "nextCheckpoint");
        this.context = ExceptionsHelper.requireNonNull(context, "context");

        // give runState a default
        this.runState = RunState.FULL_RUN;
    }

    public int getPageSize() {
        return pageSize;
    }

    @Override
    protected String getJobId() {
        return transformConfig.getId();
    }

    public TransformConfig getConfig() {
        return transformConfig;
    }

    public boolean isContinuous() {
        return getConfig().getSyncConfig() != null;
    }

    public Map<String, String> getFieldMappings() {
        return fieldMappings;
    }

    public TransformProgress getProgress() {
        return progress;
    }

    public TransformCheckpoint getLastCheckpoint() {
        return lastCheckpoint;
    }

    public TransformCheckpoint getNextCheckpoint() {
        return nextCheckpoint;
    }

    public CheckpointProvider getCheckpointProvider() {
        return checkpointProvider;
    }

    /**
     * Request a checkpoint
     */
    protected void createCheckpoint(ActionListener<TransformCheckpoint> listener) {
        checkpointProvider.createNextCheckpoint(
            getLastCheckpoint(),
            ActionListener.wrap(
                checkpoint -> transformsConfigManager.putTransformCheckpoint(
                    checkpoint,
                    ActionListener.wrap(putCheckPointResponse -> listener.onResponse(checkpoint), createCheckpointException -> {
                        logger.warn(new ParameterizedMessage("[{}] failed to create checkpoint.", getJobId()), createCheckpointException);
                        listener.onFailure(
                            new RuntimeException(
                                "Failed to create checkpoint due to " + createCheckpointException.getMessage(),
                                createCheckpointException
                            )
                        );
                    })
                ),
                getCheckPointException -> {
                    logger.warn(new ParameterizedMessage("[{}] failed to retrieve checkpoint.", getJobId()), getCheckPointException);
                    listener.onFailure(
                        new RuntimeException(
                            "Failed to retrieve checkpoint due to " + getCheckPointException.getMessage(),
                            getCheckPointException
                        )
                    );
                }
            )
        );
    }

    @Override
    protected void onStart(long now, ActionListener<Boolean> listener) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] attempted to start while failed.", getJobId());
            listener.onFailure(new ElasticsearchException("Attempted to start a failed transform [{}].", getJobId()));
            return;
        }

        ActionListener<Void> finalListener = ActionListener.wrap(r -> {
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
                return;
            }
        }, listener::onFailure);

        // On each run, we need to get the total number of docs and reset the count of processed docs
        // Since multiple checkpoints can be executed in the task while it is running on the same node, we need to gather
        // the progress here, and not in the executor.
        ActionListener<Void> updateConfigListener = ActionListener.wrap(updateConfigResponse -> {
            if (initialRun()) {
                createCheckpoint(ActionListener.wrap(cp -> {
                    nextCheckpoint = cp;
                    // If nextCheckpoint > 1, this means that we are now on the checkpoint AFTER the batch checkpoint
                    // Consequently, the idea of percent complete no longer makes sense.
                    if (nextCheckpoint.getCheckpoint() > 1) {
                        progress = new TransformProgress(null, 0L, 0L);
                        finalListener.onResponse(null);
                        return;
                    }
                    progressGatherer.getInitialProgress(buildFilterQuery(), getConfig(), ActionListener.wrap(newProgress -> {
                        logger.trace("[{}] reset the progress from [{}] to [{}].", getJobId(), progress, newProgress);
                        progress = newProgress;
                        finalListener.onResponse(null);
                    }, failure -> {
                        progress = null;
                        logger.warn(new ParameterizedMessage("[{}] unable to load progress information for task.", getJobId()), failure);
                        finalListener.onResponse(null);
                    }));
                }, listener::onFailure));
            } else {
                finalListener.onResponse(null);
            }
        }, listener::onFailure);

        // If we are continuous, we will want to verify we have the latest stored configuration
        ActionListener<Void> changedSourceListener = ActionListener.wrap(r -> {
            if (isContinuous()) {
                transformsConfigManager.getTransformConfiguration(getJobId(), ActionListener.wrap(config -> {
                    transformConfig = config;
                    logger.debug("[{}] successfully refreshed transform config from index.", getJobId());
                    updateConfigListener.onResponse(null);
                }, failure -> {
                    String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_RELOAD_TRANSFORM_CONFIGURATION, getJobId());
                    logger.error(msg, failure);
                    // If the transform config index or the transform config is gone, something serious occurred
                    // We are in an unknown state and should fail out
                    if (failure instanceof ResourceNotFoundException) {
                        updateConfigListener.onFailure(new TransformConfigReloadingException(msg, failure));
                    } else {
                        auditor.warning(getJobId(), msg);
                        updateConfigListener.onResponse(null);
                    }
                }));
            } else {
                updateConfigListener.onResponse(null);
            }
        }, listener::onFailure);

        // If we are not on the initial batch checkpoint and its the first pass of whatever continuous checkpoint we are on,
        // we should verify if there are local changes based on the sync config. If not, do not proceed further and exit.
        if (context.getCheckpoint() > 0 && initialRun()) {
            sourceHasChanged(ActionListener.wrap(hasChanged -> {
                hasSourceChanged = hasChanged;
                if (hasChanged) {
                    context.setChangesLastDetectedAt(Instant.now());
                    logger.debug("[{}] source has changed, triggering new indexer run.", getJobId());
                    changedSourceListener.onResponse(null);
                } else {
                    logger.trace("[{}] source has not changed, finish indexer early.", getJobId());
                    // No changes, stop executing
                    listener.onResponse(false);
                }
            }, failure -> {
                // If we failed determining if the source changed, it's safer to assume there were changes.
                // We should allow the failure path to complete as normal
                hasSourceChanged = true;
                listener.onFailure(failure);
            }));
        } else {
            hasSourceChanged = true;
            changedSourceListener.onResponse(null);
        }
    }

    protected boolean initialRun() {
        return getPosition() == null;
    }

    @Override
    protected void onFinish(ActionListener<Void> listener) {
        try {
            // This indicates an early exit since no changes were found.
            // So, don't treat this like a checkpoint being completed, as no work was done.
            if (hasSourceChanged == false) {
                if (context.shouldStopAtCheckpoint()) {
                    stop();
                }
                listener.onResponse(null);
                return;
            }

            // reset the page size, so we do not memorize a low page size forever
            pageSize = pivot.getInitialPageSize();
            // reset the changed bucket to free memory
            changedBuckets = null;

            long checkpoint = context.getAndIncrementCheckpoint();
            lastCheckpoint = getNextCheckpoint();
            nextCheckpoint = null;
            // Reset our failure count as we have finished and may start again with a new checkpoint
            context.resetReasonAndFailureCounter();

            // With bucket_selector we could have read all the buckets and completed the transform
            // but not "see" all the buckets since they were filtered out. Consequently, progress would
            // show less than 100% even though we are done.
            // NOTE: this method is called in the same thread as the processing thread.
            // Theoretically, there should not be a race condition with updating progress here.
            // NOTE 2: getPercentComplete should only NOT be null on the first (batch) checkpoint
            if (progress != null && progress.getPercentComplete() != null && progress.getPercentComplete() < 100.0) {
                progress.incrementDocsProcessed(progress.getTotalDocs() - progress.getDocumentsProcessed());
            }
            // If the last checkpoint is now greater than 1, that means that we have just processed the first
            // continuous checkpoint and should start recording the exponential averages
            if (lastCheckpoint != null && lastCheckpoint.getCheckpoint() > 1) {
                long docsIndexed = 0;
                long docsProcessed = 0;
                // This should not happen as we simply create a new one when we reach continuous checkpoints
                // but this is a paranoid `null` check
                if (progress != null) {
                    docsIndexed = progress.getDocumentsIndexed();
                    docsProcessed = progress.getDocumentsProcessed();
                }
                long durationMs = System.currentTimeMillis() - lastCheckpoint.getTimestamp();
                getStats().incrementCheckpointExponentialAverages(durationMs < 0 ? 0 : durationMs, docsIndexed, docsProcessed);
            }
            if (shouldAuditOnFinish(checkpoint)) {
                auditor.info(getJobId(), "Finished indexing for transform checkpoint [" + checkpoint + "].");
            }
            logger.debug("[{}] finished indexing for transform checkpoint [{}].", getJobId(), checkpoint);
            auditBulkFailures = true;
            if (context.shouldStopAtCheckpoint()) {
                stop();
            }

            if (checkpoint - lastCheckpointCleanup > CHECKPOINT_CLEANUP_INTERVAL) {
                // delete old checkpoints, on a failure we keep going
                cleanupOldCheckpoints(listener);
            } else {
                listener.onResponse(null);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected IterationResult<TransformIndexerPosition> doProcess(SearchResponse searchResponse) {
        final Aggregations aggregations = searchResponse.getAggregations();
        // Treat this as a "we reached the end".
        // This should only happen when all underlying indices have gone away. Consequently, there is no more data to read.
        if (aggregations == null) {
            logger.info(
                "[{}] unexpected null aggregations in search response. " + "Source indices have been deleted or closed.",
                getJobId()
            );
            auditor.info(
                getJobId(),
                "Source indices have been deleted or closed. "
                    + "Please verify that these indices exist and are open ["
                    + Strings.arrayToCommaDelimitedString(getConfig().getSource().getIndex())
                    + "]."
            );
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
                logger.warn("[{}] Encountered unexpected run state [{}]", getJobId(), runState);
                throw new IllegalStateException("Transform indexer job encountered an illegal state [" + runState + "]");
        }
    }

    @Override
    public synchronized boolean maybeTriggerAsyncJob(long now) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] schedule was triggered for transform but task is failed. Ignoring trigger.", getJobId());
            return false;
        }

        // ignore trigger if indexer is running, prevents log spam in A2P indexer
        IndexerState indexerState = getState();
        if (IndexerState.INDEXING.equals(indexerState) || IndexerState.STOPPING.equals(indexerState)) {
            logger.debug("[{}] indexer for transform has state [{}]. Ignoring trigger.", getJobId(), indexerState);
            return false;
        }

        return super.maybeTriggerAsyncJob(now);
    }

    @Override
    protected void onFailure(Exception exc) {
        // the failure handler must not throw an exception due to internal problems
        try {
            handleFailure(exc);
        } catch (Exception e) {
            logger.error(new ParameterizedMessage("[{}] transform encountered an unexpected internal exception: ", getJobId()), e);
        }
    }

    @Override
    protected void onStop() {
        auditor.info(transformConfig.getId(), "Transform has stopped.");
        logger.info("[{}] transform has stopped.", transformConfig.getId());
    }

    @Override
    protected void onAbort() {
        auditor.info(transformConfig.getId(), "Received abort request, stopping transform.");
        logger.info("[{}] transform received abort request. Stopping indexer.", transformConfig.getId());
        context.shutdown();
    }

    synchronized void handleFailure(Exception e) {
        logger.warn(new ParameterizedMessage("[{}] transform encountered an exception: ", getJobId()), e);
        Throwable unwrappedException = ExceptionRootCauseFinder.getRootCauseException(e);

        if (unwrappedException instanceof CircuitBreakingException) {
            handleCircuitBreakingException((CircuitBreakingException) unwrappedException);
        } else if (unwrappedException instanceof ScriptException) {
            handleScriptException((ScriptException) unwrappedException);
            // irrecoverable error without special handling
        } else if (unwrappedException instanceof IndexNotFoundException
            || unwrappedException instanceof AggregationResultUtils.AggregationExtractionException
            || unwrappedException instanceof TransformConfigReloadingException) {
            failIndexer("task encountered irrecoverable failure: " + e.getMessage());
        } else if (context.getAndIncrementFailureCount() > context.getNumFailureRetries()) {
            failIndexer(
                "task encountered more than "
                    + context.getNumFailureRetries()
                    + " failures; latest failure: "
                    + ExceptionRootCauseFinder.getDetailedMessage(unwrappedException)
            );
        } else {
            // Since our schedule fires again very quickly after failures it is possible to run into the same failure numerous
            // times in a row, very quickly. We do not want to spam the audit log with repeated failures, so only record the first one
            if (e.getMessage().equals(lastAuditedExceptionMessage) == false) {
                String message = ExceptionRootCauseFinder.getDetailedMessage(unwrappedException);

                auditor.warning(
                    getJobId(),
                    "Transform encountered an exception: " + message + " Will attempt again at next scheduled trigger."
                );
                lastAuditedExceptionMessage = message;
            }
        }
    }

    /**
     * Cleanup old checkpoints
     *
     * @param listener listener to call after done
     */
    private void cleanupOldCheckpoints(ActionListener<Void> listener) {
        long now = getTime();
        long checkpointLowerBound = context.getCheckpoint() - NUMBER_OF_CHECKPOINTS_TO_KEEP;
        long lowerBoundEpochMs = now - RETENTION_OF_CHECKPOINTS_MS;

        if (checkpointLowerBound > 0 && lowerBoundEpochMs > 0) {
            transformsConfigManager.deleteOldCheckpoints(
                transformConfig.getId(),
                checkpointLowerBound,
                lowerBoundEpochMs,
                ActionListener.wrap(deletes -> {
                    logger.debug("[{}] deleted [{}] outdated checkpoints", getJobId(), deletes);
                    listener.onResponse(null);
                    lastCheckpointCleanup = context.getCheckpoint();
                }, e -> {
                    logger.warn(
                        new ParameterizedMessage("[{}] failed to cleanup old checkpoints, retrying after next checkpoint", getJobId()),
                        e
                    );
                    auditor.warning(
                        getJobId(),
                        "Failed to cleanup old checkpoints, retrying after next checkpoint. Exception: " + e.getMessage()
                    );

                    listener.onResponse(null);
                })
            );
        } else {
            logger.debug("[{}] checked for outdated checkpoints", getJobId());
            listener.onResponse(null);
        }
    }

    private void sourceHasChanged(ActionListener<Boolean> hasChangedListener) {
        checkpointProvider.sourceHasChanged(getLastCheckpoint(), ActionListener.wrap(hasChanged -> {
            logger.trace("[{}] change detected [{}].", getJobId(), hasChanged);
            hasChangedListener.onResponse(hasChanged);
        }, e -> {
            logger.warn(
                new ParameterizedMessage("[{}] failed to detect changes for transform. Skipping update till next check.", getJobId()),
                e
            );
            auditor.warning(
                getJobId(),
                "Failed to detect changes for transform, skipping update till next check. Exception: " + e.getMessage()
            );
            hasChangedListener.onResponse(false);
        }));
    }

    private IterationResult<TransformIndexerPosition> processBuckets(final CompositeAggregation agg) {
        // we reached the end
        if (agg.getBuckets().isEmpty()) {
            return new IterationResult<>(Collections.emptyList(), null, true);
        }

        long docsBeforeProcess = getStats().getNumDocuments();

        TransformIndexerPosition oldPosition = getPosition();
        TransformIndexerPosition newPosition = new TransformIndexerPosition(
            agg.afterKey(),
            oldPosition != null ? getPosition().getBucketsPosition() : null
        );

        IterationResult<TransformIndexerPosition> result = new IterationResult<>(
            processBucketsToIndexRequests(agg).collect(Collectors.toList()),
            newPosition,
            agg.getBuckets().isEmpty()
        );

        // NOTE: progress is also mutated in onFinish
        if (progress != null) {
            progress.incrementDocsProcessed(getStats().getNumDocuments() - docsBeforeProcess);
            progress.incrementDocsIndexed(result.getToIndex().size());
        }

        return result;
    }

    private IterationResult<TransformIndexerPosition> processPartialBucketUpdates(final CompositeAggregation agg) {
        // we reached the end
        if (agg.getBuckets().isEmpty()) {
            // cleanup changed Buckets
            changedBuckets = null;

            // reset the runState to fetch changed buckets
            runState = RunState.PARTIAL_RUN_IDENTIFY_CHANGES;
            // advance the cursor for changed bucket detection
            return new IterationResult<>(Collections.emptyList(), new TransformIndexerPosition(null, changedBucketsAfterKey), false);
        }

        return processBuckets(agg);
    }

    private IterationResult<TransformIndexerPosition> processChangedBuckets(final CompositeAggregation agg) {
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
        agg.getBuckets().stream().forEach(bucket -> { bucket.getKey().forEach((k, v) -> { changedBuckets.get(k).add(v.toString()); }); });

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
        final TransformConfig transformConfig = getConfig();
        String indexName = transformConfig.getDestination().getIndex();

        return pivot.extractResults(agg, getFieldMappings(), getStats()).map(document -> {
            String id = (String) document.get(TransformField.DOCUMENT_ID_FIELD);

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

        TransformConfig config = getConfig();
        if (this.isContinuous()) {

            BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(pivotQueryBuilder);

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

        SearchRequest searchRequest = new SearchRequest(getConfig().getSource().getIndex()).allowPartialSearchResults(false)
            .indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(0);

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
                throw new IllegalStateException("Transform indexer job encountered an illegal state [" + runState + "]");
        }

        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    private SearchSourceBuilder buildFullRunQuery(SearchSourceBuilder sourceBuilder) {
        TransformIndexerPosition position = getPosition();

        sourceBuilder.aggregation(pivot.buildAggregation(position != null ? position.getIndexerPosition() : null, pageSize));
        TransformConfig config = getConfig();

        QueryBuilder pivotQueryBuilder = config.getSource().getQueryConfig().getQuery();
        if (isContinuous()) {
            BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(pivotQueryBuilder)
                .filter(config.getSyncConfig().getRangeQuery(nextCheckpoint));
            sourceBuilder.query(filteredQuery);
        } else {
            sourceBuilder.query(pivotQueryBuilder);
        }

        logger.trace("running full run query: {}", sourceBuilder);

        return sourceBuilder;
    }

    private SearchSourceBuilder buildChangedBucketsQuery(SearchSourceBuilder sourceBuilder) {
        assert isContinuous();

        TransformIndexerPosition position = getPosition();

        CompositeAggregationBuilder changesAgg = pivot.buildIncrementalBucketUpdateAggregation(pageSize);
        changesAgg.aggregateAfter(position != null ? position.getBucketsPosition() : null);
        sourceBuilder.aggregation(changesAgg);

        QueryBuilder pivotQueryBuilder = getConfig().getSource().getQueryConfig().getQuery();

        TransformConfig config = getConfig();
        BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(pivotQueryBuilder)
            .filter(config.getSyncConfig().getRangeQuery(lastCheckpoint, nextCheckpoint));

        sourceBuilder.query(filteredQuery);

        logger.trace("running changes query {}", sourceBuilder);
        return sourceBuilder;
    }

    private SearchSourceBuilder buildPartialUpdateQuery(SearchSourceBuilder sourceBuilder) {
        assert isContinuous();

        TransformIndexerPosition position = getPosition();

        sourceBuilder.aggregation(pivot.buildAggregation(position != null ? position.getIndexerPosition() : null, pageSize));
        TransformConfig config = getConfig();

        QueryBuilder pivotQueryBuilder = config.getSource().getQueryConfig().getQuery();

        BoolQueryBuilder filteredQuery = new BoolQueryBuilder().filter(pivotQueryBuilder)
            .filter(config.getSyncConfig().getRangeQuery(nextCheckpoint));

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
     * @param circuitBreakingException CircuitBreakingException thrown
     */
    private void handleCircuitBreakingException(CircuitBreakingException circuitBreakingException) {
        double reducingFactor = Math.min(
            (double) circuitBreakingException.getByteLimit() / circuitBreakingException.getBytesWanted(),
            1 - (Math.log10(pageSize) * 0.1)
        );

        int newPageSize = (int) Math.round(reducingFactor * pageSize);

        if (newPageSize < MINIMUM_PAGE_SIZE) {
            String message = TransformMessages.getMessage(TransformMessages.LOG_TRANSFORM_PIVOT_LOW_PAGE_SIZE_FAILURE, pageSize);
            failIndexer(message);
            return;
        }

        String message = TransformMessages.getMessage(TransformMessages.LOG_TRANSFORM_PIVOT_REDUCE_PAGE_SIZE, pageSize, newPageSize);
        auditor.info(getJobId(), message);
        logger.info("[{}] {}", getJobId(), message);
        pageSize = newPageSize;
        return;
    }

    /**
     * Handle script exception case. This is error is irrecoverable.
     *
     * @param scriptException ScriptException thrown
     */
    private void handleScriptException(ScriptException scriptException) {
        String message = TransformMessages.getMessage(
            TransformMessages.LOG_TRANSFORM_PIVOT_SCRIPT_ERROR,
            scriptException.getDetailedMessage(),
            scriptException.getScriptStack()
        );
        failIndexer(message);
    }

    protected void failIndexer(String failureMessage) {
        logger.error("[{}] transform has failed; experienced: [{}].", getJobId(), failureMessage);
        auditor.error(getJobId(), failureMessage);
        context.markAsFailed(failureMessage);
    }

    /*
     * Get the current time, abstracted for the purpose of testing
     */
    long getTime() {
        return System.currentTimeMillis();
    }

    /**
     * Indicates if an audit message should be written when onFinish is called for the given checkpoint
     * We audit the first checkpoint, and then every 10 checkpoints until completedCheckpoint == 99
     * Then we audit every 100, until completedCheckpoint == 999
     *
     * Then we always audit every 1_000 checkpoints
     *
     * @param completedCheckpoint The checkpoint that was just completed
     * @return {@code true} if an audit message should be written
     */
    protected boolean shouldAuditOnFinish(long completedCheckpoint) {
        if (++logCount % logEvery != 0) {
            return false;
        }
        if (completedCheckpoint == 0) {
            return true;
        }
        int log10Checkpoint = (int) Math.floor(Math.log10(completedCheckpoint));
        logEvery = log10Checkpoint >= 3 ? 1_000 : (int) Math.pow(10.0, log10Checkpoint);
        logCount = 0;
        return true;
    }

    private RunState determineRunStateAtStart() {
        // either 1st run or not a continuous transform
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

    static class TransformConfigReloadingException extends ElasticsearchException {
        TransformConfigReloadingException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }
}
