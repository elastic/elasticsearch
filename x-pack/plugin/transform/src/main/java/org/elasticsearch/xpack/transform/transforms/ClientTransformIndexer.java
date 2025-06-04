/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformEffectiveSettings;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;
import org.elasticsearch.xpack.transform.TransformExtension;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformIndex;
import org.elasticsearch.xpack.transform.transforms.pivot.SchemaUtil;
import org.elasticsearch.xpack.transform.utils.ExceptionRootCauseFinder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;

class ClientTransformIndexer extends TransformIndexer {

    private static final TimeValue PIT_KEEP_ALIVE = TimeValue.timeValueSeconds(30);
    private static final Logger logger = LogManager.getLogger(ClientTransformIndexer.class);

    private final ParentTaskAssigningClient client;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Settings destIndexSettings;
    private final AtomicBoolean oldStatsCleanedUp = new AtomicBoolean(false);

    private final AtomicReference<SeqNoPrimaryTermAndIndex> seqNoPrimaryTermAndIndexHolder;
    private final ConcurrentHashMap<String, PointInTimeBuilder> namedPits = new ConcurrentHashMap<>();
    private volatile long pitCheckpoint;
    private volatile boolean disablePit = false;

    ClientTransformIndexer(
        ThreadPool threadPool,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformExtension transformExtension,
        TransformServices transformServices,
        CheckpointProvider checkpointProvider,
        AtomicReference<IndexerState> initialState,
        TransformIndexerPosition initialPosition,
        ParentTaskAssigningClient client,
        TransformIndexerStats initialStats,
        TransformConfig transformConfig,
        TransformProgress transformProgress,
        TransformCheckpoint lastCheckpoint,
        TransformCheckpoint nextCheckpoint,
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
        TransformContext context,
        boolean shouldStopAtCheckpoint
    ) {
        super(
            ExceptionsHelper.requireNonNull(threadPool, "threadPool"),
            transformServices,
            checkpointProvider,
            transformConfig,
            ExceptionsHelper.requireNonNull(initialState, "initialState"),
            initialPosition,
            initialStats == null ? new TransformIndexerStats() : initialStats,
            transformProgress,
            lastCheckpoint,
            nextCheckpoint,
            context
        );
        this.client = ExceptionsHelper.requireNonNull(client, "client");
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.destIndexSettings = transformExtension.getTransformDestinationIndexSettings();
        this.seqNoPrimaryTermAndIndexHolder = new AtomicReference<>(seqNoPrimaryTermAndIndex);

        // TODO: move into context constructor
        context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);

        disablePit = TransformEffectiveSettings.isPitDisabled(transformConfig.getSettings());
    }

    @Override
    public void applyNewSettings(SettingsConfig newSettings) {
        disablePit = TransformEffectiveSettings.isPitDisabled(newSettings);
        super.applyNewSettings(newSettings);
    }

    @Override
    protected void doNextSearch(long waitTimeInNanos, ActionListener<SearchResponse> nextPhase) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] attempted to search while failed.", getJobId());
            nextPhase.onFailure(new ElasticsearchException("Attempted to do a search request for failed transform [{}].", getJobId()));
            return;
        }

        if (getNextCheckpoint().getCheckpoint() != pitCheckpoint) {
            closePointInTime();
        }

        injectPointInTimeIfNeeded(
            buildSearchRequest(),
            ActionListener.wrap(searchRequest -> doSearch(searchRequest, nextPhase), nextPhase::onFailure)
        );
    }

    @Override
    protected void doNextBulk(BulkRequest request, ActionListener<BulkResponse> nextPhase) {
        if (context.getTaskState() == TransformTaskState.FAILED) {
            logger.debug("[{}] attempted to bulk index while failed.", getJobId());
            nextPhase.onFailure(new ElasticsearchException("Attempted to do a bulk index request for failed transform [{}].", getJobId()));
            return;
        }
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            TransportBulkAction.TYPE,
            request,
            ActionListener.wrap(bulkResponse -> handleBulkResponse(bulkResponse, nextPhase), nextPhase::onFailure)
        );
    }

    protected void handleBulkResponse(BulkResponse bulkResponse, ActionListener<BulkResponse> nextPhase) {
        if (bulkResponse.hasFailures() == false) {
            // We don't know the of failures that have occurred (searching, processing, indexing, etc.),
            // but if we search, process and bulk index then we have
            // successfully processed an entire page of the transform and should reset the counter, even if we are in the middle
            // of a checkpoint
            context.resetReasonAndFailureCounter();
            nextPhase.onResponse(bulkResponse);
            return;
        }
        int failureCount = 0;
        // dedup the failures by the type of the exception, as they most likely have the same cause
        Map<String, BulkItemResponse> deduplicatedFailures = new LinkedHashMap<>();

        for (BulkItemResponse item : bulkResponse.getItems()) {
            if (item.isFailed()) {
                var exceptionClass = item.getFailure().getCause().getClass();
                if (IndexNotFoundException.class.isAssignableFrom(exceptionClass)) {
                    context.setShouldRecreateDestinationIndex(true);
                }
                deduplicatedFailures.putIfAbsent(exceptionClass.getSimpleName(), item);
                failureCount++;
            }
        }

        // note: bulk failures are audited/logged in {@link TransformIndexer#handleFailure(Exception)}

        // This calls AsyncTwoPhaseIndexer#finishWithIndexingFailure
        // Determine whether the failure is irrecoverable (transform should go into failed state) or not (transform increments
        // the indexing failure counter
        // and possibly retries)
        Throwable irrecoverableException = ExceptionRootCauseFinder.getFirstIrrecoverableExceptionFromBulkResponses(
            deduplicatedFailures.values()
        );
        if (irrecoverableException == null) {
            String failureMessage = getBulkIndexDetailedFailureMessage("Significant failures: ", deduplicatedFailures);
            logger.debug("[{}] Bulk index experienced [{}] failures. {}", getJobId(), failureCount, failureMessage);

            Exception firstException = deduplicatedFailures.values().iterator().next().getFailure().getCause();
            nextPhase.onFailure(
                new BulkIndexingException("Bulk index experienced [{}] failures. {}", firstException, false, failureCount, failureMessage)
            );
        } else {
            deduplicatedFailures.remove(irrecoverableException.getClass().getSimpleName());
            String failureMessage = getBulkIndexDetailedFailureMessage("Other failures: ", deduplicatedFailures);
            irrecoverableException = decorateBulkIndexException(irrecoverableException);

            logger.debug(
                "[{}] Bulk index experienced [{}] failures and at least 1 irrecoverable [{}]. {}",
                getJobId(),
                failureCount,
                ExceptionRootCauseFinder.getDetailedMessage(irrecoverableException),
                failureMessage
            );

            nextPhase.onFailure(
                new BulkIndexingException(
                    "Bulk index experienced [{}] failures and at least 1 irrecoverable [{}]. {}",
                    irrecoverableException,
                    true,
                    failureCount,
                    ExceptionRootCauseFinder.getDetailedMessage(irrecoverableException),
                    failureMessage
                )
            );
        }
    }

    @Override
    protected void doDeleteByQuery(DeleteByQueryRequest deleteByQueryRequest, ActionListener<BulkByScrollResponse> responseListener) {
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            DeleteByQueryAction.INSTANCE,
            deleteByQueryRequest,
            responseListener
        );
    }

    @Override
    protected void refreshDestinationIndex(ActionListener<Void> responseListener) {
        // note: this gets executed _without_ the headers of the user as the user might not have the rights to call
        // _refresh for performance reasons. However this refresh is an internal detail of transform and this is only
        // called for the transform destination index
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.TRANSFORM_ORIGIN,
            RefreshAction.INSTANCE,
            new RefreshRequest(transformConfig.getDestination().getIndex()),
            ActionListener.wrap(refreshResponse -> {
                if (refreshResponse.getFailedShards() > 0) {
                    logger.warn(
                        "[{}] failed to refresh transform destination index, not all data might be available after checkpoint.",
                        getJobId()
                    );
                }
                responseListener.onResponse(null);
            }, e -> {
                if (e instanceof IndexNotFoundException) {
                    // We ignore IndexNotFound error. A non-existent index does not need refreshing.
                    responseListener.onResponse(null);
                    return;
                }
                responseListener.onFailure(e);
            })
        );
    }

    @Override
    void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            TransportSearchAction.TYPE,
            request,
            responseListener
        );
    }

    @Override
    void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener) {
        SchemaUtil.getDestinationFieldMappings(client, getConfig().getDestination().getIndex(), fieldMappingsListener);
    }

    @Override
    void doMaybeCreateDestIndex(Map<String, String> deducedDestIndexMappings, ActionListener<Boolean> listener) {
        TransformIndex.createDestinationIndex(
            client,
            auditor,
            indexNameExpressionResolver,
            clusterService.state(),
            transformConfig,
            destIndexSettings,
            deducedDestIndexMappings,
            listener
        );
    }

    void validate(ActionListener<ValidateTransformAction.Response> listener) {
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.TRANSFORM_ORIGIN,
            ValidateTransformAction.INSTANCE,
            new ValidateTransformAction.Request(transformConfig, false, AcknowledgedRequest.DEFAULT_ACK_TIMEOUT),
            listener
        );
    }

    /**
     * Runs the persistence part of state storage
     */
    @Override
    protected void persistState(TransformState state, ActionListener<Void> listener) {
        // This could be `null` but the putOrUpdateTransformStoredDoc handles that case just fine
        SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex = getSeqNoPrimaryTermAndIndex();

        // Persist the current state and stats in the internal index. The interval of this method being
        // called is controlled by AsyncTwoPhaseIndexer#onBulkResponse which calls doSaveState every so
        // often when doing bulk indexing calls or at the end of one indexing run.
        transformsConfigManager.putOrUpdateTransformStoredDoc(
            new TransformStoredDoc(getJobId(), state, getStats()),
            seqNoPrimaryTermAndIndex,
            ActionListener.wrap(r -> {
                updateSeqNoPrimaryTermAndIndex(seqNoPrimaryTermAndIndex, r);
                context.resetStatePersistenceFailureCount();

                // Only do this clean up once, if it succeeded, no reason to do the query again.
                if (oldStatsCleanedUp.compareAndSet(false, true)) {
                    transformsConfigManager.deleteOldTransformStoredDocuments(getJobId(), ActionListener.wrap(deletedDocs -> {
                        logger.trace(
                            "[{}] deleted old transform stats and state document, deleted: [{}] documents",
                            getJobId(),
                            deletedDocs
                        );
                        listener.onResponse(null);
                    }, e -> {
                        String msg = LoggerMessageFormat.format("[{}] failed deleting old transform configurations.", getJobId());
                        logger.warn(msg, e);
                        // If we have failed, we should attempt the clean up again later
                        oldStatsCleanedUp.set(false);
                        listener.onResponse(null);
                    }));
                } else {
                    listener.onResponse(null);
                }
            }, statsExc -> {
                if (org.elasticsearch.exception.ExceptionsHelper.unwrapCause(statsExc) instanceof VersionConflictEngineException) {
                    // this should never happen, but indicates a race condition in state persistence:
                    // - there should be only 1 save persistence at a time
                    // - there are reasons the seq_id, primary_term changes without user intervention, e.g. an internal
                    // retry (seq_id) or an unexpected node failure (primary_term), these are rare
                    // - in case re-get the versions and retry on the next persistence
                    // - the transform can (extremely unlikely) fail if state persistence fails in a row
                    // - for tests the number of allowed retries is set to 0 and therefore causes the transform to fail
                    logger.warn(
                        () -> format(
                            "[%s] updating stats of transform failed, unexpected version conflict of internal state, resetting to recover.",
                            transformConfig.getId()
                        ),
                        statsExc
                    );
                    auditor.warning(
                        getJobId(),
                        "Failure updating stats of transform, unexpected version conflict of internal state, resetting to recover: "
                            + statsExc.getMessage()
                    );

                    if (failureHandler.handleStatePersistenceFailure(statsExc, getConfig().getSettings())) {
                        // get the current seqNo and primary term, however ignore the stored state
                        transformsConfigManager.getTransformStoredDoc(
                            transformConfig.getId(),
                            false,
                            ActionListener.wrap(storedDocAndSeqNoPrimaryTerm -> {
                                updateSeqNoPrimaryTermAndIndex(seqNoPrimaryTermAndIndex, storedDocAndSeqNoPrimaryTerm.v2());
                                listener.onFailure(statsExc);
                            }, e2 -> listener.onFailure(statsExc))
                        );
                        // wrapped listener gets called
                        return;
                    }
                } else {
                    logger.warn(() -> "[" + transformConfig.getId() + "] updating stats of transform failed.", statsExc);
                    auditor.warning(getJobId(), "Failure updating stats of transform: " + statsExc.getMessage());
                    failureHandler.handleStatePersistenceFailure(statsExc, getConfig().getSettings());
                }
                listener.onFailure(statsExc);
            })
        );
    }

    void updateSeqNoPrimaryTermAndIndex(SeqNoPrimaryTermAndIndex expectedValue, SeqNoPrimaryTermAndIndex newValue) {
        logger.debug(() -> format("[%s] Updated state document from [%s] to [%s]", transformConfig.getId(), expectedValue, newValue));
        boolean updated = seqNoPrimaryTermAndIndexHolder.compareAndSet(expectedValue, newValue);
        // This should never happen. We ONLY ever update this value if at initialization or we just finished updating the document
        // famous last words...
        if (updated == false) {
            logger.warn(
                "[{}] Unexpected change to internal state detected, expected [{}], got [{}]",
                transformConfig.getId(),
                expectedValue,
                seqNoPrimaryTermAndIndexHolder.get()
            );
            assert updated : "[" + getJobId() + "] unexpected change to seqNoPrimaryTermAndIndex.";
        }
    }

    @Nullable
    SeqNoPrimaryTermAndIndex getSeqNoPrimaryTermAndIndex() {
        return seqNoPrimaryTermAndIndexHolder.get();
    }

    @Override
    protected void afterFinishOrFailure() {
        closePointInTime();
        super.afterFinishOrFailure();
    }

    @Override
    public boolean maybeTriggerAsyncJob(long now) {
        if (TransformMetadata.upgradeMode(clusterService.state())) {
            logger.debug("[{}] schedule was triggered but the Transform is upgrading. Ignoring trigger.", getJobId());
            return false;
        }
        if (context.isWaitingForIndexToUnblock()) {
            if (destinationIndexHasWriteBlock()) {
                logger.debug("[{}] schedule was triggered but the destination index has a write block. Ignoring trigger.", getJobId());
                return false;
            }
            logger.debug("[{}] destination index is no longer blocked.", getJobId());
            context.setIsWaitingForIndexToUnblock(false);
        }

        return super.maybeTriggerAsyncJob(now);
    }

    private boolean destinationIndexHasWriteBlock() {
        var clusterState = clusterService.state();
        if (clusterState == null) {
            // if we can't determine if the index is blocked, we assume it isn't, even though the bulk request may fail again
            return false;
        }

        var destinationIndexName = transformConfig.getDestination().getIndex();
        var destinationIndex = indexNameExpressionResolver.concreteWriteIndex(
            clusterState,
            IndicesOptions.lenientExpandOpen(),
            destinationIndexName,
            true,
            false
        );
        return destinationIndex != null && clusterState.blocks().indexBlocked(ClusterBlockLevel.WRITE, destinationIndex.getName());
    }

    @Override
    protected void onStop() {
        closePointInTime();
        super.onStop();
    }

    private void closePointInTime() {
        for (String name : namedPits.keySet()) {
            closePointInTime(name);
        }
    }

    private void closePointInTime(String name) {
        PointInTimeBuilder pit = namedPits.remove(name);

        if (pit == null) {
            return;
        }

        BytesReference oldPit = pit.getEncodedId();

        ClosePointInTimeRequest closePitRequest = new ClosePointInTimeRequest(oldPit);
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            TransportClosePointInTimeAction.TYPE,
            closePitRequest,
            ActionListener.wrap(response -> {
                logger.trace("[{}] closed pit search context [{}]", getJobId(), oldPit);
            }, e -> {
                // note: closing the pit should never throw, even if the pit is invalid
                logger.error(() -> "[" + getJobId() + "] Failed to close point in time reader", e);
            })
        );
    }

    private void injectPointInTimeIfNeeded(
        Tuple<String, SearchRequest> namedSearchRequest,
        ActionListener<Tuple<String, SearchRequest>> listener
    ) {
        SearchRequest searchRequest = namedSearchRequest.v2();
        // We explicitly disable PIT in the presence of remote clusters in the source due to huge PIT handles causing performance problems.
        // We should not re-enable until this is resolved: https://github.com/elastic/elasticsearch/issues/80187
        if (disablePit || searchRequest.indices().length == 0 || transformConfig.getSource().requiresRemoteCluster()) {
            listener.onResponse(namedSearchRequest);
            return;
        }

        PointInTimeBuilder pit = namedPits.get(namedSearchRequest.v1());
        if (pit != null) {
            searchRequest.source().pointInTimeBuilder(pit);
            listener.onResponse(namedSearchRequest);
            return;
        }

        // no pit, create a new one
        OpenPointInTimeRequest pitRequest = new OpenPointInTimeRequest(searchRequest.indices()).keepAlive(PIT_KEEP_ALIVE);
        // use index filter for better performance
        pitRequest.indexFilter(transformConfig.getSource().getQueryConfig().getQuery());

        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            TransportOpenPointInTimeAction.TYPE,
            pitRequest,
            ActionListener.wrap(response -> {
                PointInTimeBuilder newPit = new PointInTimeBuilder(response.getPointInTimeId()).setKeepAlive(PIT_KEEP_ALIVE);
                namedPits.put(namedSearchRequest.v1(), newPit);
                searchRequest.source().pointInTimeBuilder(newPit);
                pitCheckpoint = getNextCheckpoint().getCheckpoint();
                logger.trace(
                    "[{}] using pit search context with id [{}]; request [{}]",
                    getJobId(),
                    newPit.getEncodedId(),
                    namedSearchRequest.v1()
                );
                listener.onResponse(namedSearchRequest);
            }, e -> {
                Throwable unwrappedException = ExceptionsHelper.findSearchExceptionRootCause(e);

                // in case of a 404 forward the error, this isn't due to pit usage
                if (unwrappedException instanceof ResourceNotFoundException) {
                    listener.onFailure(e);
                    return;
                }

                // if point in time is not supported, disable it but do not remember forever (stopping and starting will give it another
                // try)
                if (unwrappedException instanceof ActionNotFoundTransportException) {
                    logger.warn(
                        "[{}] source does not support point in time reader, falling back to normal search (more resource intensive)",
                        getJobId()
                    );
                    auditor.warning(
                        getJobId(),
                        "Source does not support point in time reader, falling back to normal search (more resource intensive)"
                    );
                    disablePit = true;
                } else {
                    logger.warn(
                        () -> format("[%s] Failed to create a point in time reader, falling back to normal search.", getJobId()),
                        e
                    );
                }
                listener.onResponse(namedSearchRequest);
            })
        );
    }

    void doSearch(Tuple<String, SearchRequest> namedSearchRequest, ActionListener<SearchResponse> listener) {
        String name = namedSearchRequest.v1();
        SearchRequest originalRequest = namedSearchRequest.v2();
        // We want to treat a request to search 0 indices as a request to do nothing, not a request to search all indices
        if (originalRequest.indices().length == 0) {
            logger.debug("[{}] Search request [{}] optimized to noop; searchRequest [{}]", getJobId(), name, originalRequest);
            listener.onResponse(null);
            return;
        }

        final SearchRequest searchRequest;
        PointInTimeBuilder pit = originalRequest.pointInTimeBuilder();
        if (pit != null) {
            // remove the indices from the request, they will be derived from the provided pit
            searchRequest = new SearchRequest(originalRequest).indices(new String[0]).indicesOptions(SearchRequest.DEFAULT_INDICES_OPTIONS);
        } else {
            searchRequest = originalRequest;
        }
        logger.trace("searchRequest: [{}]", searchRequest);

        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            TransportSearchAction.TYPE,
            searchRequest,
            ActionListener.wrap(response -> {
                // did the pit change?
                if (response.pointInTimeId() != null && (pit == null || response.pointInTimeId().equals(pit.getEncodedId())) == false) {
                    namedPits.put(name, new PointInTimeBuilder(response.pointInTimeId()).setKeepAlive(PIT_KEEP_ALIVE));
                    logger.trace("point in time handle has changed; request [{}]", name);
                }

                listener.onResponse(response);
            }, e -> {
                // check if the error has been caused by a missing search context, which could be a timed out pit
                // re-try this search without pit, if it fails again the normal failure handler is called, if it
                // succeeds a new pit gets created at the next run
                Throwable unwrappedException = ExceptionsHelper.findSearchExceptionRootCause(e);
                if (unwrappedException instanceof SearchContextMissingException) {
                    logger.warn(
                        () -> format("[%s] Search context missing, falling back to normal search; request [%s]", getJobId(), name),
                        e
                    );
                    namedPits.remove(name);
                    originalRequest.source().pointInTimeBuilder(null);
                    ClientHelper.executeWithHeadersAsync(
                        transformConfig.getHeaders(),
                        ClientHelper.TRANSFORM_ORIGIN,
                        client,
                        TransportSearchAction.TYPE,
                        originalRequest,
                        listener
                    );
                    return;
                }
                if (unwrappedException instanceof IndexNotFoundException && pit != null) {
                    /*
                     * gh#81252 pit API search request can fail if indices get deleted (by ILM)
                     * fall-back to normal search, the pit gets re-created (with an updated set of indices) on the next run
                     *
                     * Note: Due to BWC this needs to be kept until CCS support for < 8.1 is dropped
                     */
                    namedPits.remove(name);
                    originalRequest.source().pointInTimeBuilder(null);
                    ClientHelper.executeWithHeadersAsync(
                        transformConfig.getHeaders(),
                        ClientHelper.TRANSFORM_ORIGIN,
                        client,
                        TransportSearchAction.TYPE,
                        originalRequest,
                        listener
                    );
                    return;
                }

                listener.onFailure(e);
            })
        );
    }

    private static String getBulkIndexDetailedFailureMessage(String prefix, Map<String, BulkItemResponse> failures) {
        if (failures.isEmpty()) {
            return "";
        }

        StringBuilder failureMessageBuilder = new StringBuilder(prefix);
        for (Entry<String, BulkItemResponse> failure : failures.entrySet()) {
            failureMessageBuilder.append("\n[")
                .append(failure.getKey())
                .append("] message [")
                .append(failure.getValue().getFailureMessage())
                .append("]");
        }
        String failureMessage = failureMessageBuilder.toString();
        return failureMessage;
    }

    private static Throwable decorateBulkIndexException(Throwable irrecoverableException) {
        if (irrecoverableException instanceof DocumentParsingException) {
            return new TransformException(
                "Destination index mappings are incompatible with the transform configuration.",
                irrecoverableException
            );
        }

        return irrecoverableException;
    }
}
