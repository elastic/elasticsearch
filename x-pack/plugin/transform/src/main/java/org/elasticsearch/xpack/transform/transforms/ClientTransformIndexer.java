/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClosePointInTimeAction;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeAction;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.CheckpointProvider;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.transforms.pivot.SchemaUtil;
import org.elasticsearch.xpack.transform.utils.ExceptionRootCauseFinder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class ClientTransformIndexer extends TransformIndexer {

    private static final TimeValue PIT_KEEP_ALIVE = TimeValue.timeValueSeconds(30);
    private static final Logger logger = LogManager.getLogger(ClientTransformIndexer.class);

    private final Client client;
    private final AtomicBoolean oldStatsCleanedUp = new AtomicBoolean(false);

    private final AtomicReference<SeqNoPrimaryTermAndIndex> seqNoPrimaryTermAndIndex;
    private final ConcurrentHashMap<String, PointInTimeBuilder> namedPits = new ConcurrentHashMap<>();
    private volatile long pitCheckpoint;
    private volatile boolean disablePit = false;

    ClientTransformIndexer(
        ThreadPool threadPool,
        TransformServices transformServices,
        CheckpointProvider checkpointProvider,
        AtomicReference<IndexerState> initialState,
        TransformIndexerPosition initialPosition,
        Client client,
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
        this.seqNoPrimaryTermAndIndex = new AtomicReference<>(seqNoPrimaryTermAndIndex);

        // TODO: move into context constructor
        context.setShouldStopAtCheckpoint(shouldStopAtCheckpoint);
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
            ActionListener.wrap(pitSearchRequest -> { doSearch(pitSearchRequest, nextPhase); }, nextPhase::onFailure)
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
            BulkAction.INSTANCE,
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
                deduplicatedFailures.putIfAbsent(item.getFailure().getCause().getClass().getSimpleName(), item);
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
    protected void refreshDestinationIndex(ActionListener<RefreshResponse> responseListener) {
        // note: this gets executed _without_ the headers of the user as the user might not have the rights to call
        // _refresh for performance reasons. However this refresh is an internal detail of transform and this is only
        // called for the transform destination index
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.TRANSFORM_ORIGIN,
            RefreshAction.INSTANCE,
            new RefreshRequest(transformConfig.getDestination().getIndex()),
            responseListener
        );
    }

    @Override
    void doGetInitialProgress(SearchRequest request, ActionListener<SearchResponse> responseListener) {
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            SearchAction.INSTANCE,
            request,
            responseListener
        );
    }

    @Override
    void doGetFieldMappings(ActionListener<Map<String, String>> fieldMappingsListener) {
        SchemaUtil.getDestinationFieldMappings(client, getConfig().getDestination().getIndex(), fieldMappingsListener);
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
                if (org.elasticsearch.ExceptionsHelper.unwrapCause(statsExc) instanceof VersionConflictEngineException) {
                    // this should never happen, but indicates a race condition in state persistence:
                    // - there should be only 1 save persistence at a time
                    // - this is not a catastrophic failure, if 2 state persistence calls run at the same time, 1 should succeed and update
                    // seqNoPrimaryTermAndIndex
                    // - for tests fail(assert), so we can debug the problem
                    logger.error(
                        new ParameterizedMessage(
                            "[{}] updating stats of transform failed, unexpected version conflict of internal state, resetting to recover.",
                            transformConfig.getId()
                        ),
                        statsExc
                    );
                    auditor.warning(
                        getJobId(),
                        "Failure updating stats of transform, unexpected version conflict of internal state, resetting to recover: "
                            + statsExc.getMessage()
                    );
                    assert false : "[" + getJobId() + "] updating stats of transform failed, unexpected version conflict of internal state";
                } else {
                    logger.error(new ParameterizedMessage("[{}] updating stats of transform failed.", transformConfig.getId()), statsExc);
                    auditor.warning(getJobId(), "Failure updating stats of transform: " + statsExc.getMessage());
                }
                listener.onFailure(statsExc);
            })
        );
    }

    void updateSeqNoPrimaryTermAndIndex(SeqNoPrimaryTermAndIndex expectedValue, SeqNoPrimaryTermAndIndex newValue) {
        logger.debug(
            () -> new ParameterizedMessage(
                "[{}] Updated state document from [{}] to [{}]",
                transformConfig.getId(),
                expectedValue,
                newValue
            )
        );
        boolean updated = seqNoPrimaryTermAndIndex.compareAndSet(expectedValue, newValue);
        // This should never happen. We ONLY ever update this value if at initialization or we just finished updating the document
        // famous last words...
        if (updated == false) {
            logger.warn(
                "[{}] Unexpected change to internal state detected, expected [{}], got [{}]",
                transformConfig.getId(),
                expectedValue,
                seqNoPrimaryTermAndIndex.get()
            );
            assert updated : "[" + getJobId() + "] unexpected change to seqNoPrimaryTermAndIndex.";
        }
    }

    @Nullable
    SeqNoPrimaryTermAndIndex getSeqNoPrimaryTermAndIndex() {
        return seqNoPrimaryTermAndIndex.get();
    }

    @Override
    protected void afterFinishOrFailure() {
        closePointInTime();
        super.afterFinishOrFailure();
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

        String oldPit = pit.getEncodedId();

        ClosePointInTimeRequest closePitRequest = new ClosePointInTimeRequest(oldPit);
        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            ClosePointInTimeAction.INSTANCE,
            closePitRequest,
            ActionListener.wrap(response -> { logger.trace("[{}] closed pit search context [{}]", getJobId(), oldPit); }, e -> {
                // note: closing the pit should never throw, even if the pit is invalid
                logger.error(new ParameterizedMessage("[{}] Failed to close point in time reader", getJobId()), e);
            })
        );
    }

    private void injectPointInTimeIfNeeded(
        Tuple<String, SearchRequest> namedSearchRequest,
        ActionListener<Tuple<String, SearchRequest>> listener
    ) {
        if (disablePit) {
            listener.onResponse(namedSearchRequest);
            return;
        }

        SearchRequest searchRequest = namedSearchRequest.v2();
        PointInTimeBuilder pit = namedPits.get(namedSearchRequest.v1());
        if (pit != null) {
            searchRequest.source().pointInTimeBuilder(pit);
            listener.onResponse(namedSearchRequest);
            return;
        }

        // no pit, create a new one
        OpenPointInTimeRequest pitRequest = new OpenPointInTimeRequest(searchRequest.indices()).keepAlive(PIT_KEEP_ALIVE);

        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            OpenPointInTimeAction.INSTANCE,
            pitRequest,
            ActionListener.wrap(response -> {
                PointInTimeBuilder newPit = new PointInTimeBuilder(response.getPointInTimeId()).setKeepAlive(PIT_KEEP_ALIVE);
                namedPits.put(namedSearchRequest.v1(), newPit);
                searchRequest.source().pointInTimeBuilder(newPit);
                pitCheckpoint = getNextCheckpoint().getCheckpoint();
                logger.trace("[{}] using pit search context with id [{}]", getJobId(), newPit.getEncodedId());
                listener.onResponse(namedSearchRequest);
            }, e -> {
                Throwable unwrappedException = ExceptionsHelper.findSearchExceptionRootCause(e);
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
                        new ParameterizedMessage(
                            "[{}] Failed to create a point in time reader, falling back to normal search.",
                            getJobId()
                        ),
                        e
                    );
                }
                listener.onResponse(namedSearchRequest);
            })
        );
    }

    private void doSearch(Tuple<String, SearchRequest> namedSearchRequest, ActionListener<SearchResponse> listener) {
        logger.trace("searchRequest: {}", namedSearchRequest.v2());

        PointInTimeBuilder pit = namedSearchRequest.v2().pointInTimeBuilder();

        ClientHelper.executeWithHeadersAsync(
            transformConfig.getHeaders(),
            ClientHelper.TRANSFORM_ORIGIN,
            client,
            SearchAction.INSTANCE,
            namedSearchRequest.v2(),
            ActionListener.wrap(response -> {
                // did the pit change?
                if (response.pointInTimeId() != null && (pit == null || response.pointInTimeId() != pit.getEncodedId())) {
                    namedPits.put(namedSearchRequest.v1(), new PointInTimeBuilder(response.pointInTimeId()).setKeepAlive(PIT_KEEP_ALIVE));
                    logger.trace("point in time handle has changed");
                }

                listener.onResponse(response);
            }, e -> {
                // check if the error has been caused by a missing search context, which could be a timed out pit
                // re-try this search without pit, if it fails again the normal failure handler is called, if it
                // succeeds a new pit gets created at the next run
                Throwable unwrappedException = ExceptionsHelper.findSearchExceptionRootCause(e);
                if (unwrappedException instanceof SearchContextMissingException) {
                    logger.warn(new ParameterizedMessage("[{}] Search context missing, falling back to normal search.", getJobId()), e);
                    namedPits.remove(namedSearchRequest.v1());
                    namedSearchRequest.v2().source().pointInTimeBuilder(null);
                    ClientHelper.executeWithHeadersAsync(
                        transformConfig.getHeaders(),
                        ClientHelper.TRANSFORM_ORIGIN,
                        client,
                        SearchAction.INSTANCE,
                        namedSearchRequest.v2(),
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
        if (irrecoverableException instanceof MapperParsingException) {
            return new TransformException(
                "Destination index mappings are incompatible with the transform configuration.",
                irrecoverableException
            );
        }

        return irrecoverableException;
    }
}
