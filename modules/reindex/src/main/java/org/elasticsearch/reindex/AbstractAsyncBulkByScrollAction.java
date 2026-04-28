/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.index.reindex.WorkerBulkByScrollTaskState;
import org.elasticsearch.reindex.remote.RemotePitPaginatedHitSource;
import org.elasticsearch.reindex.remote.RemoteScrollablePaginatedHitSource;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.Metadata;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES;
import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

/**
 * Abstract base for scrolling across a search and executing bulk actions on all results. All package private methods are package private so
 * their tests can use them. Most methods run in the listener thread pool because they are meant to be fast and don't expect to block.
 */
public abstract class AbstractAsyncBulkByScrollAction<
    Request extends AbstractBulkByScrollRequest<Request>,
    Action extends TransportAction<Request, ?>> {

    protected final Logger logger;
    protected final BulkByScrollTask task;
    protected final WorkerBulkByScrollTaskState worker;
    protected final ThreadPool threadPool;
    protected final ScriptService scriptService;
    protected final ReindexSslConfig sslConfig;

    /**
     * The request for this action. Named mainRequest because we create lots of <code>request</code> variables all representing child
     * requests of this mainRequest.
     */
    protected final Request mainRequest;

    private final AtomicLong startTimeEpochMillis = new AtomicLong(-1);
    private final Set<String> destinationIndices = ConcurrentCollections.newConcurrentSet();

    private final ParentTaskAssigningClient searchClient;
    private final ParentTaskAssigningClient bulkClient;
    private final ActionListener<BulkByScrollResponse> listener;
    private final Retry bulkRetry;
    private final PaginatedHitSource paginatedHitSource;

    /**
     * This BiFunction is used to apply various changes depending of the Reindex action and  the search hit,
     * from copying search hit metadata (parent, routing, etc) to potentially transforming the
     * {@link RequestWrapper} completely.
     */
    private final BiFunction<RequestWrapper<?>, PaginatedHitSource.Hit, RequestWrapper<?>> scriptApplier;
    private int lastBatchSize;
    /**
     * The current scroll response being processed. Set atomically so that either {@link #prepareBulkRequest} or
     * {@link #finishHim(Exception, List, List, boolean)} can claim exclusive ownership of the remaining hits and release them exactly once.
     */
    private final AtomicReference<ScrollConsumableHitsResponse> currentScrollResponse = new AtomicReference<>();
    /**
     * Set to {@code true} at the start of {@link #finishHim(Exception, List, List, boolean)} so {@link #prepareBulkRequest} can still
     * release unconsumed hits when {@link #currentScrollResponse} is temporarily {@code null} after prepare's CAS (before the ref is
     * restored when {@code maxDocs} leaves a partial batch).
     */
    private final AtomicBoolean requestFinishing = new AtomicBoolean(false);
    /**
     * Keeps track of the total number of bulk operations performed
     * from a single scroll response. It is possible that
     * multiple bulk requests are performed from a single scroll
     * response, meaning that we have to take into account the total
     * in order to compute a correct scroll keep alive time.
     */
    private final AtomicInteger totalBatchSizeInSingleScrollResponse = new AtomicInteger();
    /**
     * Minimum time a relocated task must run before it can be relocated again.
     * Prevents quick back-to-back relocations that could cause issues with tasks endpoints,
     * as of writing, mainly race condition in list where a two-listing approach could hit two relocations and have missing results.
     */
    private final long relocationCooldownNanos;

    /**
     * Version of the remote cluster when reindexing from remote, or null when reindexing locally.
     */
    protected final Version remoteVersion;

    AbstractAsyncBulkByScrollAction(
        BulkByScrollTask task,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm,
        boolean needsVectors,
        Logger logger,
        ParentTaskAssigningClient client,
        ThreadPool threadPool,
        Request mainRequest,
        ActionListener<BulkByScrollResponse> listener,
        @Nullable ScriptService scriptService,
        @Nullable ReindexSslConfig sslConfig,
        TimeValue maxTaskShutdownGracePeriod
    ) {
        this(
            task,
            needsSourceDocumentVersions,
            needsSourceDocumentSeqNoAndPrimaryTerm,
            needsVectors,
            logger,
            client,
            client,
            threadPool,
            mainRequest,
            listener,
            scriptService,
            sslConfig,
            null,
            maxTaskShutdownGracePeriod
        );
    }

    AbstractAsyncBulkByScrollAction(
        BulkByScrollTask task,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm,
        boolean needsVectors,
        Logger logger,
        ParentTaskAssigningClient searchClient,
        ParentTaskAssigningClient bulkClient,
        ThreadPool threadPool,
        Request mainRequest,
        ActionListener<BulkByScrollResponse> listener,
        @Nullable ScriptService scriptService,
        @Nullable ReindexSslConfig sslConfig,
        @Nullable Version remoteVersion,
        TimeValue maxTaskShutdownGracePeriod
    ) {
        this.task = task;
        this.scriptService = scriptService;
        this.sslConfig = sslConfig;
        if (task.isWorker() == false) {
            throw new IllegalArgumentException("Given task [" + task.getId() + "] must have a child worker");
        }
        this.worker = task.getWorkerState();

        this.logger = logger;
        this.searchClient = searchClient;
        this.bulkClient = bulkClient;
        this.threadPool = threadPool;
        this.mainRequest = mainRequest;
        this.relocationCooldownNanos = computeRelocationCooldownNanos(maxTaskShutdownGracePeriod);
        this.listener = listener;
        BackoffPolicy backoffPolicy = buildBackoffPolicy();
        bulkRetry = new Retry(BackoffPolicy.wrap(backoffPolicy, worker::countBulkRetry), threadPool);
        this.remoteVersion = remoteVersion;
        paginatedHitSource = buildScrollableResultSource(
            backoffPolicy,
            prepareSearchRequest(mainRequest, needsSourceDocumentVersions, needsSourceDocumentSeqNoAndPrimaryTerm, needsVectors)
        );
        scriptApplier = Objects.requireNonNull(buildScriptApplier(), "script applier must not be null");
    }

    /** Computes the minimum time a relocated task must run before it can be relocated again. Visible for testing. */
    static long computeRelocationCooldownNanos(TimeValue maxTaskShutdownGracePeriod) {
        assert maxTaskShutdownGracePeriod.nanos() >= 0 : "shutdownTimeout must be non-negative"; // implicit nullcheck
        // 5s should give us strong guarantees that list race condition won't happen, anything above 30s is likely redundant
        final long lowerBoundNanos = TimeUnit.SECONDS.toNanos(5);
        final long upperBoundNanos = TimeUnit.SECONDS.toNanos(30);
        return Math.clamp(maxTaskShutdownGracePeriod.nanos() / 2, lowerBoundNanos, upperBoundNanos);
    }

    /**
     * Prepares a search request to be used in a {@link PaginatedHitSource}.
     * Preparation might set a sort order (if not set already) and disable scroll if max docs is small enough.
     */
    // Visible for testing
    static <Request extends AbstractBulkByScrollRequest<Request>> SearchRequest prepareSearchRequest(
        Request mainRequest,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm,
        boolean needsVectors
    ) {
        var preparedSearchRequest = new SearchRequest(mainRequest.getSearchRequest());

        /*
         * Default to sorting by doc. We can't do this in the request itself because it is normal to *add* to the sorts rather than replace
         * them and if we add _doc as the first sort by default then sorts will never work.... So we add it here, only if there isn't
         * another sort.
         *
         * When using PIT, use _shard_doc for search_after compatibility and performance (see paginate-search-results docs).
         * When using scroll, use _doc.
         *
         * This modifies the original request!
         */
        final SearchSourceBuilder sourceBuilder = preparedSearchRequest.source();
        List<SortBuilder<?>> sorts = sourceBuilder.sorts();
        if (sorts == null || sorts.isEmpty()) {
            if (sourceBuilder.pointInTimeBuilder() != null) {
                sourceBuilder.sort(fieldSort(FieldSortBuilder.SHARD_DOC_FIELD_NAME));
            } else {
                sourceBuilder.sort(fieldSort("_doc"));
            }
        }
        sourceBuilder.version(needsSourceDocumentVersions);
        sourceBuilder.seqNoAndPrimaryTerm(needsSourceDocumentSeqNoAndPrimaryTerm);

        if (needsVectors) {
            // always include vectors in the response unless explicitly set
            var fetchSource = sourceBuilder.fetchSource();
            if (fetchSource == null) {
                sourceBuilder.fetchSource(FetchSourceContext.FETCH_ALL_SOURCE_EXCLUDE_INFERENCE_FIELDS);
            } else if (fetchSource.excludeVectors() == null) {
                sourceBuilder.excludeVectors(false);
            }
        }

        // When using PIT, scroll must not be set (PIT and scroll are mutually exclusive), and 'from' must be 0 for search_after
        // compatibility.
        if (sourceBuilder.pointInTimeBuilder() != null) {
            preparedSearchRequest.scroll(null);
            sourceBuilder.from(0);
        }
        // Do not open scroll if max docs <= scroll size and not resuming on version conflicts.
        // Sliced searches must keep scroll (or use PIT above). Slicing without scroll or PIT fails SearchRequest validation.
        else if (sourceBuilder.slice() == null
            && mainRequest.getMaxDocs() != MAX_DOCS_ALL_MATCHES
            && mainRequest.getMaxDocs() <= preparedSearchRequest.source().size()
            && mainRequest.isAbortOnVersionConflict()) {
                preparedSearchRequest.scroll(null);
            }

        return preparedSearchRequest;
    }

    /**
     * Build the {@link BiFunction} to apply to all {@link RequestWrapper}.
     *
     * Public for testings....
     */
    public BiFunction<RequestWrapper<?>, PaginatedHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
        // The default script applier executes a no-op
        return (request, searchHit) -> request;
    }

    /**
     * Build the {@link RequestWrapper} for a single search hit. This shouldn't handle
     * metadata or scripting. That will be handled by copyMetadata and
     * apply functions that can be overridden.
     */
    protected abstract RequestWrapper<?> buildRequest(PaginatedHitSource.Hit doc);

    /**
     * Copies the metadata from a hit to the request.
     */
    protected RequestWrapper<?> copyMetadata(RequestWrapper<?> request, PaginatedHitSource.Hit doc) {
        copyRouting(request, doc.getRouting());
        return request;
    }

    /**
     * Copy the routing from a search hit to the request.
     */
    protected void copyRouting(RequestWrapper<?> request, String routing) {
        request.setRouting(routing);
    }

    /**
     * Used to accept or ignore a search hit. Ignored search hits will be excluded
     * from the bulk request. It is also where we fail on invalid search hits, like
     * when the document has no source but it's required.
     */
    protected boolean accept(PaginatedHitSource.Hit doc) {
        if (doc.getSource() == null) {
            /*
             * Either the document didn't store _source or we didn't fetch it for some reason. Since we don't allow the user to
             * change the "fields" part of the search request it is unlikely that we got here because we didn't fetch _source.
             * Thus the error message assumes that it wasn't stored.
             */
            throw new IllegalArgumentException("[" + doc.getIndex() + "][" + doc.getId() + "] didn't store _source");
        }
        return true;
    }

    protected BulkRequest buildBulk(Iterable<? extends PaginatedHitSource.Hit> docs) {
        BulkRequest bulkRequest = new BulkRequest();
        for (PaginatedHitSource.Hit doc : docs) {
            if (accept(doc)) {
                RequestWrapper<?> request = scriptApplier.apply(copyMetadata(buildRequest(doc), doc), doc);
                if (request != null) {
                    bulkRequest.add(request.self());
                }
            }
        }
        return bulkRequest;
    }

    protected PaginatedHitSource buildScrollableResultSource(BackoffPolicy backoffPolicy, SearchRequest searchRequest) {
        // If we're using point-in-time search, then return a ClientPitPaginatedHitSource
        if (searchRequest.source() != null && searchRequest.source().pointInTimeBuilder() != null) {
            return new ClientPitPaginatedHitSource(
                logger,
                backoffPolicy,
                threadPool,
                worker::countSearchRetry,
                this::onScrollResponse,
                this::finishHim,
                searchClient,
                searchRequest
            );
        }
        // Default to scroll
        return new ClientScrollablePaginatedHitSource(
            logger,
            backoffPolicy,
            threadPool,
            worker::countSearchRetry,
            this::onScrollResponse,
            this::finishHim,
            searchClient,
            searchRequest
        );
    }

    /**
     * Build the response for reindex actions.
     */
    protected BulkByScrollResponse buildResponse(
        TimeValue took,
        List<BulkItemResponse.Failure> indexingFailures,
        List<PaginatedSearchFailure> searchFailures,
        boolean timedOut
    ) {
        BytesReference pitId = paginatedHitSource instanceof PitPaginatedHitSource pit ? pit.getPitId() : null;
        return new BulkByScrollResponse(took, task.getStatus(), indexingFailures, searchFailures, timedOut, null, pitId);
    }

    /**
     * Start the worker action by firing the initial search request or resume search from the resumeInfo state
     */
    public void start() {
        logger.debug("[{}]: starting", task.getId());
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        try {
            if (mainRequest.getResumeInfo().isPresent()) {
                var resumeInfo = mainRequest.getResumeInfo().get();
                // At this point only worker task can be started, leader task would have split slices into worker tasks
                assert resumeInfo.getWorker().isPresent() : "Resume info for worker task must have worker resume info";
                WorkerResumeInfo workerResumeInfo = resumeInfo.getWorker().get();
                startTimeEpochMillis.set(workerResumeInfo.startTimeEpochMillis());
                worker.restoreState(workerResumeInfo.status());
                paginatedHitSource.resume(workerResumeInfo);
            } else {
                startTimeEpochMillis.set(System.currentTimeMillis());
                paginatedHitSource.start();
            }
        } catch (Exception e) {
            finishHim(e);
        }
    }

    void onScrollResponse(PaginatedHitSource.AsyncResponse asyncResponse) {
        onScrollResponse(new ScrollConsumableHitsResponse(asyncResponse));
    }

    void onScrollResponse(ScrollConsumableHitsResponse asyncResponse) {
        // lastBatchStartTime is essentially unused (see WorkerBulkByScrollTaskState.throttleWaitTime. Leaving it for now, since it seems
        // like a bug?
        onScrollResponse(System.nanoTime(), this.lastBatchSize, asyncResponse);
    }

    /**
     * Process a scroll response.
     * @param lastBatchStartTimeNS the time when the last batch started. Used to calculate the throttling delay.
     * @param lastBatchSizeToUse the size of the last batch. Used to calculate the throttling delay.
     * @param asyncResponse the response to process from {@link PaginatedHitSource}
     */
    void onScrollResponse(long lastBatchStartTimeNS, int lastBatchSizeToUse, ScrollConsumableHitsResponse asyncResponse) {
        currentScrollResponse.set(asyncResponse);
        PaginatedHitSource.Response response = asyncResponse.response();
        logger.debug("[{}]: got scroll response with [{}] hits", task.getId(), asyncResponse.remainingHits());
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            tryReleaseCurrentResponse(asyncResponse);
            finishHim(null);
            return;
        }
        if (    // If any of the shards failed that should abort the request.
        (response.getFailures().size() > 0)
            // Timeouts aren't shard failures but we still need to pass them back to the user.
            || response.isTimedOut()) {
            tryReleaseCurrentResponse(asyncResponse);
            refreshAndFinish(emptyList(), response.getFailures(), response.isTimedOut());
            return;
        }
        long total = response.getTotalHits();
        if (mainRequest.getMaxDocs() > 0) {
            total = min(total, mainRequest.getMaxDocs());
        }
        worker.setTotal(total);
        AbstractRunnable prepareBulkRequestRunnable = new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                /*
                 * It is important that the batch start time be calculated from here, scroll response to scroll response. That way the time
                 * waiting on the scroll doesn't count against this batch in the throttle.
                 */
                prepareBulkRequest(System.nanoTime(), asyncResponse);
            }

            @Override
            public void onFailure(Exception e) {
                tryReleaseCurrentResponse(asyncResponse);
                finishHim(e);
            }
        };
        prepareBulkRequestRunnable = (AbstractRunnable) threadPool.getThreadContext().preserveContext(prepareBulkRequestRunnable);
        worker.delayPrepareBulkRequest(threadPool, lastBatchStartTimeNS, lastBatchSizeToUse, prepareBulkRequestRunnable);
    }

    /**
     * Prepare the bulk request. Called on the generic thread pool after some preflight checks have been done one the SearchResponse and any
     * delay has been slept. Uses the generic thread pool because reindex is rare enough not to need its own thread pool and because the
     * thread may be blocked by the user script.
     */
    void prepareBulkRequest(long thisBatchStartTimeNS, ScrollConsumableHitsResponse asyncResponse) {
        logger.debug("[{}]: preparing bulk request", task.getId());
        // Atomically claim ownership of the response. If finishHim already claimed it (CAS returns false),
        // the hits have already been released — nothing to do here.
        if (currentScrollResponse.compareAndSet(asyncResponse, null) == false) {
            return;
        }
        if (requestFinishing.get()) {
            asyncResponse.releaseRemainingHits();
            return;
        }
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            asyncResponse.releaseRemainingHits();
            finishHim(null);
            return;
        }
        if (asyncResponse.hasRemainingHits() == false) {
            refreshAndFinish(emptyList(), emptyList(), false);
            return;
        }
        worker.countBatch();
        final List<? extends PaginatedHitSource.Hit> hits;

        if (mainRequest.getMaxDocs() != MAX_DOCS_ALL_MATCHES) {
            // Truncate the hits if we have more than the request max docs
            long remainingDocsToProcess = max(0, mainRequest.getMaxDocs() - worker.getSuccessfullyProcessed());
            hits = remainingDocsToProcess < asyncResponse.remainingHits()
                ? asyncResponse.consumeHits((int) remainingDocsToProcess)
                : asyncResponse.consumeRemainingHits();
        } else {
            hits = asyncResponse.consumeRemainingHits();
        }

        // If there are unconsumed hits (e.g. maxDocs truncated the batch), restore the reference so finishHim can release them
        // if the operation ends before the next prepareBulkRequest runs (e.g. bulk failure, maxDocs reached in onBulkResponse).
        if (asyncResponse.hasRemainingHits()) {
            currentScrollResponse.set(asyncResponse);
        }

        if (requestFinishing.get()) {
            releaseHits(hits);
            asyncResponse.releaseRemainingHits();
            return;
        }

        final Releasable releaseBatchHits = Releasables.releaseOnce(() -> releaseHits(hits));
        boolean releaseBatchHitsHandedOff = false;
        try {
            final BulkRequest request = buildBulk(hits);
            if (request.requests().isEmpty()) {
                /*
                 * If we noop-ed the entire batch then just skip to the next batch or the BulkRequest would fail validation.
                 */
                notifyDone(thisBatchStartTimeNS, asyncResponse, 0);
                return;
            }
            request.timeout(mainRequest.getTimeout());
            request.waitForActiveShards(mainRequest.getWaitForActiveShards());
            sendBulkRequest(request, releaseBatchHits, () -> notifyDone(thisBatchStartTimeNS, asyncResponse, request.requests().size()));
            releaseBatchHitsHandedOff = true;
        } finally {
            if (releaseBatchHitsHandedOff == false) {
                releaseBatchHits.close();
            }
        }
    }

    /**
     * Send a bulk request, handling retries. Releases {@code releaseBatchHits} on cancellation, terminal finish, and before the bulk
     * listener completes (success or failure) so hits are never leaked.
     */
    void sendBulkRequest(BulkRequest request, Releasable releaseBatchHits, Runnable onSuccess) {
        final int requestSize = request.requests().size();
        if (logger.isDebugEnabled()) {
            logger.debug(
                "[{}]: sending [{}] entry, [{}] bulk request",
                task.getId(),
                requestSize,
                ByteSizeValue.ofBytes(request.estimatedSizeInBytes())
            );
        }
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            releaseBatchHits.close();
            finishHim(null);
            return;
        }
        if (requestFinishing.get()) {
            releaseBatchHits.close();
            return;
        }
        bulkRetry.withBackoff(bulkClient::bulk, request, ActionListener.releaseBefore(releaseBatchHits, ActionListener.wrap(response -> {
            logger.debug("[{}]: completed [{}] entry bulk request", task.getId(), requestSize);
            onBulkResponse(response, onSuccess);
        }, this::finishHim)));
    }

    /**
     * Processes bulk responses, accounting for failures.
     */
    void onBulkResponse(BulkResponse response, Runnable onSuccess) {
        try {
            List<Failure> failures = new ArrayList<>();
            Set<String> destinationIndicesThisBatch = new HashSet<>();
            for (BulkItemResponse item : response) {
                if (item.isFailed()) {
                    recordFailure(item.getFailure(), failures);
                    continue;
                }
                switch (item.getOpType()) {
                    case CREATE:
                    case INDEX:
                        if (item.getResponse().getResult() == DocWriteResponse.Result.CREATED) {
                            worker.countCreated();
                        } else {
                            worker.countUpdated();
                        }
                        break;
                    case UPDATE:
                        worker.countUpdated();
                        break;
                    case DELETE:
                        worker.countDeleted();
                        break;
                }
                // Track the indexes we've seen so we can refresh them if requested
                destinationIndicesThisBatch.add(item.getIndex());
            }

            if (task.isCancelled()) {
                logger.debug("[{}]: Finishing early because the task was cancelled", task.getId());
                finishHim(null);
                return;
            }

            addDestinationIndices(destinationIndicesThisBatch);

            if (false == failures.isEmpty()) {
                refreshAndFinish(unmodifiableList(failures), emptyList(), false);
                return;
            }

            if (mainRequest.getMaxDocs() != MAX_DOCS_ALL_MATCHES && worker.getSuccessfullyProcessed() >= mainRequest.getMaxDocs()) {
                // We've processed all the requested docs.
                refreshAndFinish(emptyList(), emptyList(), false);
                return;
            }

            if (paginatedHitSource.hasMoreBatches() == false) {
                // Index contains fewer matching docs than max_docs (found < max_docs <= scroll size)
                refreshAndFinish(emptyList(), emptyList(), false);
                return;
            }

            onSuccess.run();
        } catch (Exception t) {
            finishHim(t);
        }
    }

    void notifyDone(long thisBatchStartTimeNS, ScrollConsumableHitsResponse asyncResponse, int batchSize) {
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        this.lastBatchSize = batchSize;
        this.totalBatchSizeInSingleScrollResponse.addAndGet(batchSize);

        if (asyncResponse.hasRemainingHits()) {
            // NB this means the next bulk task will be traced as a child of the current one, but it should really be a sibling
            onScrollResponse(asyncResponse);
            return;
        }
        if (task.isRelocationRequested()) {
            final boolean tooYoungToRelocateAgain = task.isRelocatedTask()
                && (System.nanoTime() - task.getStartTimeNanos()) < relocationCooldownNanos;
            if (tooYoungToRelocateAgain) {
                logger.debug("skipping re-relocation for recently-relocated task [{}]", task.getId());
            } else {
                final Optional<String> nodeToRelocateTo = worker.getNodeToRelocateTo();
                if (nodeToRelocateTo.isPresent()) {
                    final PaginatedHitSource.Response paginatedHitSourceResponse = asyncResponse.response();
                    final WorkerResumeInfo workerResumeInfo;
                    if (paginatedHitSource instanceof PitPaginatedHitSource pit) {
                        final Object[] searchAfterValues = paginatedHitSourceResponse.getSearchAfterValues();
                        if (searchAfterValues == null) {
                            throw new IllegalStateException("PIT relocation requires search_after values from the last hit");
                        }
                        final BytesReference pitIdForResume = paginatedHitSourceResponse.getPitId() != null
                            ? paginatedHitSourceResponse.getPitId()
                            : pit.getPitId();
                        final Version remoteVersion = paginatedHitSource instanceof RemotePitPaginatedHitSource s
                            ? s.remoteVersion().orElseThrow(() -> new IllegalStateException("Remote PIT version should be set"))
                            : null;
                        workerResumeInfo = new ResumeInfo.PitWorkerResumeInfo(
                            pitIdForResume,
                            searchAfterValues,
                            startTimeEpochMillis.get(),
                            worker.getStatus(),
                            remoteVersion
                        );
                    } else {
                        final Version remoteVersion = paginatedHitSource instanceof RemoteScrollablePaginatedHitSource s
                            ? s.remoteVersion().orElseThrow(() -> new IllegalStateException("Remote scroll version should be set"))
                            : null;
                        workerResumeInfo = new ResumeInfo.ScrollWorkerResumeInfo(
                            paginatedHitSourceResponse.getScrollId(),
                            startTimeEpochMillis.get(),
                            worker.getStatus(),
                            remoteVersion
                        );
                    }
                    final ResumeInfo resumeInfo = new ResumeInfo(task.relocationOrigin(), workerResumeInfo, null);
                    // This response is a local carrier for resumeInfo — for higher-level code to handle relocation and then discard.
                    // However, status must be accurate for sliced tasks only, the leader state stores this response and derives
                    // its own combined status from it to serialize to .tasks index.
                    // For non-sliced, status is unused (comes from the worker state).
                    BytesReference pitId = paginatedHitSource instanceof PitPaginatedHitSource pit ? pit.getPitId() : null;
                    final BulkByScrollResponse response = new BulkByScrollResponse(
                        TimeValue.MINUS_ONE,
                        task.getStatus(),
                        List.of(),
                        List.of(),
                        false,
                        resumeInfo,
                        pitId
                    );
                    // Don't call finishHim — it clears the pagination which the relocated task needs.
                    // Do close local resources (e.g. the remote REST client) that won't be reused.
                    paginatedHitSource.cleanupWithoutClosingPagination(
                        threadPool.getThreadContext().preserveContext(() -> listener.onResponse(response))
                    );
                    return;
                }
            }
            // if we can't relocate, continue. we could still finish gracefully, or eventually meet the conditions for relocation.
        }

        int totalBatchSize = totalBatchSizeInSingleScrollResponse.getAndSet(0);
        asyncResponse.done(worker.throttleWaitTime(thisBatchStartTimeNS, System.nanoTime(), totalBatchSize));
    }

    /**
     * Start terminating a request that finished non-catastrophically by refreshing the modified indices and then proceeding to
     * {@link #finishHim(Exception, List, List, boolean)}.
     */
    void refreshAndFinish(List<Failure> indexingFailures, List<PaginatedSearchFailure> searchFailures, boolean timedOut) {
        if (task.isCancelled() || false == mainRequest.isRefresh() || destinationIndices.isEmpty()) {
            finishHim(null, indexingFailures, searchFailures, timedOut);
            return;
        }
        RefreshRequest refresh = new RefreshRequest();
        refresh.indices(destinationIndices.toArray(new String[destinationIndices.size()]));
        logger.debug("[{}]: refreshing", task.getId());
        bulkClient.admin().indices().refresh(refresh, new ActionListener<>() {
            @Override
            public void onResponse(BroadcastResponse response) {
                finishHim(null, indexingFailures, searchFailures, timedOut);
            }

            @Override
            public void onFailure(Exception e) {
                finishHim(e);
            }
        });
    }

    /**
     * Finish the request.
     *
     * @param failure if non null then the request failed catastrophically with this exception
     */
    protected void finishHim(Exception failure) {
        logger.debug(() -> "[" + task.getId() + "]: finishing with a catastrophic failure", failure);
        finishHim(failure, emptyList(), emptyList(), false);
    }

    /**
     * Finish the request.
     * @param failure if non null then the request failed catastrophically with this exception
     * @param indexingFailures any indexing failures accumulated during the request
     * @param searchFailures any search failures accumulated during the request
     * @param timedOut have any of the sub-requests timed out?
     */
    protected void finishHim(
        Exception failure,
        List<Failure> indexingFailures,
        List<PaginatedSearchFailure> searchFailures,
        boolean timedOut
    ) {
        logger.debug("[{}]: finishing without any catastrophic failures", task.getId());
        requestFinishing.set(true);
        // Atomically claim the current response. If prepareBulkRequest already claimed it (null), this is a no-op.
        // If we win the CAS, we release any hits that were not yet consumed (i.e. from consumedOffset to end).
        // This covers: prepareBulkRequest hasn't run yet (consumedOffset == 0) and the maxDocs partial-batch case.
        ScrollConsumableHitsResponse toRelease = currentScrollResponse.getAndSet(null);
        if (toRelease != null) {
            toRelease.releaseRemainingHits();
        }
        paginatedHitSource.close(threadPool.getThreadContext().preserveContext(() -> {
            if (failure == null) {
                BulkByScrollResponse response = buildResponse(
                    timeValueMillis(System.currentTimeMillis() - startTimeEpochMillis.get()),
                    indexingFailures,
                    searchFailures,
                    timedOut
                );
                listener.onResponse(response);
            } else {
                listener.onFailure(failure);
            }
        }));
    }

    /**
     * Get the backoff policy for use with retries.
     */
    BackoffPolicy buildBackoffPolicy() {
        return exponentialBackoff(mainRequest.getRetryBackoffInitialTime(), mainRequest.getMaxRetries());
    }

    /**
     * Add to the list of indices that were modified by this request. This is the list of indices refreshed at the end of the request if the
     * request asks for a refresh.
     */
    void addDestinationIndices(Collection<String> indices) {
        destinationIndices.addAll(indices);
    }

    /**
     * Set the last returned scrollId. Exists entirely for testing.
     */
    void setScroll(String scroll) {
        if (paginatedHitSource instanceof ScrollablePaginatedHitSource scrollable) {
            scrollable.setScrollId(scroll);
        }
    }

    /**
     * Set the search_after values for the next batch. Exists entirely for testing.
     */
    void setSearchAfterValues(Object[] searchAfterValues) {
        if (paginatedHitSource instanceof PitPaginatedHitSource pit) {
            pit.setSearchAfterValues(searchAfterValues);
        }
    }

    /**
     * Seeds {@link #currentScrollResponse} for tests that need a specific scroll ref before {@link #prepareBulkRequest} (e.g. partial-batch
     * / terminal-finish scenarios). Exists entirely for testing.
     */
    void setCurrentScrollResponseForTests(ScrollConsumableHitsResponse response) {
        currentScrollResponse.set(response);
    }

    private static void releaseHits(List<? extends PaginatedHitSource.Hit> hits) {
        for (PaginatedHitSource.Hit hit : hits) {
            hit.release();
        }
    }

    private boolean tryReleaseCurrentResponse(ScrollConsumableHitsResponse expected) {
        if (currentScrollResponse.compareAndSet(expected, null)) {
            expected.releaseRemainingHits();
            return true;
        }
        return false;
    }

    private void recordFailure(Failure failure, List<Failure> failures) {
        if (failure.getStatus() == CONFLICT) {
            worker.countVersionConflict();
            if (false == mainRequest.isAbortOnVersionConflict()) {
                return;
            }
        }
        failures.add(failure);
    }

    /**
     * Wrapper for the {@link DocWriteRequest} that are used in this action class.
     */
    public interface RequestWrapper<Self extends DocWriteRequest<Self>> {

        void setIndex(String index);

        String getIndex();

        void setId(String id);

        String getId();

        void setVersion(long version);

        long getVersion();

        void setVersionType(VersionType versionType);

        void setRouting(String routing);

        String getRouting();

        void setSource(Map<String, Object> source);

        Map<String, Object> getSource();

        Self self();
    }

    /**
     * {@link RequestWrapper} for {@link IndexRequest}
     */
    public static class IndexRequestWrapper implements RequestWrapper<IndexRequest> {

        private final IndexRequest request;

        IndexRequestWrapper(IndexRequest request) {
            this.request = Objects.requireNonNull(request, "Wrapped IndexRequest can not be null");
        }

        @Override
        public void setIndex(String index) {
            request.index(index);
        }

        @Override
        public String getIndex() {
            return request.index();
        }

        @Override
        public void setId(String id) {
            request.id(id);
        }

        @Override
        public String getId() {
            return request.id();
        }

        @Override
        public void setVersion(long version) {
            request.version(version);
        }

        @Override
        public long getVersion() {
            return request.version();
        }

        @Override
        public void setVersionType(VersionType versionType) {
            request.versionType(versionType);
        }

        @Override
        public void setRouting(String routing) {
            request.routing(routing);
        }

        @Override
        public String getRouting() {
            return request.routing();
        }

        @Override
        public Map<String, Object> getSource() {
            return request.sourceAsMap();
        }

        @Override
        public void setSource(Map<String, Object> source) {
            request.source(source);
        }

        @Override
        public IndexRequest self() {
            return request;
        }
    }

    /**
     * Wraps a {@link IndexRequest} in a {@link RequestWrapper}
     */
    public static RequestWrapper<IndexRequest> wrap(IndexRequest request) {
        return new IndexRequestWrapper(request);
    }

    /**
     * {@link RequestWrapper} for {@link DeleteRequest}
     */
    public static class DeleteRequestWrapper implements RequestWrapper<DeleteRequest> {

        private final DeleteRequest request;

        DeleteRequestWrapper(DeleteRequest request) {
            this.request = Objects.requireNonNull(request, "Wrapped DeleteRequest can not be null");
        }

        @Override
        public void setIndex(String index) {
            request.index(index);
        }

        @Override
        public String getIndex() {
            return request.index();
        }

        @Override
        public void setId(String id) {
            request.id(id);
        }

        @Override
        public String getId() {
            return request.id();
        }

        @Override
        public void setVersion(long version) {
            request.version(version);
        }

        @Override
        public long getVersion() {
            return request.version();
        }

        @Override
        public void setVersionType(VersionType versionType) {
            request.versionType(versionType);
        }

        @Override
        public void setRouting(String routing) {
            request.routing(routing);
        }

        @Override
        public String getRouting() {
            return request.routing();
        }

        @Override
        public Map<String, Object> getSource() {
            throw new UnsupportedOperationException("unable to get source from action request [" + request.getClass() + "]");
        }

        @Override
        public void setSource(Map<String, Object> source) {
            throw new UnsupportedOperationException("unable to set [source] on action request [" + request.getClass() + "]");
        }

        @Override
        public DeleteRequest self() {
            return request;
        }
    }

    /**
     * Wraps a {@link DeleteRequest} in a {@link RequestWrapper}
     */
    public static RequestWrapper<DeleteRequest> wrap(DeleteRequest request) {
        return new DeleteRequestWrapper(request);
    }

    /**
     * Apply a {@link Script} to a {@link RequestWrapper}
     */
    public abstract static class ScriptApplier<T extends Metadata>
        implements
            BiFunction<RequestWrapper<?>, PaginatedHitSource.Hit, RequestWrapper<?>> {

        // "index" is the default operation
        protected static final String INDEX = "index";

        private final WorkerBulkByScrollTaskState taskWorker;
        protected final ScriptService scriptService;
        protected final Script script;
        protected final Map<String, Object> params;
        protected final LongSupplier nowInMillisSupplier;

        public ScriptApplier(
            WorkerBulkByScrollTaskState taskWorker,
            ScriptService scriptService,
            Script script,
            Map<String, Object> params,
            LongSupplier nowInMillisSupplier
        ) {
            this.taskWorker = taskWorker;
            this.scriptService = scriptService;
            this.script = script;
            this.params = params;
            this.nowInMillisSupplier = nowInMillisSupplier;
        }

        @Override
        public RequestWrapper<?> apply(RequestWrapper<?> request, PaginatedHitSource.Hit doc) {
            if (script == null) {
                return request;
            }

            CtxMap<T> ctxMap = execute(doc, request.getSource());

            T metadata = ctxMap.getMetadata();

            request.setSource(ctxMap.getSource());

            updateRequest(request, metadata);

            return requestFromOp(request, metadata.getOp());
        }

        protected abstract CtxMap<T> execute(PaginatedHitSource.Hit doc, Map<String, Object> source);

        protected abstract void updateRequest(RequestWrapper<?> request, T metadata);

        protected RequestWrapper<?> requestFromOp(RequestWrapper<?> request, String op) {
            switch (op) {
                case "noop" -> {
                    taskWorker.countNoop();
                    return null;
                }
                case "delete" -> {
                    RequestWrapper<DeleteRequest> delete = wrap(new DeleteRequest(request.getIndex(), request.getId()));
                    delete.setVersion(request.getVersion());
                    delete.setVersionType(VersionType.INTERNAL);
                    delete.setRouting(request.getRouting());
                    return delete;
                }
                case INDEX -> {
                    return request;
                }
                default -> throw new IllegalArgumentException("Unsupported operation type change from [" + INDEX + "] to [" + op + "]");
            }
        }
    }

    static class ScrollConsumableHitsResponse {
        private final PaginatedHitSource.AsyncResponse asyncResponse;
        private final List<? extends PaginatedHitSource.Hit> hits;
        /**
         * Volatile for cross-thread visibility when {@link #remainingHits()} / {@link #hasRemainingHits()}
         * are read while another thread updates this field. {@link #consumeHits} uses a non-atomic
         * read-then-add on this value; {@code volatile} does not make that sequence atomic—callers rely
         * on at most one thread executing {@code consumeHits} per response, coordinated by
         * {@link AbstractAsyncBulkByScrollAction#currentScrollResponse} CAS, not on this field alone.
         */
        private volatile int consumedOffset = 0;
        private final Releasable releaseRemainingHitsOnce;

        ScrollConsumableHitsResponse(PaginatedHitSource.AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
            this.hits = asyncResponse.response().getHits();
            this.releaseRemainingHitsOnce = Releasables.releaseOnce(this::doReleaseRemainingHits);
        }

        PaginatedHitSource.Response response() {
            return asyncResponse.response();
        }

        List<? extends PaginatedHitSource.Hit> consumeRemainingHits() {
            return consumeHits(remainingHits());
        }

        List<? extends PaginatedHitSource.Hit> consumeHits(int numberOfHits) {
            if (numberOfHits < 0) {
                throw new IllegalArgumentException("Invalid number of hits to consume [" + numberOfHits + "]");
            }

            if (numberOfHits > remainingHits()) {
                throw new IllegalArgumentException(
                    "Unable to provide [" + numberOfHits + "] hits as there are only [" + remainingHits() + "] hits available"
                );
            }

            int start = consumedOffset;
            consumedOffset += numberOfHits;
            return hits.subList(start, consumedOffset);
        }

        boolean hasRemainingHits() {
            return remainingHits() > 0;
        }

        int remainingHits() {
            return hits.size() - consumedOffset;
        }

        /**
         * Release only the unconsumed hits (from consumedOffset to end). Safe to call from multiple
         * threads concurrently — only the first call releases; subsequent calls are no-ops. This
         * prevents double-release in the window where both prepareBulkRequest (via the requestFinishing
         * path) and finishHim can hold a reference to this response simultaneously.
         */
        void releaseRemainingHits() {
            releaseRemainingHitsOnce.close();
        }

        private void doReleaseRemainingHits() {
            for (; consumedOffset < hits.size(); consumedOffset++) {
                hits.get(consumedOffset).release();
            }
        }

        void done(TimeValue extraKeepAlive) {
            asyncResponse.done(extraKeepAlive);
        }
    }
}
