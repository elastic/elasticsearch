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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.reindex.AbstractBulkByPaginatedSearchRequest;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.PaginatedSearchFailure;
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResumeInfo;
import org.elasticsearch.index.reindex.WorkerBulkByPaginatedSearchTaskState;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.reindex.remote.RemotePitPaginatedHitSource;
import org.elasticsearch.reindex.remote.RemoteScrollablePaginatedHitSource;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.Metadata;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.common.BackoffPolicy.exponentialBackoff;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.index.reindex.AbstractBulkByPaginatedSearchRequest.MAX_DOCS_ALL_MATCHES;
import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

/**
 * Abstract base for paginating across a search and executing bulk actions on all results. All package private methods are package private
 * so their tests can use them. Most methods run in the listener thread pool because they are meant to be fast and don't expect to block.
 * <p>
 * Before each search the worker reserves one coordinating indexing pressure charge for the page it is about to hold. A page that does not
 * fit waits, with no hits held, until the reservation succeeds or the task is cancelled. When the bulk is built that same charge becomes
 * the bulk's real size and stays with the page until the page is released, including while a rejected bulk is retried.
 */
public abstract class AbstractAsyncBulkByPaginatedSearchAction<
    Request extends AbstractBulkByPaginatedSearchRequest<Request>,
    Action extends TransportAction<Request, ?>> {

    protected final Logger logger;
    protected final BulkByPaginatedSearchTask task;
    protected final WorkerBulkByPaginatedSearchTaskState worker;
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
    private final ActionListener<BulkByPaginatedSearchResponse> listener;
    private final Retry bulkRetry;
    private final PaginatedHitSource paginatedHitSource;

    /**
     * This BiFunction is used to apply various changes depending of the Reindex action and  the search hit,
     * from copying search hit metadata (parent, routing, etc) to potentially transforming the
     * {@link RequestWrapper} completely.
     */
    private final BiFunction<RequestWrapper<?>, PaginatedHitSource.Hit, RequestWrapper<?>> scriptApplier;

    /**
     * Hits the script already released while building the current bulk.
     * Cleanup walks those same hits and must leave them alone.
     * <p>
     * This set is not synchronised. That is safe because every add and every remove is ordered by an executor handoff, and
     * {@code ClientHit#release} is idempotent, so even a missed entry would only release the same hit a second time.
     */
    private final Set<PaginatedHitSource.Hit> hitsReleasedWhileBuilding = Collections.newSetFromMap(new IdentityHashMap<>());

    private int lastBatchSize;
    /**
     * The current paginated search response being processed. Set atomically so that either {@link #prepareBulkRequest} or
     * {@link #finishHim(Exception, List, List, boolean)} can claim exclusive ownership of the remaining hits and release them exactly once.
     */
    private final AtomicReference<PaginatedSearchConsumableHitsResponse> currentPaginatedSearchResponse = new AtomicReference<>();
    /**
     * Set to {@code true} at the start of {@link #finishHim(Exception, List, List, boolean)} so {@link #prepareBulkRequest} can still
     * release unconsumed hits when {@link #currentPaginatedSearchResponse} is temporarily {@code null} after prepare's CAS
     * (before the ref is restored when {@code maxDocs} leaves a partial batch).
     */
    private final AtomicBoolean requestFinishing = new AtomicBoolean(false);
    /**
     * Keeps track of the total number of bulk operations performed
     * from a single paginated search response. It is possible that
     * multiple bulk requests are performed from a single paginated search
     * response, meaning that we have to take into account the total
     * in order to compute a correct search context keep-alive time.
     */
    private final AtomicInteger totalBatchSizeInSinglePaginatedSearchResponse = new AtomicInteger();
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

    protected final SearchContextKeepaliveDeadline searchContextKeepaliveDeadline;

    @Nullable
    private final BulkByPaginatedSearchSearchContextMetrics bulkByPaginatedSearchSearchContextMetrics;
    private final BulkByPaginatedSearchSearchContextMetrics.TaskKind bulkByPaginatedSearchTaskKind;
    private final boolean remoteBulkByPaginatedSearch;

    /**
     * {@code _shard_doc} for search_after compatibility and performance (see paginate-search-results docs) was added in 7.12
     */
    private static final Version REMOTE_SHARD_DOC_SUPPORTED = Version.V_7_12_0;

    protected final ReindexSettings reindexSettings;
    private final CircuitBreaker circuitBreaker;
    private final String breakerLabel;
    /**
     * Null when this action was not given the node's indexing pressure. The bulk then stays unaccounted, and bulk execution charges the
     * node's budget for that attempt only.
     */
    @Nullable
    private final IndexingPressure indexingPressure;

    /**
     * Null in tests that do not set it up, and then no bulk counts as system-only. A system-only page may grow past the coordinating
     * limit, as a system-only bulk was never refused at the coordinating stage.
     */
    @Nullable
    private final SystemIndices systemIndices;

    /**
     * The indices lookup the system-only check reads, taken when the bulk is built so it sees the current cluster state. Unused when
     * {@link #systemIndices} is null.
     */
    @Nullable
    private final Supplier<SortedMap<String, IndexAbstraction>> indicesLookup;

    /**
     * The shortest wait between reservation attempts, so a request that sets a zero initial retry backoff cannot spin on a full budget.
     */
    private static final TimeValue MIN_RESERVATION_WAIT = timeValueMillis(50);

    /**
     * How long a reserve that is not capped is refused before the worker says so in the log.
     */
    private static final TimeValue RESERVATION_WAIT_WARN_AFTER = TimeValue.timeValueSeconds(60);

    private static final long NOT_REFUSED = -1;

    /**
     * When the current run of refused reserves began, in {@link ThreadPool#relativeTimeInMillis()}, or {@link #NOT_REFUSED}. Only the
     * worker chain writes this, one attempt at a time, and each attempt is handed to the next through the scheduler.
     */
    private volatile long reserveRefusedSinceMillis = NOT_REFUSED;

    /**
     * Set once the worker has logged a long wait, so a task that waits for a long time logs it once and not on every poll.
     */
    private volatile boolean loggedLongReserveWait;

    /**
     * The estimate reserved for the search that is in flight. Moved onto the page when that search returns, and closed here when the
     * search fails before a page exists. One worker holds this or the page charge, not both.
     */
    private final AtomicReference<IndexingPressure.Coordinating> inFlightCharge = new AtomicReference<>();

    /**
     * The one delayed reservation attempt waiting for coordinating budget. Cancelled when the task is cancelled.
     */
    private final AtomicReference<ReservationRetry> reservationRetry = new AtomicReference<>();

    private final AtomicBoolean listeningForCancellation = new AtomicBoolean();

    protected CircuitBreaker getCircuitBreaker() {
        return circuitBreaker;
    }

    AbstractAsyncBulkByPaginatedSearchAction(
        BulkByPaginatedSearchTask task,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm,
        boolean needsVectors,
        Logger logger,
        ParentTaskAssigningClient client,
        ThreadPool threadPool,
        Request mainRequest,
        ActionListener<BulkByPaginatedSearchResponse> listener,
        @Nullable ScriptService scriptService,
        @Nullable ReindexSslConfig sslConfig,
        @Nullable BulkByPaginatedSearchSearchContextMetrics bulkByPaginatedSearchSearchContextMetrics,
        BulkByPaginatedSearchSearchContextMetrics.TaskKind bulkByPaginatedSearchTaskKind,
        boolean remoteBulkByPaginatedSearch,
        TimeValue maxTaskShutdownGracePeriod,
        ReindexSettings reindexSettings,
        CircuitBreaker circuitBreaker,
        String breakerLabel,
        @Nullable IndexingPressure indexingPressure,
        @Nullable SystemIndices systemIndices,
        @Nullable Supplier<SortedMap<String, IndexAbstraction>> indicesLookup
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
            bulkByPaginatedSearchSearchContextMetrics,
            bulkByPaginatedSearchTaskKind,
            remoteBulkByPaginatedSearch,
            maxTaskShutdownGracePeriod,
            reindexSettings,
            circuitBreaker,
            breakerLabel,
            indexingPressure,
            systemIndices,
            indicesLookup
        );
    }

    AbstractAsyncBulkByPaginatedSearchAction(
        BulkByPaginatedSearchTask task,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm,
        boolean needsVectors,
        Logger logger,
        ParentTaskAssigningClient searchClient,
        ParentTaskAssigningClient bulkClient,
        ThreadPool threadPool,
        Request mainRequest,
        ActionListener<BulkByPaginatedSearchResponse> listener,
        @Nullable ScriptService scriptService,
        @Nullable ReindexSslConfig sslConfig,
        @Nullable Version remoteVersion,
        @Nullable BulkByPaginatedSearchSearchContextMetrics bulkByPaginatedSearchSearchContextMetrics,
        BulkByPaginatedSearchSearchContextMetrics.TaskKind bulkByPaginatedSearchTaskKind,
        boolean remoteBulkByPaginatedSearch,
        TimeValue maxTaskShutdownGracePeriod,
        ReindexSettings reindexSettings,
        CircuitBreaker circuitBreaker,
        String breakerLabel,
        @Nullable IndexingPressure indexingPressure,
        @Nullable SystemIndices systemIndices,
        @Nullable Supplier<SortedMap<String, IndexAbstraction>> indicesLookup
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
        this.searchContextKeepaliveDeadline = new SearchContextKeepaliveDeadline(threadPool::absoluteTimeInMillis);
        this.bulkByPaginatedSearchSearchContextMetrics = bulkByPaginatedSearchSearchContextMetrics;
        this.bulkByPaginatedSearchTaskKind = bulkByPaginatedSearchTaskKind;
        this.remoteBulkByPaginatedSearch = remoteBulkByPaginatedSearch;
        this.relocationCooldownNanos = computeRelocationCooldownNanos(maxTaskShutdownGracePeriod);
        this.listener = listener;
        BackoffPolicy backoffPolicy = buildBackoffPolicy();
        bulkRetry = new Retry(BackoffPolicy.wrap(backoffPolicy, worker::countBulkRetry), threadPool);
        this.remoteVersion = remoteVersion;
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker);
        this.breakerLabel = Objects.requireNonNull(breakerLabel);
        this.reindexSettings = Objects.requireNonNull(reindexSettings);
        this.indexingPressure = indexingPressure;
        this.systemIndices = systemIndices;
        this.indicesLookup = indicesLookup;
        paginatedHitSource = buildPaginatedSearchResultSource(
            backoffPolicy,
            prepareSearchRequest(
                mainRequest,
                needsSourceDocumentVersions,
                needsSourceDocumentSeqNoAndPrimaryTerm,
                needsVectors,
                remoteVersion
            )
        );
        scriptApplier = Objects.requireNonNull(buildScriptApplier(), "script applier must not be null");
        if (scriptApplier instanceof ScriptApplier<?> applier) {
            applier.releaseConsumedHitWith(this::releaseHitForThisBulk);
        }
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
     *
     * @param remoteVersion when reindexing from remote, the remote cluster version. {@code null} when searching the local cluster.
     */
    // Visible for testing
    static <Request extends AbstractBulkByPaginatedSearchRequest<Request>> SearchRequest prepareSearchRequest(
        Request mainRequest,
        boolean needsSourceDocumentVersions,
        boolean needsSourceDocumentSeqNoAndPrimaryTerm,
        boolean needsVectors,
        @Nullable Version remoteVersion
    ) {
        var preparedSearchRequest = new SearchRequest(mainRequest.getSearchRequest());

        /*
         * Default to sorting by doc. We can't do this in the request itself because it is normal to *add* to the sorts rather than replace
         * them and if we add _doc as the first sort by default then sorts will never work.... So we add it here, only if there isn't
         * another sort.
         *
         * When using PIT, use _shard_doc for search_after compatibility and performance (see paginate-search-results docs), except on
         * remote clusters before 7.12.0 which do not support _shard_doc. For remote requests before 7.12 and on scroll requests, we
         * default to _doc.
         *
         * This modifies the original request!
         */
        final SearchSourceBuilder sourceBuilder = preparedSearchRequest.source();
        List<SortBuilder<?>> sorts = sourceBuilder.sorts();
        if (sorts == null || sorts.isEmpty()) {
            if (sourceBuilder.pointInTimeBuilder() != null) {
                if (remoteVersion != null && remoteVersion.before(REMOTE_SHARD_DOC_SUPPORTED)) {
                    sourceBuilder.sort(fieldSort(FieldSortBuilder.DOC_FIELD_NAME));
                } else {
                    sourceBuilder.sort(fieldSort(FieldSortBuilder.SHARD_DOC_FIELD_NAME));
                }
            } else {
                sourceBuilder.sort(fieldSort("_doc"));
            }
        }
        sourceBuilder.version(needsSourceDocumentVersions);
        sourceBuilder.seqNoAndPrimaryTerm(needsSourceDocumentSeqNoAndPrimaryTerm);

        // Force accurate track_total_hits for PIT reindex so Status#total reflects the actual number of matching
        // documents instead of being clamped at the search default. Scroll doesn't need this, it already forces
        // accurate counting
        if (sourceBuilder.pointInTimeBuilder() != null && sourceBuilder.trackTotalHitsUpTo() == null) {
            sourceBuilder.trackTotalHits(true);
        }

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
        request.setRoutingFromSlice(mainRequest.getSearchRequest().isRoutingFromSlice());
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

    protected BulkRequest buildBulk(Iterable<? extends PaginatedHitSource.Hit> docs, AtomicLong reservedBytes) {
        BulkRequest bulkRequest = new BulkRequest();
        for (PaginatedHitSource.Hit doc : docs) {
            if (accept(doc)) {
                RequestWrapper<?> request = scriptApplier.apply(copyMetadata(buildRequest(doc), doc), doc);
                if (request != null) {
                    bulkRequest.add(request.self());
                    long delta = bulkRequest.estimatedSizeInBytes() - reservedBytes.get();
                    if (delta >= reindexSettings.getMemoryAccountingThresholdInBytes()) {
                        circuitBreaker.addEstimateBytesAndMaybeBreak(delta, breakerLabel);
                        reservedBytes.addAndGet(delta);
                    }
                }
            }
        }
        // Reserve any bytes not yet covered by threshold checks above
        long remaining = bulkRequest.estimatedSizeInBytes() - reservedBytes.get();
        if (remaining > 0) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(remaining, breakerLabel);
            reservedBytes.addAndGet(remaining);
        }
        return bulkRequest;
    }

    protected PaginatedHitSource buildPaginatedSearchResultSource(BackoffPolicy backoffPolicy, SearchRequest searchRequest) {
        // If we're using point-in-time search, then return a ClientPitPaginatedHitSource
        if (searchRequest.source() != null && searchRequest.source().pointInTimeBuilder() != null) {
            return new ClientPitPaginatedHitSource(
                logger,
                backoffPolicy,
                threadPool,
                worker::countSearchRetry,
                this::onPaginatedSearchResponse,
                this::finishHim,
                searchClient,
                searchRequest,
                searchContextKeepaliveDeadline
            );
        }
        // Default to scroll
        return new ClientScrollablePaginatedHitSource(
            logger,
            backoffPolicy,
            threadPool,
            worker::countSearchRetry,
            this::onPaginatedSearchResponse,
            this::finishHim,
            searchClient,
            searchRequest,
            searchContextKeepaliveDeadline
        );
    }

    /**
     * Build the response for reindex actions.
     */
    protected BulkByPaginatedSearchResponse buildResponse(
        TimeValue took,
        List<BulkItemResponse.Failure> indexingFailures,
        List<PaginatedSearchFailure> searchFailures,
        boolean timedOut
    ) {
        BytesReference pitId = paginatedHitSource instanceof PitPaginatedHitSource pit ? pit.getPitId() : null;
        return new BulkByPaginatedSearchResponse(took, task.getStatus(), indexingFailures, searchFailures, timedOut, null, pitId);
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
                reservePageThen(() -> paginatedHitSource.resume(workerResumeInfo));
            } else {
                startTimeEpochMillis.set(System.currentTimeMillis());
                reservePageThen(() -> paginatedHitSource.start());
            }
        } catch (Exception e) {
            finishHim(e);
        }
    }

    void onPaginatedSearchResponse(PaginatedHitSource.AsyncResponse asyncResponse) {
        // The search listener can run before start or done returns, so the charge has to be on the page before any of this response is
        // read. The previous page's charge is already closed. This worker holds one reservation.
        PaginatedSearchConsumableHitsResponse page = new PaginatedSearchConsumableHitsResponse(asyncResponse);
        page.attachCoordinatingCharge(inFlightCharge.getAndSet(null));
        onPaginatedSearchResponse(page);
    }

    void onPaginatedSearchResponse(PaginatedSearchConsumableHitsResponse asyncResponse) {
        // TODO - https://github.com/elastic/elasticsearch/issues/150875
        // lastBatchStartTime is essentially unused (see WorkerBulkByPaginatedSearchTaskState.throttleWaitTime).
        // Leaving it for now, since it seems like a bug?
        onPaginatedSearchResponse(System.nanoTime(), this.lastBatchSize, asyncResponse);
    }

    /**
     * Process a paginated search response.
     * @param lastBatchStartTimeNS the time when the last batch started. Used to calculate the throttling delay.
     * @param lastBatchSizeToUse the size of the last batch. Used to calculate the throttling delay.
     * @param asyncResponse the response to process from {@link PaginatedHitSource}
     */
    void onPaginatedSearchResponse(long lastBatchStartTimeNS, int lastBatchSizeToUse, PaginatedSearchConsumableHitsResponse asyncResponse) {
        currentPaginatedSearchResponse.set(asyncResponse);
        PaginatedHitSource.Response response = asyncResponse.response();
        logger.debug("[{}]: got paginated search response with [{}] hits", task.getId(), asyncResponse.remainingHits());
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
                 * It is important that the batch start time be calculated from here, paginated search response to paginated search
                 * response. That way the time waiting on the search doesn't count against this batch in the throttle.
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
     * <p>
     * The coordinating charge was taken before this page was searched. This method resizes it once to the real page, which is the bulk just
     * built, or the whole page when {@code max_docs} leaves hits behind. A page whose own size is above the coordinating limit fails the
     * task, unless the bulk targets only system indices. The charge stays on the page across bulk retries and across {@code max_docs} sub
     * bulks, and the cleanup that already wraps the bulk listener releases it when the page has nothing left.
     */
    void prepareBulkRequest(long thisBatchStartTimeNS, PaginatedSearchConsumableHitsResponse asyncResponse) {
        logger.debug("[{}]: preparing bulk request", task.getId());
        // Atomically claim ownership of the response. If finishHim already claimed it (CAS returns false),
        // the hits have already been released — nothing to do here.
        if (currentPaginatedSearchResponse.compareAndSet(asyncResponse, null) == false) {
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
            asyncResponse.releaseRemainingHits();
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
            currentPaginatedSearchResponse.set(asyncResponse);
        }

        if (requestFinishing.get()) {
            releaseHits(hits);
            asyncResponse.releaseRemainingHits();
            return;
        }

        // Reserve heap budget against the REQUEST breaker incrementally as docs are added in buildBulk,
        // so oversized batches are rejected before all their source bytes land in memory. The reservation
        // is released (via cleanup) when the bulk listener completes or if we return early for any reason.
        final AtomicLong reservedBytes = new AtomicLong(0);
        final Releasable releaseBatchHits = Releasables.releaseOnce(() -> releaseHits(hits));
        final Releasable releaseBreakerAndBody = Releasables.releaseOnce(Releasables.wrap(releaseBatchHits, () -> {
            long r = reservedBytes.getAndSet(0);
            if (r > 0) circuitBreaker.addWithoutBreaking(-r);
            asyncResponse.response().closeBodyReleasable();
        }));
        boolean cleanupHandedOff = false;
        boolean continuing = false;
        try {
            final BulkRequest request;
            try {
                request = buildBulk(hits, reservedBytes);
            } catch (CircuitBreakingException e) {
                // releaseBatchHits is once, so the finally block does not release the same hit again.
                releaseBatchHits.close();
                asyncResponse.releaseRemainingHits();
                finishHim(e);
                return;
            }
            if (request.requests().isEmpty()) {
                /*
                 * If we noop-ed the entire batch then just skip to the next batch or the BulkRequest would fail validation.
                 * The page charge has to be gone before notifyDone reserves the next search.
                 */
                releaseBatchHits.close();
                asyncResponse.releaseChargeWhenNothingRemains();
                notifyDone(thisBatchStartTimeNS, asyncResponse, 0);
                continuing = true;
                return;
            }
            try {
                resizePageCharge(asyncResponse, request);
            } catch (EsRejectedExecutionException e) {
                // The page is already in memory and it cannot be admitted. Drop it and fail. Do not send, and do not retry.
                releaseBatchHits.close();
                asyncResponse.releaseRemainingHits();
                finishHim(e);
                return;
            }
            if (mainRequest instanceof DeleteByQueryRequest) {
                // The delete request keeps the id, not the source. Drop the source before a rejected bulk is retried.
                releaseBatchHits.close();
            }
            request.timeout(mainRequest.getTimeout());
            request.waitForActiveShards(mainRequest.getWaitForActiveShards());
            if (asyncResponse.holdsCoordinatingCharge()) {
                // doExecute skips its own coordinating charge when the bulk says this page already holds one.
                // A partial retry copies the flag and does not change the number held here.
                request.indexingPressureAccounted(true);
            }
            // The listener releases this before onBulkResponse, so a page with nothing left drops its charge before the next search.
            final Releasable cleanup = Releasables.releaseOnce(
                Releasables.wrap(releaseBreakerAndBody, asyncResponse::releaseChargeWhenNothingRemains)
            );
            sendBulkRequest(request, cleanup, () -> notifyDone(thisBatchStartTimeNS, asyncResponse, request.requests().size()));
            cleanupHandedOff = true;
        } finally {
            if (cleanupHandedOff == false) {
                releaseBreakerAndBody.close();
                // A path that notified the next batch left the charge with whatever hits that batch still holds.
                // Every other exit drops the page, including an exception from building the bulk.
                if (continuing == false) {
                    asyncResponse.releaseRemainingHits();
                }
            }
        }
    }

    /**
     * Send a bulk request, handling retries. Releases {@code releaseOnCompletion} on cancellation, terminal finish, and before the bulk
     * listener completes, on success or failure.
     */
    void sendBulkRequest(BulkRequest request, Releasable releaseOnCompletion, Runnable onSuccess) {
        final int requestSize = request.requests().size();
        boolean handedOff = false;
        try {
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
                releaseOnCompletion.close();
                finishHim(null);
                handedOff = true;
                return;
            }
            if (requestFinishing.get()) {
                releaseOnCompletion.close();
                handedOff = true;
                return;
            }
            bulkRetry.withBackoff(
                this::sendBulkUnlessCancelled,
                request,
                ActionListener.releaseBefore(releaseOnCompletion, ActionListener.wrap(response -> {
                    logger.debug("[{}]: completed [{}] entry bulk request", task.getId(), requestSize);
                    onBulkResponse(response, onSuccess);
                }, this::finishHim))
            );
            handedOff = true;
        } finally {
            if (handedOff == false) {
                releaseOnCompletion.close();
            }
        }
    }

    /**
     * Sends one bulk attempt. When the task has been cancelled since the previous attempt, sends nothing and completes the listener so the
     * task can finish cancelled. {@link Retry} calls this for the first send and for every attempt it has scheduled.
     * <p>
     * The page already holds the coordinating charge, so this send does not take another one.
     *
     * @param bulkRequest the bulk for this attempt
     * @param listener completed when the attempt finishes
     */
    private void sendBulkUnlessCancelled(BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        if (task.isCancelled()) {
            logger.debug("[{}]: not sending bulk because the task was cancelled", task.getId());
            // A non-429 failure would fail the task. Cancellation finishes with the cancel reason, so this attempt reports nothing.
            listener.onResponse(new BulkResponse(new BulkItemResponse[0], 0L));
            return;
        }
        bulkClient.bulk(bulkRequest, listener);
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
                // Index contains fewer matching docs than max_docs (found < max_docs <= batch size)
                refreshAndFinish(emptyList(), emptyList(), false);
                return;
            }

            onSuccess.run();
        } catch (Exception t) {
            finishHim(t);
        }
    }

    void notifyDone(long thisBatchStartTimeNS, PaginatedSearchConsumableHitsResponse asyncResponse, int batchSize) {
        if (task.isCancelled()) {
            logger.debug("[{}]: finishing early because the task was cancelled", task.getId());
            finishHim(null);
            return;
        }
        this.lastBatchSize = batchSize;
        this.totalBatchSizeInSinglePaginatedSearchResponse.addAndGet(batchSize);

        if (asyncResponse.hasRemainingHits()) {
            // NB this means the next bulk task will be traced as a child of the current one, but it should really be a sibling
            onPaginatedSearchResponse(asyncResponse);
            return;
        }
        if (task.isRelocationRequested()) {
            final boolean tooYoungToRelocateAgain = task.isRelocatedTask()
                && (System.nanoTime() - task.getStartTimeNanos()) < relocationCooldownNanos;
            if (tooYoungToRelocateAgain) {
                logger.debug("skipping re-relocation for recently-relocated task [{}]", task.getId());
            } else {
                if (worker.hasNodeToRelocateTo()) {
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
                    final BulkByPaginatedSearchResponse response = new BulkByPaginatedSearchResponse(
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

        int totalBatchSize = totalBatchSizeInSinglePaginatedSearchResponse.getAndSet(0);
        // The previous page released its charge before this method reserved another. One worker, one reservation.
        TimeValue extraKeepAlive = worker.throttleWaitTime(thisBatchStartTimeNS, System.nanoTime(), totalBatchSize);
        // The wait below holds no page bytes, but the lambda keeps this response alive. Every hit is already released, so drop the
        // list now or the finished page stays reachable and uncharged until the reserve succeeds.
        asyncResponse.dropHits();
        reservePageThen(() -> asyncResponse.done(extraKeepAlive));
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
     * When pagination used PIT and the inferred keep-alive window has passed, {@link SearchContextMissingException} failures are mapped to
     * HTTP 500; otherwise their default ({@link RestStatus#NOT_FOUND}) is preserved for BWC.
     */
    static List<PaginatedSearchFailure> remapSearchFailures(
        boolean pitPagination,
        boolean pastKeepaliveDeadline,
        List<PaginatedSearchFailure> searchFailures
    ) {
        if (pitPagination == false || pastKeepaliveDeadline == false || searchFailures.isEmpty()) {
            return searchFailures;
        }
        boolean anyScm = false;
        for (PaginatedSearchFailure f : searchFailures) {
            if (ExceptionsHelper.unwrap(f.getReason(), SearchContextMissingException.class) != null) {
                anyScm = true;
                break;
            }
        }
        if (anyScm == false) {
            return searchFailures;
        }
        List<PaginatedSearchFailure> remapped = new ArrayList<>(searchFailures.size());
        for (PaginatedSearchFailure f : searchFailures) {
            if (ExceptionsHelper.unwrap(f.getReason(), SearchContextMissingException.class) != null) {
                remapped.add(
                    new PaginatedSearchFailure(f.getReason(), f.getIndex(), f.getShardId(), f.getNodeId(), RestStatus.INTERNAL_SERVER_ERROR)
                );
            } else {
                remapped.add(f);
            }
        }
        return remapped;
    }

    /**
     * Same policy as {@link #remapSearchFailures} for catastrophic failure on {@link ActionListener#onFailure}.
     */
    static Exception maybeWrapCatastrophicFailure(boolean pitPagination, boolean pastKeepaliveDeadline, Exception failure) {
        if (failure == null || pitPagination == false || pastKeepaliveDeadline == false) {
            return failure;
        }
        if (ExceptionsHelper.unwrap(failure, SearchContextMissingException.class) == null) {
            return failure;
        }
        Throwable root = ExceptionsHelper.unwrapCause(failure);
        String message = root.getMessage();
        return new ElasticsearchStatusException(
            message != null ? message : "search context missing",
            RestStatus.INTERNAL_SERVER_ERROR,
            failure
        );
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
        if (bulkByPaginatedSearchSearchContextMetrics != null
            && searchContextKeepaliveDeadline.shouldRecordKeepaliveExpiry(failure, searchFailures)) {
            bulkByPaginatedSearchSearchContextMetrics.recordKeepaliveExpiry(bulkByPaginatedSearchTaskKind, remoteBulkByPaginatedSearch);
            logger.warn(
                "[{}]: bulk-by-paginated-search [{}] ({}) likely failed because the {} keep-alive expired",
                task.getId(),
                bulkByPaginatedSearchTaskKind.attributeValue(),
                remoteBulkByPaginatedSearch
                    ? BulkByPaginatedSearchSearchContextMetrics.ATTRIBUTE_VALUE_SEARCH_SOURCE_REMOTE
                    : BulkByPaginatedSearchSearchContextMetrics.ATTRIBUTE_VALUE_SEARCH_SOURCE_LOCAL,
                paginatedHitSource instanceof PitPaginatedHitSource ? "point-in-time" : "scroll"
            );
        }
        final boolean pitPagination = paginatedHitSource instanceof PitPaginatedHitSource;
        final boolean pastKeepaliveDeadline = searchContextKeepaliveDeadline.isPastKeepaliveDeadline();
        final List<PaginatedSearchFailure> resolvedSearchFailures = remapSearchFailures(
            pitPagination,
            pastKeepaliveDeadline,
            searchFailures
        );
        final Exception resolvedFailure = maybeWrapCatastrophicFailure(pitPagination, pastKeepaliveDeadline, failure);
        requestFinishing.set(true);
        // A search that failed before a page existed still holds its estimate. A page that exists closes its own charge below.
        ReservationRetry scheduledReservation = reservationRetry.getAndSet(null);
        if (scheduledReservation != null) {
            scheduledReservation.cancel();
        }
        Releasables.close(inFlightCharge.getAndSet(null));
        // Atomically claim the current response. If prepareBulkRequest already claimed it (null), this is a no-op.
        // If we win the CAS, we release any hits that were not yet consumed (i.e. from consumedOffset to end).
        // This covers: prepareBulkRequest hasn't run yet (consumedOffset == 0) and the maxDocs partial-batch case.
        PaginatedSearchConsumableHitsResponse toRelease = currentPaginatedSearchResponse.getAndSet(null);
        if (toRelease != null) {
            toRelease.releaseRemainingHits();
        }
        paginatedHitSource.close(threadPool.getThreadContext().preserveContext(() -> {
            if (resolvedFailure == null) {
                BulkByPaginatedSearchResponse response = buildResponse(
                    timeValueMillis(System.currentTimeMillis() - startTimeEpochMillis.get()),
                    indexingFailures,
                    resolvedSearchFailures,
                    timedOut
                );
                listener.onResponse(response);
            } else {
                listener.onFailure(resolvedFailure);
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
     * Occupies the reservation-retry slot. The next schedule finds it taken and fails the task. Exists entirely for testing.
     */
    void plantReservationRetryForTests() {
        if (reservationRetry.compareAndSet(null, new ReservationRetry()) == false) {
            throw new AssertionError("a reservation retry is already planted");
        }
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
     * Seeds {@link #currentPaginatedSearchResponse} for tests that need a specific response ref before {@link #prepareBulkRequest}
     * (e.g. partial-batch / terminal-finish scenarios). Exists entirely for testing.
     */
    void setCurrentPaginatedSearchResponseForTests(PaginatedSearchConsumableHitsResponse response) {
        currentPaginatedSearchResponse.set(response);
    }

    /**
     * Reserves the page estimate, then runs {@code search}.
     * When the estimate does not fit, schedules one retry and leaves the task running with no page held. The retry does not consume
     * {@code max_retries}. Any other failure fails the task. The wait is scheduled on the generic pool, so this method does not block.
     */
    private void reservePageThen(Runnable search) {
        if (task.isCancelled()) {
            finishIfStillRunning(null);
            return;
        }
        if (requestFinishing.get()) {
            return;
        }
        final IndexingPressure pressure = indexingPressure;
        if (pressure != null) {
            final IndexingPressure.Coordinating charge;
            try {
                // The reserve is never forced, so a full coordinating budget refuses an estimate that fits under the limit instead of
                // borrowing past it. An estimate that was cut down to the limit is admitted whatever else the node holds, because
                // otherwise it would only ever fit on an empty node. The operation count is the page we asked for. The bytes are resized
                // to the bulk once it exists.
                final PageEstimate estimate = estimatedPageBytes(pressure);
                charge = pressure.reserveReindexPage(pageDocCount(), estimate.bytes(), estimate.cappedAtLimit());
            } catch (EsRejectedExecutionException e) {
                logger.debug("[{}]: waiting to reserve the next page [{}]", task.getId(), e.getMessage());
                pressure.addReindexPageReserveRejection();
                warnOnceIfReserveHasWaitedLong();
                scheduleReservationRetry(search);
                return;
            } catch (Exception e) {
                finishIfStillRunning(e);
                return;
            }
            reserveRefusedSinceMillis = NOT_REFUSED;
            if (inFlightCharge.compareAndSet(null, charge) == false) {
                charge.close();
                final IllegalStateException alreadyHeld = new IllegalStateException(
                    "task [" + task.getId() + "] already holds a coordinating charge"
                );
                finishIfStillRunning(alreadyHeld);
                assert false : alreadyHeld;
                return;
            }
            if (task.isCancelled() || requestFinishing.get()) {
                Releasables.close(inFlightCharge.getAndSet(null));
                finishIfStillRunning(null);
                return;
            }
        }
        if (requestFinishing.get() || task.isCancelled()) {
            Releasables.close(inFlightCharge.getAndSet(null));
            finishIfStillRunning(null);
            return;
        }
        try {
            search.run();
        } catch (Exception e) {
            if (requestFinishing.get() == false) {
                Releasables.close(inFlightCharge.getAndSet(null));
                finishHim(e);
            }
        }
    }

    /**
     * Notes a refused reserve, and logs once per worker when the refusals have run on for {@link #RESERVATION_WAIT_WARN_AFTER}.
     * The task is not failed, because a full budget is expected to clear, so this is the only sign a stuck wait leaves.
     */
    private void warnOnceIfReserveHasWaitedLong() {
        final long now = threadPool.relativeTimeInMillis();
        if (reserveRefusedSinceMillis == NOT_REFUSED) {
            reserveRefusedSinceMillis = now;
            return;
        }
        if (loggedLongReserveWait == false && now - reserveRefusedSinceMillis >= RESERVATION_WAIT_WARN_AFTER.millis()) {
            loggedLongReserveWait = true;
            logger.warn("[{}]: still waiting to reserve the next page after [{}]", task.getId(), RESERVATION_WAIT_WARN_AFTER);
        }
    }

    /**
     * Schedules another attempt to reserve the next page. One attempt is outstanding per worker. The delay is the request's initial retry
     * backoff, but never less than {@link #MIN_RESERVATION_WAIT}, and the attempt is cancelled when the task is cancelled.
     */
    private void scheduleReservationRetry(Runnable search) {
        if (task.isCancelled()) {
            finishIfStillRunning(null);
            return;
        }
        if (listeningForCancellation.compareAndSet(false, true)) {
            task.addListener(this::onReservationRetryCancelled);
        }
        // Published before scheduling, so cancellation can see the attempt even when the scheduler runs it immediately.
        ReservationRetry retry = new ReservationRetry();
        if (reservationRetry.compareAndSet(null, retry) == false) {
            // runReservationRetry clears the slot before it reserves, so this should not happen. Finishing the task is better than
            // dropping the search continuation and leaving the task running with nothing scheduled.
            final IllegalStateException e = new IllegalStateException(
                "task [" + task.getId() + "] already has a reservation retry scheduled"
            );
            finishIfStillRunning(e);
            assert false : e;
            return;
        }
        final Scheduler.ScheduledCancellable scheduled;
        try {
            // Generic, not the scheduler thread. The retry reserves and then searches, and a search listener can run inline.
            scheduled = threadPool.schedule(() -> runReservationRetry(retry, search), reservationWait(), threadPool.generic());
        } catch (Exception e) {
            reservationRetry.compareAndSet(retry, null);
            finishIfStillRunning(e);
            return;
        }
        // True when cancellation won before this attempt was attached, and the runnable will not start.
        if (retry.attach(scheduled)) {
            reservationRetry.compareAndSet(retry, null);
            finishIfStillRunning(null);
            return;
        }
        if (task.isCancelled() || requestFinishing.get()) {
            onReservationRetryCancelled();
        }
    }

    /**
     * The request's initial retry backoff, floored at {@link #MIN_RESERVATION_WAIT}. The setter allows zero, and the scheduler runs a
     * zero delay straight away, so a full budget would otherwise spin and count a rejection on every spin.
     */
    private TimeValue reservationWait() {
        final TimeValue initialBackoff = mainRequest.getRetryBackoffInitialTime();
        return initialBackoff.nanos() < MIN_RESERVATION_WAIT.nanos() ? MIN_RESERVATION_WAIT : initialBackoff;
    }

    private void onReservationRetryCancelled() {
        ReservationRetry scheduled = reservationRetry.getAndSet(null);
        // cancel returns true only when the attempt will not run. That attempt is the one that has to finish the task.
        if (scheduled != null && scheduled.cancel()) {
            finishIfStillRunning(null);
        }
    }

    private void runReservationRetry(ReservationRetry retry, Runnable search) {
        // Drop only this attempt. A newer one, published if the scheduler ran us before attach returned, has to stay.
        reservationRetry.compareAndSet(retry, null);
        if (requestFinishing.get()) {
            return;
        }
        reservePageThen(search);
    }

    /**
     * One scheduled reservation attempt. Published before the scheduler runs it, so cancel still finds it when the delay is zero.
     */
    private static final class ReservationRetry {
        private final AtomicBoolean cancelled = new AtomicBoolean();
        @Nullable
        private volatile Scheduler.ScheduledCancellable scheduled;

        /**
         * @return true when this attempt will not run
         */
        boolean cancel() {
            if (cancelled.compareAndSet(false, true) == false) {
                return false;
            }
            Scheduler.ScheduledCancellable current = scheduled;
            // Not attached yet. attach cancels the future and reports whether that stopped the run.
            // The caller onReservationRetryCancelled does not finish the task in that case. attach does, through
            // scheduleReservationRetry.
            if (current == null) {
                return false;
            }
            return current.cancel();
        }

        /**
         * @return true when this attempt was already cancelled and the scheduled run will not start
         */
        boolean attach(Scheduler.ScheduledCancellable scheduledRun) {
            this.scheduled = scheduledRun;
            if (cancelled.get() == false) {
                return false;
            }
            return scheduledRun.cancel();
        }
    }

    private void finishIfStillRunning(@Nullable Exception failure) {
        if (requestFinishing.get() == false) {
            finishHim(failure);
        }
    }

    /**
     * Documents the next search can return. An unset size uses the search default, which is what the search will actually fetch.
     */
    private int pageDocCount() {
        SearchSourceBuilder source = mainRequest.getSearchRequest().source();
        if (source == null || source.size() < 0) {
            return SearchService.DEFAULT_SIZE;
        }
        return source.size();
    }

    /**
     * The bytes to reserve for the next page, and whether that guess was cut down to the coordinating limit.
     */
    private record PageEstimate(long bytes, boolean cappedAtLimit) {}

    private PageEstimate estimatedPageBytes(IndexingPressure pressure) {
        final long limit = pressure.getCoordinatingLimitInBytes();
        // Read at reserve time so a dynamic update applies to this reserve, not to a value captured when the node started.
        final long perDocument = pressure.reindexBytesEstimatePerDocument();
        final long raw;
        try {
            // A product that wraps is negative, and would pass the min below and then shrink the node's total.
            raw = Math.addExact(
                Math.multiplyExact((long) pageDocCount(), perDocument),
                threadPool.getThreadContext().estimatedRequestContextBytes()
            );
        } catch (ArithmeticException e) {
            // Far past the limit, so it is capped the same way.
            return new PageEstimate(limit, true);
        }
        // A reservation larger than the limit is rejected even when the node is empty, so the estimate stops at the limit.
        return raw > limit ? new PageEstimate(limit, true) : new PageEstimate(raw, false);
    }

    /**
     * Sets the page's coordinating charge to the real size of the page. This happens once per page, on the first bulk built from it.
     * <p>
     * A page that is not split is resized to that bulk's real size. A page that {@code max_docs} splits still holds the hits the first
     * bulk left out, and those are in memory for as long as the page lives, so it is resized to the whole page and not to the first
     * sub bulk. Later sub bulks from the same page leave the charge alone, and record no estimate error. A partial retry never calls this.
     * <p>
     * A bulk that targets only system indices may grow the charge past the coordinating limit, as a system-only bulk was never refused
     * there. Any other page whose own size is above the limit fails.
     * <p>
     * This relies on the single worker chain that owns the page and its charge. Only one thread resizes or closes the charge at a time,
     * and each stage is handed to the next through an executor.
     *
     * @throws EsRejectedExecutionException when the page's own size is above the coordinating limit. The charge is left unchanged.
     */
    private void resizePageCharge(PaginatedSearchConsumableHitsResponse page, BulkRequest request) {
        IndexingPressure.Coordinating charge = page.coordinatingCharge();
        if (charge == null || page.chargeSized()) {
            return;
        }
        final long contextBytes = threadPool.getThreadContext().estimatedRequestContextBytes();
        long realBytes = request.ramBytesUsed() + contextBytes;
        if (page.hasRemainingHits()) {
            // Never below this sub bulk, and at least every hit the page holds.
            realBytes = max(realBytes, page.hitSourceBytes() + contextBytes);
        }
        long reserved = charge.currentBytes();
        if (realBytes > reserved) {
            long grew = realBytes - reserved;
            try {
                charge.increaseBytes(grew, isOnlySystem(request));
            } catch (EsRejectedExecutionException e) {
                indexingPressure.addReindexPageExceedsLimit();
                throw e;
            }
            indexingPressure.addReindexPageBytesAboveEstimate(grew);
        } else if (realBytes < reserved) {
            long shrank = reserved - realBytes;
            charge.reduceBytes(shrank);
            indexingPressure.addReindexPageBytesBelowEstimate(shrank);
        }
        page.markChargeSized();
    }

    private boolean isOnlySystem(BulkRequest request) {
        return systemIndices != null
            && indicesLookup != null
            && TransportBulkAction.isOnlySystem(request, indicesLookup.get(), systemIndices);
    }

    private void releaseHitForThisBulk(PaginatedHitSource.Hit hit) {
        hit.release();
        hitsReleasedWhileBuilding.add(hit);
    }

    private void releaseHits(List<? extends PaginatedHitSource.Hit> hits) {
        for (PaginatedHitSource.Hit hit : hits) {
            // The script already dropped this hit when it copied the source.
            if (hitsReleasedWhileBuilding.remove(hit) == false) {
                hit.release();
            }
        }
    }

    private boolean tryReleaseCurrentResponse(PaginatedSearchConsumableHitsResponse expected) {
        if (currentPaginatedSearchResponse.compareAndSet(expected, null)) {
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

        boolean isRoutingFromSlice();

        void setRoutingFromSlice(boolean routingFromSlice);

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
        public boolean isRoutingFromSlice() {
            return request.isRoutingFromSlice();
        }

        @Override
        public void setRoutingFromSlice(boolean routingFromSlice) {
            request.setRoutingFromSlice(routingFromSlice);
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
        public void setRoutingFromSlice(boolean routingFromSlice) {
            request.setRoutingFromSlice(routingFromSlice);
        }

        @Override
        public boolean isRoutingFromSlice() {
            return request.isRoutingFromSlice();
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
     * Applies a script to a request built from one search hit.
     * Once the script has replaced the source, the hit is released once. The request holds the new source, so the hit's bytes are no
     * longer needed, and the page cleanup does not release that hit again. A request with no script keeps the hit until the page is
     * released, because the bulk shares that source.
     */
    public abstract static class ScriptApplier<T extends Metadata>
        implements
            BiFunction<RequestWrapper<?>, PaginatedHitSource.Hit, RequestWrapper<?>> {

        // "index" is the default operation
        protected static final String INDEX = "index";

        private final WorkerBulkByPaginatedSearchTaskState taskWorker;
        protected final ScriptService scriptService;
        protected final Script script;
        protected final Map<String, Object> params;
        protected final LongSupplier nowInMillisSupplier;
        private Consumer<PaginatedHitSource.Hit> releaseConsumedHit = PaginatedHitSource.Hit::release;

        public ScriptApplier(
            WorkerBulkByPaginatedSearchTaskState taskWorker,
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

        /**
         * Where a consumed hit is released. The action records the hit so page cleanup does not release it again.
         */
        private void releaseConsumedHitWith(Consumer<PaginatedHitSource.Hit> releaseConsumedHit) {
            this.releaseConsumedHit = releaseConsumedHit;
        }

        @Override
        public RequestWrapper<?> apply(RequestWrapper<?> request, PaginatedHitSource.Hit doc) {
            if (script == null) {
                return request;
            }

            CtxMap<T> ctxMap = execute(doc, request.getSource());

            T metadata = ctxMap.getMetadata();

            request.setSource(ctxMap.getSource());
            // The request now holds the source the script produced, so the hit's bytes are no longer needed.
            // Shared source, the path with no script, keeps the hit until the page is released.
            releaseConsumedHit.accept(doc);

            updateRequest(request, metadata);

            return requestFromOp(request, metadata.getOp());
        }

        protected abstract CtxMap<T> execute(PaginatedHitSource.Hit doc, Map<String, Object> source);

        protected Runnable buildCancellationCheck() {
            BulkByPaginatedSearchTask task = taskWorker.getTask();
            return () -> { if (task.isCancelled()) throw new TaskCancelledException(task.getReasonCancelled()); };
        }

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

    /**
     * One page of search hits and the coordinating charge for that page.
     * The charge is closed once, when the page is dropped or when the caller has consumed every hit and releases the page.
     */
    static class PaginatedSearchConsumableHitsResponse {
        private final PaginatedHitSource.AsyncResponse asyncResponse;
        private volatile List<? extends PaginatedHitSource.Hit> hits;
        /**
         * Volatile for cross-thread visibility when {@link #remainingHits()} / {@link #hasRemainingHits()}
         * are read while another thread updates this field. {@link #consumeHits} uses a non-atomic
         * read-then-add on this value; {@code volatile} does not make that sequence atomic—callers rely
         * on at most one thread executing {@code consumeHits} per response, coordinated by
         * {@link AbstractAsyncBulkByPaginatedSearchAction#currentPaginatedSearchResponse} CAS, not on this field alone.
         */
        private volatile int consumedOffset = 0;
        private final Releasable releaseRemainingHitsOnce;
        private final AtomicReference<IndexingPressure.Coordinating> coordinatingCharge = new AtomicReference<>();
        private final Releasable closeChargeOnce;
        /**
         * Source bytes of every hit on the page, counted when the page arrives, before a script can release a hit.
         */
        private final long hitSourceBytes;
        /**
         * Whether the charge has been resized to this page. Set on the worker chain that owns the page, one stage after another.
         */
        private volatile boolean chargeSized;

        PaginatedSearchConsumableHitsResponse(PaginatedHitSource.AsyncResponse asyncResponse) {
            this.asyncResponse = asyncResponse;
            this.hits = asyncResponse.response().getHits();
            long sourceBytes = 0;
            for (PaginatedHitSource.Hit hit : hits) {
                BytesReference source = hit.getSource();
                if (source != null) {
                    sourceBytes += source.length();
                }
            }
            this.hitSourceBytes = sourceBytes;
            this.releaseRemainingHitsOnce = Releasables.releaseOnce(this::doReleaseRemainingHits);
            this.closeChargeOnce = Releasables.releaseOnce(this::closeCoordinatingCharge);
        }

        /**
         * Takes the charge reserved for this page. Called before the page is read, including when the search listener runs inline.
         * A second charge is closed so it cannot leak.
         */
        void attachCoordinatingCharge(@Nullable IndexingPressure.Coordinating charge) {
            if (charge == null) {
                return;
            }
            if (coordinatingCharge.compareAndSet(null, charge) == false) {
                charge.close();
            }
        }

        /**
         * The live coordinating charge for this page, or null when this page has none.
         */
        @Nullable
        IndexingPressure.Coordinating coordinatingCharge() {
            return coordinatingCharge.get();
        }

        /**
         * Source bytes of every hit this page arrived with, whether or not they are consumed yet.
         */
        long hitSourceBytes() {
            return hitSourceBytes;
        }

        /**
         * Whether the charge has already been resized to this page.
         */
        boolean chargeSized() {
            return chargeSized;
        }

        void markChargeSized() {
            chargeSized = true;
        }

        /**
         * Whether this page still holds a coordinating charge.
         */
        boolean holdsCoordinatingCharge() {
            return coordinatingCharge.get() != null;
        }

        /**
         * Closes the coordinating charge once every hit on this page has been consumed.
         * A page that still has hits keeps the charge for the next sub bulk.
         */
        void releaseChargeWhenNothingRemains() {
            if (hasRemainingHits() == false) {
                closeChargeOnce.close();
            }
        }

        private void closeCoordinatingCharge() {
            Releasables.close(coordinatingCharge.getAndSet(null));
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
            // Dropping the page drops its charge, including when the page held no hits.
            closeChargeOnce.close();
            asyncResponse.response().closeBodyReleasable();
        }

        /**
         * Drops this page's hit list, and the one on the underlying response, so a finished page that is still referenced does not
         * keep its hits reachable. Call only once every hit has been consumed and released.
         */
        void dropHits() {
            // Leave consumedOffset alone. remainingHits() reads hits.size() and then consumedOffset, so resetting the offset
            // could pair the old list with a zero offset, which looks like hits remain and would stop
            // releaseChargeWhenNothingRemains closing the page charge. With the old offset kept, both the old list and the
            // empty list report nothing remaining. doReleaseRemainingHits is then a no-op as well.
            hits = List.of();
            asyncResponse.response().dropHits();
        }

        void done(TimeValue extraKeepAlive) {
            asyncResponse.done(extraKeepAlive);
        }
    }
}
