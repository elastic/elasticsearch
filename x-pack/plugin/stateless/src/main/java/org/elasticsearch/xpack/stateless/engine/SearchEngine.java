/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.CompletionStatsCache;
import org.elasticsearch.index.engine.ElasticsearchReaderManager;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineCreationFailureException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.DocumentParser;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.ClosedShardService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.reshard.ReshardSearchFilters;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.concurrent.EsExecutors.DIRECT_EXECUTOR_SERVICE;
import static org.elasticsearch.index.IndexSettings.STATELESS_DEFAULT_REFRESH_INTERVAL;
import static org.elasticsearch.xpack.stateless.commits.BlobFileRanges.computeBlobFileRanges;

/**
 * {@link Engine} implementation for search shards
 * <p>
 * This class implements the minimal behavior to allow a stateless search shard to recover as a replica of an index/primary shard. Most of
 * indexing behavior is faked and will be removed once operations are not replicated anymore (ES-4861).
 * <p>
 * // TODO Remove methods related to indexing operations and local/global checkpoints
 * - {@link #index(Index)}
 * - {@link #delete(Delete)}
 * - {@link #noOp(NoOp)}
 * - {@link #getPersistedLocalCheckpoint()}
 */
public class SearchEngine extends Engine {

    private final ClosedShardService closedShardService;
    private final Map<PrimaryTermAndGeneration, SubscribableListener<Long>> segmentGenerationListeners = ConcurrentCollections
        .newConcurrentMap();
    private final LinkedBlockingQueue<NewCommitNotification> commitNotifications = new LinkedBlockingQueue<>();
    private final AtomicInteger pendingCommitNotifications = new AtomicInteger();
    private final ReferenceManager<ElasticsearchDirectoryReader> readerManager;
    private final Directory directory;
    private final SearchDirectory searchDirectory;
    private final CompletionStatsCache completionStatsCache;
    private final SearchCommitPrefetcher commitPrefetcher;
    private final SearchCommitPrefetcherDynamicSettings prefetcherDynamicSettings;
    // Used for filtering unowned documents from a shard during resharding.
    private final ReshardSearchFilters reshardSearchFilters;
    // task runner used to process commit notifications and incoming PIT metadata merges sequentially
    private final ThrottledTaskRunner processCommitTaskRunner;

    private volatile SegmentInfosAndCommit segmentInfosAndCommit;
    // The current primary term/generation is updated on initialization and after new commit notifications from the indexing shard.
    // It can be higher than the term/gen found by the search shard in the object store, if the commit is not yet uploaded or if the
    // commit is a hollow commit that has been uploaded before the indexing shard primary term changed (e.g., after a shard failure).
    private volatile PrimaryTermAndGeneration currentPrimaryTermGeneration;
    private volatile long maxSequenceNumber = SequenceNumbers.NO_OPS_PERFORMED;
    private volatile long processedLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
    private volatile long lastSearcherAcquiredTime;

    // Guarded by the openReaders monitor
    private final Map<DirectoryReader, OpenReaderInfo> openReaders = new HashMap<>();

    private final CircuitBreaker readerHeapBreaker;
    private final SegmentReservations reservations = new SegmentReservations();
    private final ToLongFunction<SegmentCommitInfo> segmentBytesFn = DirectoryReaderHeapEstimator::segmentBytes;
    private final AtomicLong refreshDeferredCount = new AtomicLong();
    private final AtomicLong refreshDeferredPendingBytes = new AtomicLong();
    // Signal from refreshIfNeeded to doUpdateInternalState that the last refresh was deferred and
    // segmentInfosAndCommit must be reverted so the commit notification is re-processed on the next cycle.
    private volatile boolean lastRefreshDeferred;
    // Coalesced retry slot: at most one deferred-refresh retry is in flight at a time. The volatile
    // field always tracks the most recently deferred notification so the retry re-injects the latest one
    // even after a burst of defers.
    private final AtomicBoolean deferredRefreshScheduled = new AtomicBoolean();
    private volatile NewCommitNotification pendingDeferredNotification;
    // Independent slot for event-driven retries fired when a reader closes and frees reservation bytes. Decoupled
    // from `deferredRefreshScheduled` so an in-flight timer does not block a budget-released kick (and vice
    // versa). Both paths converge in `commitNotifications` / `findLatestNotification`, which handles redundancy.
    private final AtomicBoolean immediateRetryScheduled = new AtomicBoolean();
    private final AtomicLong refreshImmediateRetryCount = new AtomicLong();
    private final RelocatedPITReaderTracker relocatedPITReaderTracker;

    @SuppressWarnings("this-escape")
    public SearchEngine(
        EngineConfig config,
        ClosedShardService closedShardService,
        StatelessSharedBlobCacheService statelessSharedBlobCacheService,
        ClusterSettings clusterSettings,
        Executor prefetchExecutor,
        SearchCommitPrefetcherDynamicSettings prefetcherDynamicSettings,
        CircuitBreaker readerHeapBreaker,
        StatelessReaderHeapMetrics readerHeapMetrics,
        ReshardSearchFilters reshardSearchFilters
    ) {
        super(config);
        assert config.isPromotableToPrimary() == false;
        this.reshardSearchFilters = reshardSearchFilters;
        this.closedShardService = closedShardService;
        this.readerHeapBreaker = readerHeapBreaker;
        var refreshExecutor = config.getThreadPool().executor(ThreadPool.Names.REFRESH);
        // we limit to one task to force sequential execution of enqueued tasks
        this.processCommitTaskRunner = new ThrottledTaskRunner("engine", 1, refreshExecutor);

        this.relocatedPITReaderTracker = new RelocatedPITReaderTracker(
            relocatedPITReader -> acquireSearcherSupplier(
                relocatedPITReader.wrapper,
                SearcherScope.EXTERNAL,
                r -> reshardSearchFilters.maybeWrapDirectoryReaderForPitRelocation(
                    r,
                    shardId,
                    engineConfig.getIndexSettings().getIndexMetadata(),
                    engineConfig.getMapperService(),
                    relocatedPITReader.reshardingMetadata,
                    relocatedPITReader.splitShardCountSummary
                ),
                relocatedPITReader.pitReaderManager
            )
        );

        ElasticsearchDirectoryReader directoryReader = null;
        ElasticsearchReaderManager readerManager = null;
        boolean success = false;
        store.incRef();
        try {
            this.directory = store.directory();
            this.searchDirectory = SearchDirectory.unwrapDirectory(store.directory());
            directoryReader = ElasticsearchDirectoryReader.wrap(
                new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(directory, config.getLeafSorter()), Lucene.SOFT_DELETES_FIELD),
                shardId
            );
            IndexCommit initialCommit = directoryReader.getIndexCommit();
            OptionalLong primaryTerm = searchDirectory.getPrimaryTerm(initialCommit.getSegmentsFileName());
            var initialSegmentInfosAndCommit = new SegmentInfosAndCommit(
                store.readLastCommittedSegmentsInfo(),
                searchDirectory.getCurrentCommit()
            );

            // Always reserve so the close listener can release a single Reservation regardless of whether the
            // initial commit ends up tracked as an "open reader". trackLocalOpenReader and registerReaderHeapRelease
            // both attach close listeners and can throw before doing so (IllegalArgumentException from
            // ElasticsearchDirectoryReader.addReaderCloseListener, AlreadyClosedException from the cache helper);
            // if either throws, the no-break charge above must be reverted.
            final SegmentReservations.Reservation initialReservation = reserveAndChargeWithoutBreaking(
                initialSegmentInfosAndCommit.segmentInfos()
            );
            boolean initialReservationOwned = false;
            try {
                // do not consider the empty commit an open reader (no data to delete from object store)
                if (primaryTerm.isPresent()) {
                    trackLocalOpenReader(
                        directoryReader,
                        initialCommit,
                        initialReservation,
                        initialSegmentInfosAndCommit.getBCCDependenciesForCommit()
                    );
                }
                registerReaderHeapRelease(directoryReader, initialReservation);
                initialReservationOwned = true;
            } finally {
                if (initialReservationOwned == false) {
                    releaseReservationAndUncharge(initialReservation);
                }
            }
            readerManager = new ElasticsearchReaderManager(directoryReader) {
                private SegmentInfosAndCommit previousSegmentInfosAndCommit;

                @Override
                protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
                    lastRefreshDeferred = false;
                    SegmentInfosAndCommit segmentInfosAndCommitCopy = segmentInfosAndCommit;
                    if (segmentInfosAndCommitCopy == previousSegmentInfosAndCommit) {
                        return null;
                    }
                    final SegmentInfos nextInfos = segmentInfosAndCommitCopy.segmentInfos();
                    final SegmentReservations.Reservation reservation = reservations.reserve(nextInfos, segmentBytesFn);
                    final long delta = reservation.bytesReserved();
                    if (delta > 0) {
                        if (readerHeapBreaker.getLimit() != -1) {
                            try {
                                readerHeapBreaker.addEstimateBytesAndMaybeBreak(delta, StatelessReaderHeapBreaker.NAME);
                            } catch (CircuitBreakingException e) {
                                // Establish the revert invariant first: if anything below throws, doUpdateInternalState
                                // still sees lastRefreshDeferred == true and rolls segmentInfosAndCommit back to the
                                // previous snapshot so the deferred notification is re-processed.
                                lastRefreshDeferred = true;
                                reservation.release();
                                refreshDeferredCount.incrementAndGet();
                                refreshDeferredPendingBytes.set(delta);
                                readerHeapMetrics.recordRefreshDeferred();
                                logger.warn(
                                    "[{}] deferring refresh: reader-heap limit would be exceeded — delta [{}] used [{}] limit [{}]",
                                    shardId,
                                    delta,
                                    readerHeapBreaker.getUsed(),
                                    readerHeapBreaker.getLimit()
                                );
                                return null;
                            }
                        } else {
                            readerHeapBreaker.addWithoutBreaking(delta);
                        }
                    }
                    // After the charge, every throw site below (Lucene.getIndexCommit / openIfChanged IOException,
                    // addReaderCloseListener IllegalArgumentException or AlreadyClosedException inside
                    // addNextReader / registerReaderHeapRelease) must roll the ledger and breaker back. Ownership
                    // transfers to a close listener only once registerReaderHeapRelease returns successfully (or,
                    // for the no-changes branch, once we have explicitly released the reservation ourselves).
                    boolean reservationOwned = false;
                    try {
                        final IndexCommit indexCommit = Lucene.getIndexCommit(nextInfos, directory);
                        ElasticsearchDirectoryReader next = (ElasticsearchDirectoryReader) DirectoryReader.openIfChanged(
                            referenceToRefresh,
                            indexCommit
                        );
                        if (next == null) {
                            releaseReservationAndUncharge(reservation);
                            reservationOwned = true;
                        } else {
                            addNextReader(next, segmentInfosAndCommitCopy, indexCommit, reservation);
                            reservationOwned = true;
                            refreshDeferredPendingBytes.set(0L);
                        }
                        previousSegmentInfosAndCommit = segmentInfosAndCommitCopy;
                        return next;
                    } finally {
                        if (reservationOwned == false) {
                            releaseReservationAndUncharge(reservation);
                        }
                    }
                }

                // Installs both close listeners (openReaders cleanup + heap-release) under a single ownership
                // flag so that if either install throws after the new reader has been opened, the reader is
                // closed here. The reservation itself is released by the surrounding finally in refreshIfNeeded.
                private void addNextReader(
                    ElasticsearchDirectoryReader next,
                    SegmentInfosAndCommit segmentInfosAndCommitCopy,
                    IndexCommit first,
                    SegmentReservations.Reservation reservation
                ) throws IOException {
                    boolean added = false;
                    try {
                        trackLocalOpenReader(next, first, reservation, segmentInfosAndCommitCopy.getBCCDependenciesForCommit());
                        registerReaderHeapRelease(next, reservation);
                        added = true;
                    } finally {
                        if (added == false) {
                            IOUtils.closeWhileHandlingException(next);
                        }
                    }
                }
            };

            this.segmentInfosAndCommit = initialSegmentInfosAndCommit;
            final var segmentInfos = initialSegmentInfosAndCommit.segmentInfos();
            final var compoundCommit = initialSegmentInfosAndCommit.statelessCompoundCommit();
            final var segmentIsHollow = compoundCommit == null ? false : compoundCommit.hollow();
            final var segmentPrimaryTerm = primaryTerm(segmentInfos);
            final var segmentGeneration = segmentInfos.getGeneration();
            final var operationPrimaryTerm = config().getPrimaryTermSupplier().getAsLong();
            if (segmentIsHollow == false) {
                currentPrimaryTermGeneration = new PrimaryTermAndGeneration(segmentPrimaryTerm, segmentGeneration);
            } else {
                // Hollow indexing shards do not flush when the primary term changes, so we can have a segment with a primary term
                // lower than the current operation primary term. We should consider the current operation primary term for notifying
                // segment generation listeners.
                currentPrimaryTermGeneration = new PrimaryTermAndGeneration(operationPrimaryTerm, segmentGeneration);
            }
            assert assertCurrentPrimaryTermGeneration(segmentInfosAndCommit, currentPrimaryTermGeneration);

            this.setSequenceNumbers(segmentInfosAndCommit.segmentInfos());
            this.readerManager = wrapForAssertions(readerManager, config);
            this.completionStatsCache = new CompletionStatsCache(() -> acquireSearcher("completion_stats"));
            this.readerManager.addListener(completionStatsCache);
            for (ReferenceManager.RefreshListener refreshListener : config.getExternalRefreshListener()) {
                readerManager.addListener(refreshListener);
            }
            // SearchEngine has a single reader manager, so internal refresh listeners must also be wired onto it.
            for (ReferenceManager.RefreshListener refreshListener : config.getInternalRefreshListener()) {
                readerManager.addListener(refreshListener);
            }
            this.prefetcherDynamicSettings = prefetcherDynamicSettings;
            this.commitPrefetcher = new SearchCommitPrefetcher(
                searchDirectory.getShardId(),
                statelessSharedBlobCacheService,
                searchDirectory::getCacheBlobReaderForPreFetching,
                config.getThreadPool(),
                prefetchExecutor,
                clusterSettings,
                prefetcherDynamicSettings
            );
            success = true;
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "Failed to create a search engine", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(readerManager, directoryReader, store::decRef);
            }
        }
    }

    // Package-private (was private) so tests can override to inject an exception and verify the leak-plug
    // try/finally at each call site releases the reservation and refunds the breaker.
    void trackLocalOpenReader(
        ElasticsearchDirectoryReader directoryReader,
        IndexCommit commit,
        SegmentReservations.Reservation reservation,
        Set<PrimaryTermAndGeneration> bccDependencies
    ) throws IOException {
        ElasticsearchDirectoryReader.addReaderCloseListener(directoryReader, ignored -> {
            synchronized (openReaders) {
                openReaders.remove(directoryReader);
            }
        });

        synchronized (openReaders) {
            openReaders.put(directoryReader, new OpenReaderInfo(commit.getFileNames(), reservation, bccDependencies));
        }
    }

    // Reserve bytes for a non-refresh reader (engine open, PIT relocation) using the no-break path so the caller's
    // open never fails on budget pressure. The returned Reservation must be paired with registerReaderHeapRelease
    // so the close listener decrements both the ledger and the breaker.
    private SegmentReservations.Reservation reserveAndChargeWithoutBreaking(SegmentInfos infos) {
        SegmentReservations.Reservation reservation = reservations.reserve(infos, segmentBytesFn);
        if (reservation.bytesReserved() > 0) {
            readerHeapBreaker.addWithoutBreaking(reservation.bytesReserved());
        }
        return reservation;
    }

    // Failure-path inverse of reserveAndChargeWithoutBreaking / refreshIfNeeded's charge step. Releases the ledger
    // reservation and refunds the breaker by however many bytes the ledger actually freed. Reservation.release()
    // is idempotent, so this is safe to call even if a close listener was later installed and may also fire.
    private void releaseReservationAndUncharge(SegmentReservations.Reservation reservation) {
        long freed = reservation.release();
        if (freed > 0) {
            readerHeapBreaker.addWithoutBreaking(-freed);
        }
    }

    // Capture only the Reservation handle (a short key set internally) for the close listener so the lambda does
    // not pin the full SegmentInfos / SegmentInfo graph for the lifetime of the reader. When the close actually
    // frees ledger bytes, kick a budget-released retry for any pending deferred refresh.
    //
    // Package-private (was private) so tests can override to inject an exception and verify the leak-plug
    // try/finally at each call site releases the reservation and refunds the breaker.
    void registerReaderHeapRelease(ElasticsearchDirectoryReader reader, SegmentReservations.Reservation reservation) throws IOException {
        ElasticsearchDirectoryReader.addReaderCloseListener(reader, ignored -> {
            long released = reservation.release();
            if (released > 0) {
                readerHeapBreaker.addWithoutBreaking(-released);
                maybeFireImmediateRetryOnRelease();
            }
        });
    }

    // visible for testing
    public long getReaderHeapReservedBytes() {
        return reservations.totalBytes();
    }

    // visible for testing
    public long getRefreshDeferredCount() {
        return refreshDeferredCount.get();
    }

    // visible for testing
    public long getRefreshDeferredPendingBytes() {
        return refreshDeferredPendingBytes.get();
    }

    // visible for testing — counts how many times the close-listener-driven immediate retry actually re-injected
    // the pending deferred notification onto the processor.
    public long getRefreshImmediateRetryCount() {
        return refreshImmediateRetryCount.get();
    }

    PrimaryTermAndGeneration getCurrentPrimaryTermAndGeneration() {
        var current = this.currentPrimaryTermGeneration;
        assert current.generation() > 0 : current;
        return current;
    }

    // visible for testing
    long getPendingCommitNotifications() {
        return pendingCommitNotifications.get();
    }

    public long getTotalPrefetchedBytes() {
        return commitPrefetcher.getTotalPrefetchedBytes();
    }

    // visible for testing
    public SearchCommitPrefetcher.BCCPreFetchedOffset getMaxPrefetchedOffset() {
        return commitPrefetcher.getMaxPrefetchedOffset();
    }

    // visible for testing
    public SearchCommitPrefetcherDynamicSettings getPrefetcherDynamicSettings() {
        return prefetcherDynamicSettings;
    }

    public Set<PrimaryTermAndGeneration> getAcquiredPrimaryTermAndGenerations() {
        // capture the term/gen used by opened Lucene generational files
        final var termAndGens = new HashSet<>(searchDirectory.getAcquiredGenerationalFileTermAndGenerations());
        // CHM iterators are weakly consistent, meaning that we're not guaranteed to see new insertions while we compute
        // the set of remaining open reader referenced BCCs, that's why we use a regular HashMap with synchronized.
        synchronized (openReaders) {
            for (var openReader : openReaders.values()) {
                termAndGens.addAll(openReader.referencedBCCs());
            }
        }
        return Collections.unmodifiableSet(termAndGens);
    }

    /**
     * Process a new commit notification from the primary, and complete the provided {@code listener} when this commit (or a later commit)
     * is visible to searches.
     */
    public void onCommitNotification(NewCommitNotification notification, ActionListener<Void> listener) {
        logger.trace(
            "{} received new commit notification [bcc={}, cc={}] with latest uploaded {} from node [{}] and cluster state version [{}]",
            shardId,
            notification.batchedCompoundCommitGeneration(),
            notification.compoundCommit().primaryTermAndGeneration(),
            notification.latestUploadedBatchedCompoundCommitTermAndGen(),
            notification.nodeId(),
            notification.clusterStateVersion()
        );
        var ccTermAndGen = notification.compoundCommit().primaryTermAndGeneration();
        searchDirectory.updateLatestUploadedBcc(notification.latestUploadedBatchedCompoundCommitTermAndGen());
        searchDirectory.updateLatestCommitInfo(ccTermAndGen, notification.nodeId());

        SubscribableListener
            // Dispatch the pre-fetching if enabled right away and if there are multiple concurrent
            // pre-fetching the cache would take care of only pre-fetching the missing gaps once.
            // There's a small risk of a merge invalidating the background pre-fetching, and wasting
            // cache space and requests.
            .<Void>newForked(l -> maybePreFetchLatestCommit(notification, l))
            .<Void>andThen(l -> {
                if (addOrExecuteSegmentGenerationListener(ccTermAndGen, l.map(g -> null))) {
                    commitNotifications.add(notification);
                    if (pendingCommitNotifications.incrementAndGet() == 1) {
                        processCommitNotifications();
                    }
                }
            })
            .addListener(listener);
    }

    private void maybePreFetchLatestCommit(NewCommitNotification newCommitNotification, ActionListener<Void> listener) {
        if (store.isClosing() || store.tryIncRef() == false) {
            // If the store is closed just skip prefetching
            listener.onResponse(null);
            return;
        }
        long timeSinceLastSearcherWasAcquiredInMillis = lastSearcherAcquiredTime == 0L
            ? Long.MAX_VALUE // never acquired a searcher
            : engineConfig.getThreadPool().relativeTimeInMillis() - lastSearcherAcquiredTime;
        commitPrefetcher.maybePrefetchLatestCommit(
            newCommitNotification,
            timeSinceLastSearcherWasAcquiredInMillis,
            listener.delegateResponse((l, e) -> {
                // We don't want to prevent the new commit notification to make progress
                logger.warn(() -> "Unable to prefetch commit for notification " + newCommitNotification, e);
                l.onResponse(null);
            })
        );

        // We're just interested in ensuring that the store is not closing, we don't need to hold a reference while the prefetch runs
        store.decRef();
    }

    private void processCommitNotifications() {
        AbstractRunnable processCommitNotification = new AbstractRunnable() {
            private final RefCounted finish = AbstractRefCounted.of(this::finish);
            int batchSize = 0;

            @Override
            protected void doRun() throws Exception {
                ensureOpen();
                batchSize = pendingCommitNotifications.get();
                assert batchSize > 0 : batchSize;

                assert assertCurrentPrimaryTermGeneration(segmentInfosAndCommit, currentPrimaryTermGeneration);
                final SegmentInfos current = segmentInfosAndCommit.segmentInfos();
                NewCommitNotification latestNotification = findLatestNotification(current);
                if (latestNotification == null) {
                    logger.trace("directory is on most recent commit generation [{}]", current.getGeneration());
                    // TODO should we assert that we have no segment listeners with minGen <= current.getGeneration()?
                    return;
                }
                StatelessCompoundCommit latestCommit = latestNotification.compoundCommit();
                if (searchDirectory.isMarkedAsCorrupted()) {
                    logger.trace("directory is marked as corrupted, ignoring all future commit notifications");
                    failSegmentGenerationListeners();
                    return;
                }

                ListenableFuture<Map<String, BlobFileRanges>> listenableFuture = new ListenableFuture<>();
                if (prefetcherDynamicSettings.internalFilesReplicatedContentForSearchShardsEnabled()) {
                    var newCommitFiles = new HashMap<>(latestCommit.commitFiles());
                    newCommitFiles.keySet().removeAll(searchDirectory.getKnownFileNames());
                    Map<String, BlobFileRanges> newBlobFileRanges = ConcurrentCollections.newConcurrentMap();
                    ObjectStoreService.readReferencedCompoundCommitsUsingCache(
                        newCommitFiles,
                        null,
                        searchDirectory,
                        IOContext.DEFAULT,
                        DIRECT_EXECUTOR_SERVICE,
                        referencedCompoundCommit -> {
                            newBlobFileRanges.putAll(
                                computeBlobFileRanges(
                                    true,
                                    referencedCompoundCommit.statelessCompoundCommitReference().compoundCommit(),
                                    referencedCompoundCommit.statelessCompoundCommitReference().headerOffsetInTheBccBlobFile(),
                                    referencedCompoundCommit.referencedInternalFiles()
                                )
                            );
                        },
                        listenableFuture.map(aVoid -> newBlobFileRanges)
                    );
                } else {
                    listenableFuture.onResponse(Map.of());
                }
                assert listenableFuture.isDone() : "unexpected sync call not done after invocation";
                final NewCommitNotification notificationToApply = latestNotification;
                listenableFuture.addListener(ActionListener.wrap(blobFileRangesMap -> {
                    logger.trace("updating directory with commit {}", latestCommit);
                    if (store.tryIncRef() == false) {
                        throw new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
                    }
                    try {
                        final boolean commitUpdated = searchDirectory.updateCommit(latestCommit, blobFileRangesMap);
                        if (commitUpdated) {
                            updateInternalState(notificationToApply, current);
                        }
                    } finally {
                        store.decRef();
                    }
                }, this::onFailure));
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof AlreadyClosedException == false) {
                    failEngine("failed to refresh segments", e);
                } else {
                    logger.debug("failed to process commit notification, engine is closed", e);
                }
            }

            @Override
            public void onAfter() {
                finish.decRef();
            }

            private void finish() {
                var remaining = pendingCommitNotifications.addAndGet(-batchSize);
                assert remaining >= 0 : remaining;
                if (remaining > 0 && isClosed.get() == false) {
                    processCommitNotifications();
                }
            }

            private NewCommitNotification findLatestNotification(SegmentInfos current) throws IOException {
                PrimaryTermAndGeneration currentPrimaryTermGeneration = new PrimaryTermAndGeneration(
                    primaryTerm(current),
                    current.getGeneration()
                );
                NewCommitNotification latestNotification = null;
                for (int i = batchSize; i > 0; i--) {
                    NewCommitNotification notification = commitNotifications.poll();
                    StatelessCompoundCommit commit = notification.compoundCommit();
                    assert commit != null;
                    if (commit.primaryTermAndGeneration().compareTo(currentPrimaryTermGeneration) <= 0) {
                        assert commit.primaryTermAndGeneration().compareTo(currentPrimaryTermGeneration) < 0
                            || current.files(true).equals(commit.commitFiles().keySet());
                        logger.trace(
                            "notification for commit generation [{}] is older or same than current generation [{}], ignoring",
                            commit.generation(),
                            currentPrimaryTermGeneration.generation()
                        );
                        continue;
                    }
                    if (latestNotification == null
                        || commit.primaryTermAndGeneration()
                            .compareTo(latestNotification.compoundCommit().primaryTermAndGeneration()) > 0) {
                        latestNotification = notification;
                    }
                }
                return latestNotification;
            }

            private void updateInternalState(NewCommitNotification latestNotification, SegmentInfos current) {
                try {
                    doUpdateInternalState(latestNotification, current);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            private void doUpdateInternalState(NewCommitNotification latestNotification, SegmentInfos current) throws IOException {
                final StatelessCompoundCommit latestCommit = latestNotification.compoundCommit();
                final SegmentInfos next = Lucene.readSegmentInfos(directory);
                setSequenceNumbers(next);

                assert next.getGeneration() == latestCommit.generation();
                final SegmentInfosAndCommit previousSegmentInfosAndCommitSnapshot = segmentInfosAndCommit;
                segmentInfosAndCommit = new SegmentInfosAndCommit(next, latestCommit);

                // The shard uses a reentrant read/write lock to guard again engine changes, a type of lock that prioritizes the threads
                // waiting for the write lock over the threads trying to acquire a (non-reentrant) read lock. Because refresh listeners
                // sometimes access the engine read lock, we need to ensure that they won't block if another thread is waiting for the
                // engine write lock. To ensure that, we acquire the read lock before the refresh lock.
                final var engineReadLock = engineConfig.getEngineResetLock().readLock();
                engineReadLock.lock();
                try {
                    readerManager.maybeRefreshBlocking();
                } finally {
                    engineReadLock.unlock();
                }

                // The reader-heap breaker deferred the refresh: revert segmentInfosAndCommit so the (commit,
                // reader) invariant holds, and schedule a retry tied to the index's refresh_interval so the
                // refresh is attempted again without waiting for a fresh commit notification.
                if (lastRefreshDeferred) {
                    segmentInfosAndCommit = previousSegmentInfosAndCommitSnapshot;
                    scheduleDeferredRefreshRetry(latestNotification);
                    return;
                }

                // must be after refresh for `addOrExecuteSegmentGenerationListener to work.
                currentPrimaryTermGeneration = new PrimaryTermAndGeneration(
                    primaryTerm(segmentInfosAndCommit.segmentInfos()),
                    segmentInfosAndCommit.getGeneration()
                );
                assert assertCurrentPrimaryTermGeneration(segmentInfosAndCommit, currentPrimaryTermGeneration);
                // The advance has caught up to (or past) any previously deferred notification, so the slot is
                // obsolete. Clearing it lets a concurrent reader-close trigger or a scheduled timer read null
                // and short-circuit, avoiding a wasted retry that races with this update.
                pendingDeferredNotification = null;

                var reader = readerManager.acquire();
                try {
                    assert assertSegmentInfosAndCommits(reader, latestCommit, current, next);
                    Set<String> filesToRetain;
                    synchronized (openReaders) {
                        filesToRetain = openReaders.values()
                            .stream()
                            .flatMap(openReaderInfo -> openReaderInfo.files().stream())
                            .collect(Collectors.toSet());
                    }
                    searchDirectory.retainFiles(filesToRetain);
                    logger.debug("segments updated from generation [{}] to [{}]", current.getGeneration(), next.getGeneration());
                    callSegmentGenerationListeners(
                        new PrimaryTermAndGeneration(primaryTerm(reader.getIndexCommit()), reader.getIndexCommit().getGeneration())
                    );
                } finally {
                    readerManager.release(reader);
                }
            }

            private boolean assertSegmentInfosAndCommits(
                ElasticsearchDirectoryReader reader,
                StatelessCompoundCommit latestCommit,
                SegmentInfos currentSegmentInfos,
                SegmentInfos nextSegmentInfos
            ) {
                try {
                    var readerCommit = reader.getIndexCommit();
                    assert readerCommit.getGeneration() == latestCommit.generation()
                        : latestCommit.shardId()
                            + ": Directory reader commit generation ["
                            + readerCommit.getGeneration()
                            + "] does not match expected generation ["
                            + latestCommit.generation()
                            + ']';

                    PrimaryTermAndGeneration currentSegmentInfosPTG = new PrimaryTermAndGeneration(
                        primaryTerm(currentSegmentInfos),
                        currentSegmentInfos.getGeneration()
                    );
                    PrimaryTermAndGeneration nextSegmentInfosPTG = new PrimaryTermAndGeneration(
                        primaryTerm(nextSegmentInfos),
                        nextSegmentInfos.getGeneration()
                    );
                    assert currentSegmentInfosPTG.compareTo(nextSegmentInfosPTG) < 0
                        : latestCommit.shardId()
                            + ": SegmentInfos primary term and generation "
                            + nextSegmentInfosPTG
                            + " must be higher than previous primary term and generation "
                            + currentSegmentInfosPTG;

                    assert primaryTerm(readerCommit) == latestCommit.primaryTerm()
                        : Strings.format(
                            "%s: Directory reader primary term=%d doesn't match latest commit primary term=%d",
                            latestCommit.shardId(),
                            primaryTerm(readerCommit),
                            latestCommit.primaryTerm()
                        );
                    assert primaryTerm(readerCommit) != Engine.UNKNOWN_PRIMARY_TERM
                        : latestCommit.shardId() + ": Directory reader primary term is not known";
                    assert nextSegmentInfosPTG.primaryTerm() != Engine.UNKNOWN_PRIMARY_TERM
                        : latestCommit.shardId() + ": SegmentInfos primary term is not known";
                    assert primaryTerm(readerCommit) == primaryTerm(nextSegmentInfos)
                        : Strings.format(
                            "%s: Directory reader primary term=%d doesn't match latest SegmentInfos primary term=%d",
                            latestCommit.shardId(),
                            primaryTerm(readerCommit),
                            nextSegmentInfosPTG.primaryTerm()
                        );
                } catch (IOException ioe) {
                    assert false : ioe;
                }
                return true;
            }
        };
        processCommitTaskRunner.enqueueTask(new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                try (releasable) {
                    processCommitNotification.run();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("failed to process commit notification", e);
            }
        });
    }

    private void setSequenceNumbers(SegmentInfos segmentInfos) {
        final var commit = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(segmentInfos.userData.entrySet());
        assert commit.maxSeqNo() >= maxSequenceNumber
            : "Commit [" + commit + "] max sequence number less than tracked max sequence number [" + maxSequenceNumber + "]";
        maxSequenceNumber = commit.maxSeqNo();
        assert commit.localCheckpoint() >= processedLocalCheckpoint
            : "Commit [" + commit + "] local checkpoint less than tracked local checkpoint [" + processedLocalCheckpoint + "]";
        processedLocalCheckpoint = commit.localCheckpoint();
    }

    /**
     * Delay before retrying a deferred refresh. Set to twice the index's {@code refresh_interval} so the retry
     * does not race a natural refresh cycle: when writes continue, the next natural notification arrives first and
     * either advances the reader (making the retry a no-op via {@code findLatestNotification}) or coalesces a fresh
     * deferred state into the same retry slot. When writes stop, the retry kicks in after at most one missed cycle,
     * bounding staleness on quiet indices.
     */
    private TimeValue deferredRefreshRetryDelay() {
        long intervalMillis = engineConfig.getIndexSettings().getRefreshInterval().millis();
        if (intervalMillis < 0) {
            // refreshes disabled / manual mode — still retry so a quiet index doesn't pin forever.
            intervalMillis = STATELESS_DEFAULT_REFRESH_INTERVAL.millis();
        } else if (intervalMillis < 1000L) {
            // sanity floor against pathologically small intervals.
            intervalMillis = 1000L;
        }
        return TimeValue.timeValueMillis(intervalMillis * 2L);
    }

    /**
     * Schedule a deferred-refresh retry. Coalesces multiple defers into a single in-flight task; the latest deferred
     * notification is captured so the retry re-injects the most recent one.
     * <p>
     * The retry appends its deferred notification to the tail of {@code commitNotifications}. That position in the
     * queue is irrelevant to correctness: {@code findLatestNotification} polls a batch and selects the
     * <em>maximum-generation</em> entry (not the most-recently-added one), so a newer natural notification queued
     * either before or after the retry's entry still wins. Two race cases follow:
     * <ul>
     * <li>A newer natural notification {@code N2} is already queued but unprocessed when the retry adds the older
     * {@code N1}: the batch contains both, {@code findLatestNotification} picks {@code N2}, and {@code N1} is
     * dropped implicitly (the loop only opens a reader for the chosen latest).</li>
     * <li>A newer natural notification {@code N2} has already advanced the reader past {@code N1}'s generation
     * before the retry fires: {@code findLatestNotification} skips {@code N1} because its generation is
     * {@code <= currentPrimaryTermGeneration}, and the {@code previousSegmentInfosAndCommit} check inside
     * {@code refreshIfNeeded} is a second line of defense.</li>
     * </ul>
     */
    private void scheduleDeferredRefreshRetry(NewCommitNotification notification) {
        pendingDeferredNotification = notification;
        if (deferredRefreshScheduled.compareAndSet(false, true) == false) {
            return;
        }
        engineConfig.getThreadPool().schedule(() -> {
            NewCommitNotification toRetry = pendingDeferredNotification;
            deferredRefreshScheduled.set(false);
            if (isClosed.get() || toRetry == null) {
                return;
            }
            commitNotifications.add(toRetry);
            if (pendingCommitNotifications.incrementAndGet() == 1) {
                processCommitNotifications();
            }
        }, deferredRefreshRetryDelay(), engineConfig.getThreadPool().executor(ThreadPool.Names.REFRESH));
    }

    /**
     * Event-driven counterpart to {@link #scheduleDeferredRefreshRetry}: kick a retry of the latest deferred
     * notification immediately when a reader closes and frees ledger bytes, in case the budget headroom now
     * accommodates a previously-deferred refresh. Skips if there is nothing pending, if the pending notification
     * has already been overtaken by the current generation, or if another immediate retry is already in flight.
     * Independent of the timer-based retry slot — both paths converge in {@code commitNotifications} and
     * {@code findLatestNotification} handles any redundancy.
     */
    private void maybeFireImmediateRetryOnRelease() {
        NewCommitNotification pending = pendingDeferredNotification;
        if (pending == null || isClosed.get()) {
            return;
        }
        PrimaryTermAndGeneration current = currentPrimaryTermGeneration;
        if (current != null && pending.compoundCommit().primaryTermAndGeneration().compareTo(current) <= 0) {
            return;
        }
        if (immediateRetryScheduled.compareAndSet(false, true) == false) {
            return;
        }
        engineConfig.getThreadPool().executor(ThreadPool.Names.REFRESH).execute(() -> {
            NewCommitNotification toRetry = pendingDeferredNotification;
            immediateRetryScheduled.set(false);
            if (isClosed.get() || toRetry == null) {
                return;
            }
            refreshImmediateRetryCount.incrementAndGet();
            commitNotifications.add(toRetry);
            if (pendingCommitNotifications.incrementAndGet() == 1) {
                processCommitNotifications();
            }
        });
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                // Save any active reader information to the ClosedShardService BEFORE potentially closing the store. The ClosedShardService
                // is hooked into store closure, so we don't want to race with it!
                closedShardService.onShardClose(shardId, getAcquiredPrimaryTermAndGenerations());
                IOUtils.close(this::failSegmentGenerationListeners, readerManager, relocatedPITReaderTracker, store::decRef);
                assert segmentGenerationListeners.isEmpty() : segmentGenerationListeners;
            } catch (Exception ex) {
                logger.warn("failed to close reader", ex);
            } finally {
                closedLatch.countDown();
            }
        }
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        throw unsupportedException();
    }

    @Override
    public DeleteResult delete(Delete delete) {
        throw unsupportedException();
    }

    @Override
    public NoOpResult noOp(NoOp noOp) {
        throw unsupportedException();
    }

    @Override
    public SegmentInfos getLastCommittedSegmentInfos() {
        return segmentInfosAndCommit.segmentInfos();
    }

    @Override
    public String getHistoryUUID() {
        return segmentInfosAndCommit.segmentInfos().getUserData().get(Engine.HISTORY_UUID_KEY);
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return new Translog.Location(0L, 0L, 0);
    }

    private static SequenceNumbers.CommitInfo getSequenceNumbersCommitInfo(SegmentInfos segmentInfos) {
        return SequenceNumbers.loadSeqNoInfoFromLuceneCommit(segmentInfos.userData.entrySet());
    }

    @Override
    public long getMaxSeqNo() {
        return maxSequenceNumber;
    }

    @Override
    public long getProcessedLocalCheckpoint() {
        return processedLocalCheckpoint;
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return processedLocalCheckpoint;
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        // The indexing shard sends data to the search shard only after they are fully persisted on the object store. So using the processed
        // local checkpoint is fine. If it is a bit behind, a new refresh will ultimately make the search shard catch up.
        return processedLocalCheckpoint;
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return completionStatsCache.get(fieldNamePatterns);
    }

    @Override
    public long getIndexThrottleTimeInMillis() {
        return 0;
    }

    @Override
    public boolean isThrottled() {
        return false;
    }

    @Override
    public void trimOperationsFromTranslog(long belowTerm, long aboveSeqNo) throws EngineException {

    }

    @Override
    public GetResult get(
        Get get,
        MappingLookup mappingLookup,
        DocumentParser documentParser,
        SplitShardCountSummary splitShardCountSummary,
        Function<Searcher, Searcher> searcherWrapper
    ) {
        return getFromSearcher(get, acquireSearcher("get", SearcherScope.EXTERNAL, splitShardCountSummary, searcherWrapper), false);
    }

    @Override
    protected void onSearcherCreation(String source, SearcherScope scope) {
        super.onSearcherCreation(source, scope);
        if (source.equals(CAN_MATCH_SEARCH_SOURCE) || source.equals(SEARCH_SOURCE)) {
            lastSearcherAcquiredTime = engineConfig.getThreadPool().relativeTimeInMillis();
        }
    }

    @Override
    protected DirectoryReader wrapExternalDirectoryReader(DirectoryReader reader, SplitShardCountSummary summary) throws IOException {
        return reshardSearchFilters.maybeWrapDirectoryReader(
            reader,
            shardId,
            summary,
            engineConfig.getIndexSettings().getIndexMetadata(),
            engineConfig.getMapperService()
        );
    }

    public long getLastSearcherAcquiredTime() {
        return lastSearcherAcquiredTime;
    }

    public void setLastSearcherAcquiredTime(long lastSearcherAcquiredTime) {
        long currentLastSearcherAcquiredTime = this.lastSearcherAcquiredTime;
        if (lastSearcherAcquiredTime > currentLastSearcherAcquiredTime) {
            logger.trace(
                "update engine last searcher acquired time for [{}] from [{}] to [{}]",
                shardId,
                currentLastSearcherAcquiredTime,
                lastSearcherAcquiredTime
            );
            this.lastSearcherAcquiredTime = lastSearcherAcquiredTime;
        }
    }

    @Override
    public SearcherSupplier acquireSearcherSupplier(
        Function<Searcher, Searcher> wrapper,
        SearcherScope scope,
        CheckedFunction<DirectoryReader, DirectoryReader, IOException> externalDirectoryReaderWrapper,
        ReferenceManager<ElasticsearchDirectoryReader> referenceManager
    ) throws EngineException {
        final SearcherSupplier delegate = super.acquireSearcherSupplier(wrapper, scope, externalDirectoryReaderWrapper, referenceManager);
        String commitId = Base64.getEncoder().encodeToString(getLastCommittedSegmentInfos().getId());
        return new SearcherSupplier(Function.identity()) {
            @Override
            protected void doClose() {
                delegate.close();
            }

            @Override
            public Searcher acquireSearcherInternal(String source) {
                return delegate.acquireSearcher(source);
            }

            @Override
            public String getSearcherId() {
                return commitId;
            }
        };
    }

    @Override
    protected ReferenceManager<ElasticsearchDirectoryReader> getReferenceManager(SearcherScope scope) {
        return readerManager;
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return false;
    }

    @Override
    public void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener) {
        listener.accept(null);
    }

    @Override
    public void asyncEnsureGlobalCheckpointSynced(long globalCheckpoint, Consumer<Exception> listener) {
        listener.accept(null);
    }

    @Override
    public void syncTranslog() throws IOException {

    }

    @Override
    public Closeable acquireHistoryRetentionLock() {
        return null;
    }

    @Override
    public int countChanges(String source, long fromSeqNo, long toSeqNo) throws IOException {
        return 0;
    }

    @Override
    public Translog.Snapshot newChangesSnapshot(
        String source,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        long chunkSize
    ) throws IOException {
        return null;
    }

    @Override
    public boolean hasCompleteOperationHistory(String reason, long startingSeqNo) {
        return false;
    }

    @Override
    public long getMinRetainedSeqNo() {
        return 0;
    }

    @Override
    public TranslogStats getTranslogStats() {
        return null;
    }

    @Override
    public long getMaxSeqNoOfUpdatesOrDeletes() {
        assert false;
        return SequenceNumbers.NO_OPS_PERFORMED;
    }

    @Override
    public void advanceMaxSeqNoOfUpdatesOrDeletes(long maxSeqNoOfUpdatesOnPrimary) {}

    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        return buildSeqNoStats(config(), segmentInfosAndCommit.segmentInfos());
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments() {
        ensureOpen();
        final SegmentInfos current = this.segmentInfosAndCommit.segmentInfos();
        if (current.size() > 0) {
            final Set<Segment> segments = new TreeSet<>(Comparator.comparingLong(Segment::getGeneration));
            for (SegmentCommitInfo info : current) {
                final Segment segment = new Segment(info.info.name);
                segment.search = true;
                segment.committed = true;
                segment.delDocCount = info.getDelCount() + info.getSoftDelCount();
                segment.docCount = info.info.maxDoc() - segment.delDocCount;
                segment.version = info.info.getVersion();
                segment.compound = info.info.getUseCompoundFile();
                segment.segmentSort = info.info.getIndexSort();
                segment.attributes = info.info.getAttributes();
                try {
                    segment.sizeInBytes = info.sizeInBytes();
                } catch (IOException e) {
                    logger.trace(() -> "failed to get size for [" + info.info.name + "]", e);
                }
                segments.add(segment);
            }
            return segments.stream().toList();
        }
        return List.of();
    }

    @Override
    public List<Segment> segments(boolean includeVectorFormatsInfo) {
        // TODO : include vector formats, when required
        return segments();
    }

    @Override
    public RefreshResult refresh(String source) throws EngineException {
        return RefreshResult.NO_REFRESH;
    }

    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {
        ActionListener.completeWith(listener, () -> RefreshResult.NO_REFRESH);
    }

    @Override
    public void writeIndexingBuffer() throws EngineException {

    }

    @Override
    public boolean shouldPeriodicallyFlush() {
        return false;
    }

    @Override
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, FlushResultListener listener) throws EngineException {
        listener.onResponse(FlushResult.FLUSH_REQUEST_PROCESSED_AND_NOT_PERFORMED);
    }

    @Override
    public void trimUnreferencedTranslogFiles() throws EngineException {

    }

    @Override
    public boolean shouldRollTranslogGeneration() {
        return false;
    }

    @Override
    public void rollTranslogGeneration() throws EngineException {

    }

    @Override
    public void forceMerge(boolean flush, int maxNumSegments, boolean onlyExpungeDeletes, String forceMergeUUID) throws EngineException,
        IOException {

    }

    @Override
    public IndexCommitRef acquireLastIndexCommit(boolean flushFirst) throws EngineException {
        if (flushFirst) {
            // there might be uncommitted changes on the indexing nodes.
            throw new IllegalArgumentException("Search engine does not support acquiring last index commit with flush_first");
        }
        Searcher searcher = acquireSearcher("acquire_last_commit");
        try {
            final IndexCommit indexCommit;
            try {
                indexCommit = searcher.getDirectoryReader().getIndexCommit();
            } catch (IOException e) {
                throw new EngineException(shardId, "failed to get index commit from searcher", e);
            }
            if (indexCommit == null) {
                assert false : "searcher from search engine should have index commit";
                throw new EngineException(shardId, "searcher from search engine should have index commit");
            }
            var indexCommitRef = new IndexCommitRef(indexCommit, searcher::close);
            searcher = null;
            return indexCommitRef;
        } finally {
            Releasables.close(searcher);
        }
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return null;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        var segmentInfos = segmentInfosAndCommit.segmentInfos();
        var sequenceNumbersCommitInfo = getSequenceNumbersCommitInfo(segmentInfos);
        return new SafeCommitInfo(sequenceNumbersCommitInfo.localCheckpoint(), segmentInfos.totalMaxDoc());
    }

    @Override
    public void activateThrottling() {

    }

    @Override
    public void deactivateThrottling() {

    }

    @Override
    public void suspendThrottling() {

    }

    @Override
    public void resumeThrottling() {

    }

    @Override
    public int restoreLocalHistoryFromTranslog(TranslogRecoveryRunner translogRecoveryRunner) throws IOException {
        return 0;
    }

    @Override
    public int fillSeqNoGaps(long primaryTerm) throws IOException {
        return 0;
    }

    @Override
    public void recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner, long recoverUpToSeqNo, ActionListener<Void> listener) {
        listener.onResponse(null);
    }

    @Override
    public void skipTranslogRecovery() {

    }

    @Override
    public void maybePruneDeletes() {

    }

    @Override
    public void updateMaxUnsafeAutoIdTimestamp(long newTimestamp) {

    }

    @Override
    public boolean refreshNeeded() {
        return false;
    }

    @Override
    public ShardLongFieldRange getRawFieldRange(String field) throws IOException {
        return ShardLongFieldRange.UNKNOWN;
    }

    @Override
    public void addPrimaryTermAndGenerationListener(long minPrimaryTerm, long minGeneration, ActionListener<Long> listener) {
        addOrExecuteSegmentGenerationListener(new PrimaryTermAndGeneration(minPrimaryTerm, minGeneration), listener);
    }

    public void afterRecovery() throws IOException {
        // Wait-for-checkpoint search requests rely on adding a refresh listener. RefreshListeners.addOrNotify() depends on
        // checking the lastRefreshedCheckpoint, which unfortunately is not set on a new search shard. For this reason, we
        // refresh the reader manager for a new search shard to ensure the lastRefreshedCheckpoint is updated.
        final var engineReadLock = engineConfig.getEngineResetLock().readLock();
        engineReadLock.lock();
        try {
            readerManager.maybeRefreshBlocking();
        } finally {
            engineReadLock.unlock();
        }
    }

    /**
     * Registers a segment generation listener or completes it immediately. Listeners are registered for a specific {@code minGeneration}
     * value and are completed once the shard is refreshed with a segment commit generation that is greater than or equal to that value. If
     * the shard is already on a newer segment generation the listener is completed immediately and the method returns false. Otherwise the
     * listener is kept around for future completion and the method returns true.
     *
     * @param minPrimaryTermGeneration the minimum primary term and segment generation to listen to
     * @param listener                 the listener
     * @return true if the listener has been registered successfully, false if the listener has been executed immediately
     *
     * @throws AlreadyClosedException if the engine is closed
     */
    boolean addOrExecuteSegmentGenerationListener(PrimaryTermAndGeneration minPrimaryTermGeneration, ActionListener<Long> listener) {
        try {
            ensureOpen();
            // check current state first - not strictly necessary, but a little more efficient than what happens next
            final PrimaryTermAndGeneration preFlightTermGeneration = getCurrentPrimaryTermAndGeneration();
            if (preFlightTermGeneration.compareTo(minPrimaryTermGeneration) >= 0) {
                listener.onResponse(preFlightTermGeneration.generation());
                return false;
            }

            // register this listener before checking current state again
            segmentGenerationListeners.computeIfAbsent(minPrimaryTermGeneration, ignored -> new SubscribableListener<>())
                .addListener(listener);

            // If the engine closed while we were registering, failSegmentGenerationListeners() may have
            // already drained the map before our computeIfAbsent added this entry. We must re-check and clean up ourselves.
            if (isClosed.get()) {
                var strandedListeners = segmentGenerationListeners.remove(minPrimaryTermGeneration);
                if (strandedListeners != null) {
                    strandedListeners.onFailure(new AlreadyClosedException(shardId + " engine is closed", failedEngine.get()));
                }
                return false;
            }

            // current state may have moved forwards in the meantime, in which case we must undo what we just did
            final PrimaryTermAndGeneration currentTermGeneration = getCurrentPrimaryTermAndGeneration();
            if (currentTermGeneration.compareTo(minPrimaryTermGeneration) >= 0) {
                final var listeners = segmentGenerationListeners.remove(minPrimaryTermGeneration);
                if (listeners != null) {
                    listeners.onResponse(currentTermGeneration.generation());
                } // else someone else executed it for us.
                return false;
            }
            return true;
        } catch (Exception e) {
            listener.onFailure(e);
            return false;
        }
    }

    private long primaryTerm(SegmentInfos segmentInfos) throws FileNotFoundException {
        return primaryTerm(segmentInfos.getSegmentsFileName());
    }

    private long primaryTerm(IndexCommit indexCommit) throws FileNotFoundException {
        return primaryTerm(indexCommit.getSegmentsFileName());
    }

    private long primaryTerm(String segmentsFileName) throws FileNotFoundException {
        return searchDirectory.getPrimaryTerm(segmentsFileName).orElse(UNKNOWN_PRIMARY_TERM);
    }

    private void callSegmentGenerationListeners(PrimaryTermAndGeneration currentTermGen) {
        final var iterator = segmentGenerationListeners.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getKey().compareTo(currentTermGen) <= 0) {
                iterator.remove();
                try {
                    entry.getValue().onResponse(currentTermGen.generation());
                } catch (Exception e) {
                    logger.warn(() -> "segment generation listener [" + entry.getKey() + "] failed", e);
                    assert false : e;
                }
            }
        }
    }

    private void failSegmentGenerationListeners() {
        assert isClosed.get();
        final var iterator = segmentGenerationListeners.entrySet().iterator();
        final AlreadyClosedException e = new AlreadyClosedException(shardId + " engine is closed", failedEngine.get());
        while (iterator.hasNext()) {
            var entry = iterator.next();
            iterator.remove();
            try {
                entry.getValue().onFailure(e);
            } catch (Exception e2) {
                e2.addSuppressed(e);
                logger.warn(() -> "segment generation listener [" + entry.getKey() + "] failed", e2);
                assert false : e2;
            }
        }
    }

    private boolean assertCurrentPrimaryTermGeneration(
        SegmentInfosAndCommit segmentInfosAndCommit,
        PrimaryTermAndGeneration currentPrimaryTermGeneration
    ) throws IOException {
        final var segmentInfos = segmentInfosAndCommit.segmentInfos();
        final var compoundCommit = segmentInfosAndCommit.statelessCompoundCommit();
        final var segmentIsHollow = compoundCommit == null ? false : compoundCommit.hollow();
        final var segmentPrimaryTerm = primaryTerm(segmentInfos);
        final var segmentGeneration = segmentInfos.getGeneration();

        if (segmentIsHollow == false) {
            assert currentPrimaryTermGeneration.primaryTerm() == segmentPrimaryTerm
                : Strings.format(
                    "current primary term [%d] must be == segment primary term [%d]",
                    currentPrimaryTermGeneration.primaryTerm(),
                    segmentPrimaryTerm
                );
        } else {
            assert currentPrimaryTermGeneration.primaryTerm() >= segmentPrimaryTerm
                : Strings.format(
                    "current primary term [%d] must be >= segment primary term [%d]",
                    currentPrimaryTermGeneration.primaryTerm(),
                    segmentPrimaryTerm
                );
        }

        return true;
    }

    private static UnsupportedOperationException unsupportedException() {
        assert false : "this operation is not supported and should have not be called";
        return new UnsupportedOperationException("Search engine does not support this operation");
    }

    public void acquireSearcherForCommit(
        String segmentsFileName,
        Map<String, BlobLocation> metadata,
        Function<Searcher, Searcher> wrapper,
        IndexReshardingMetadata relocatedReshardingMetadata,
        SplitShardCountSummary relocatedSplitShardCountSummary,
        ActionListener<SearcherSupplier> listener
    ) {
        // we use a task queue to serialize opening readers for old commits with processing commit notifications
        processCommitTaskRunner.enqueueTask(listener.map(releasable -> {
            Closeable currentReaderRef = null;
            Closeable storeRef = null;
            Closeable relocatedPitReaderRef = null;
            try (releasable) {
                ensureOpen();
                // Ensure that the store is not closed while opening the PIT searchers, since this runs async and we don't want to
                // leak open readers in that case.
                if (store.isClosing() || store.tryIncRef() == false) {
                    throw new AlreadyClosedException("Unable to acquire searcher for commit [" + segmentsFileName + "] for PIT reader");
                }
                storeRef = store::decRef;

                // Acquire the current reader so we can use it as the base for openIfChanged below.
                // If openIfChanged returns a new reader, this reference is released in the finally block.
                // If openIfChanged returns null (PIT is on the same commit), currentReaderRef is set to
                // null so the finally block skips the release — the reader stays alive as relocatedPitReader.
                ElasticsearchDirectoryReader currentReader = readerManager.acquire();
                currentReaderRef = () -> readerManager.release(currentReader);

                // merging metadata is necessary to allow opening an old commit from SearchDirectory
                // TODO: transfer replicated headers/footers and pre-warm to speed up recoveries
                searchDirectory.mergePITReaderMetadata(metadata);
                // The current reader directory has a reference to the store directory (directory variable)
                // and it does a reference comparison to see if the directory is the same. Therefore we have
                // to use that directory to get the index commit;
                SegmentInfos segmentCommitInfos = SegmentInfos.readCommit(
                    directory,
                    segmentsFileName,
                    IndexVersions.MINIMUM_READONLY_COMPATIBLE.luceneVersion().major
                );
                IndexCommit indexCommit = Lucene.getIndexCommit(segmentCommitInfos, directory);
                // Use openIfChanged rather than directly opening the commit so that Lucene can reuse
                // any segment core readers that are shared with the current reader. This avoids
                // redundant I/O against the blob store and, crucially, ensures that all reader
                // wrappers (e.g. SoftDeletesDirectoryReaderWrapper) installed by the existing Lucene
                // infrastructure are inherited — otherwise soft-deleted documents would incorrectly
                // resurface in PIT results.
                DirectoryReader pitReader = DirectoryReader.openIfChanged(currentReader, indexCommit);
                // If the PIT is referencing the latest commit, we just need to keep the reference that we acquired
                if (pitReader == null) {
                    // If the relocated PIT references the same commit as the current reader,
                    // we just transfer the reference that we acquired to the relocated PIT.
                    currentReaderRef = null;
                    pitReader = currentReader;
                }
                assert pitReader instanceof ElasticsearchDirectoryReader;

                ElasticsearchDirectoryReader relocatedPitReader = (ElasticsearchDirectoryReader) pitReader;
                var pitReaderManager = wrapForAssertions(new ElasticsearchReaderManager(relocatedPitReader), config());
                relocatedPitReaderRef = () -> pitReaderManager.release(relocatedPitReader);
                Set<PrimaryTermAndGeneration> bccDeps = new HashSet<>();
                for (BlobLocation blobLocation : metadata.values()) {
                    bccDeps.add(blobLocation.getBatchedCompoundCommitTermAndGeneration());
                }
                // Account the PIT-relocated reader against the node's reader-heap budget so its segments
                // participate in reservation tracking and metrics; uses the no-break path because relocation
                // must always succeed. trackLocalOpenReader and registerReaderHeapRelease both attach close
                // listeners and can throw before doing so; if either throws, the surrounding finally only closes
                // the reader, which is not enough to release the ledger reservation here — we must do it ourselves.
                final SegmentReservations.Reservation pitReservation = reserveAndChargeWithoutBreaking(segmentCommitInfos);
                boolean pitReservationOwned = false;
                try {
                    trackLocalOpenReader(relocatedPitReader, indexCommit, pitReservation, Collections.unmodifiableSet(bccDeps));
                    registerReaderHeapRelease(relocatedPitReader, pitReservation);
                    pitReservationOwned = true;
                } finally {
                    if (pitReservationOwned == false) {
                        releaseReservationAndUncharge(pitReservation);
                    }
                }
                // Register the relocated PIT reader with relocatedPITReaderTracker so it is closed
                // when the engine closes, even if the returned SearcherSupplier is never used (e.g.
                // because the shard closes before the PIT context is registered with SearchService).
                var searcherSupplier = relocatedPITReaderTracker.addRelocatedPitReader(
                    new RelocatedPITReader(
                        pitReaderManager,
                        wrapper,
                        relocatedReshardingMetadata,
                        relocatedSplitShardCountSummary,
                        store::decRef
                    )
                );
                // From now on, the relocated PIT reader is owned by the relocatedPITReaderTracker
                storeRef = null;
                relocatedPitReaderRef = null;
                return searcherSupplier;
            } finally {
                IOUtils.closeWhileHandlingException(currentReaderRef, relocatedPitReaderRef, storeRef);
            }
        }));
    }

    /**
     * Tracks a directory reader currently alive on this node. {@code reservation} owns the segment keys this reader
     * holds — sufficient for a future reclamation policy to answer "how many bytes would I free by closing this
     * reader?" by consulting {@link SegmentReservations}, without retaining the full {@link SegmentInfos}.
     */
    private record OpenReaderInfo(
        Collection<String> files,
        SegmentReservations.Reservation reservation,
        Set<PrimaryTermAndGeneration> referencedBCCs
    ) {

        private OpenReaderInfo(
            Collection<String> files,
            SegmentReservations.Reservation reservation,
            Set<PrimaryTermAndGeneration> referencedBCCs
        ) {
            this.files = Set.copyOf(files);
            this.reservation = reservation;
            this.referencedBCCs = referencedBCCs;
        }
    }

    private record SegmentInfosAndCommit(SegmentInfos segmentInfos, StatelessCompoundCommit statelessCompoundCommit) {

        private SegmentInfosAndCommit {
            assert statelessCompoundCommit == null || segmentInfos.getGeneration() == statelessCompoundCommit.generation()
                : "segment generation ["
                    + segmentInfos.getGeneration()
                    + "] does not match CC generation ["
                    + statelessCompoundCommit.generation()
                    + ']';
        }

        Set<PrimaryTermAndGeneration> getBCCDependenciesForCommit() {
            return statelessCompoundCommit == null
                ? Set.of()
                : BatchedCompoundCommit.computeReferencedBCCGenerations(statelessCompoundCommit);
        }

        long getGeneration() {
            return segmentInfos.getGeneration();
        }
    }

    private static class RelocatedPITReaderTracker implements Closeable {

        @FunctionalInterface
        interface SearcherSupplierFactory {
            SearcherSupplier create(RelocatedPITReader relocatedPITReader);
        }

        private final SearcherSupplierFactory searcherSupplierFactory;
        private final Set<RelocatedPITReader> trackedReaders = new HashSet<>();
        private boolean closed = false;

        RelocatedPITReaderTracker(SearcherSupplierFactory searcherSupplierFactory) {
            this.searcherSupplierFactory = searcherSupplierFactory;
        }

        synchronized SearcherSupplier addRelocatedPitReader(RelocatedPITReader relocatedPITReader) {
            ensureOpen();
            trackedReaders.add(relocatedPITReader);
            return new SearcherSupplier(Function.identity()) {
                // The delegate is lazily initialised because we don't want to acquire a searcher
                // from the reader manager while this SearcherSupplier is held in PITRelocationService
                // waiting for the shard to transition to STARTED. If the shard is never started
                // (e.g. recovery fails), no searcher is ever acquired.
                private SearcherSupplier delegate = null;
                private boolean closed = false;

                @Override
                protected synchronized void doClose() {
                    closed = true;
                    IOUtils.closeWhileHandlingException(delegate);
                }

                @Override
                protected synchronized Searcher acquireSearcherInternal(String source) {
                    if (closed) {
                        throw new AlreadyClosedException("SearcherSupplier was closed");
                    }

                    if (delegate == null) {
                        delegate = doAcquireSearchSupplierForPITReader(relocatedPITReader);
                    }

                    return delegate.acquireSearcher(source);
                }
            };
        }

        private synchronized SearcherSupplier doAcquireSearchSupplierForPITReader(RelocatedPITReader relocatedPITReader) {
            ensureOpen();
            var removed = trackedReaders.remove(relocatedPITReader);
            assert removed : "Expected to find relocated PIT reader [" + relocatedPITReader + "] in trackedReaders";
            var pitReaderManager = relocatedPITReader.pitReaderManager();
            var storeRef = relocatedPITReader.storeRef();

            try (storeRef) {
                SearcherSupplier delegate = searcherSupplierFactory.create(relocatedPITReader);
                // searcherSupplierFactory.create() acquires its own store ref for the delegate's
                // lifetime, so we can release ours (via the try-with-resources block) here.
                return new SearcherSupplier(Function.identity()) {
                    @Override
                    protected void doClose() {
                        // We need to close the readerManager before the delegate searcher supplier because it
                        // holds the directory reader that holds the file handles to the commit's files.
                        // We need to ensure those are released before closing the delegate searcher supplier,
                        // which might close the store and thereby the underlying directory if it is the thing using it.
                        IOUtils.closeWhileHandlingException(pitReaderManager, delegate);
                    }

                    @Override
                    protected Searcher acquireSearcherInternal(String source) {
                        return delegate.acquireSearcher(source);
                    }
                };
            } catch (Exception e) {
                IOUtils.closeWhileHandlingException(pitReaderManager);
                throw e;
            }
        }

        private void ensureOpen() {
            assert Thread.holdsLock(this);
            if (closed) {
                throw new AlreadyClosedException("RelocatedPITReaderTracker is closed");
            }
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            IOUtils.closeWhileHandlingException(trackedReaders);
        }
    }

    private record RelocatedPITReader(
        ElasticsearchReaderManager pitReaderManager,
        Function<Searcher, Searcher> wrapper,
        IndexReshardingMetadata reshardingMetadata,
        SplitShardCountSummary splitShardCountSummary,
        Releasable storeRef
    ) implements Closeable {
        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(pitReaderManager, storeRef);
        }
    }

}
