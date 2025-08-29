/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.cache.SearchCommitPrefetcher;
import co.elastic.elasticsearch.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.ClosedShardService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link Engine} implementation for search shards
 *
 * This class implements the minimal behavior to allow a stateless search shard to recover as a replica of an index/primary shard. Most of
 * indexing behavior is faked and will be removed once operations are not replicated anymore (ES-4861).
 *
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
    private final SearchDirectory directory;
    private final CompletionStatsCache completionStatsCache;
    private final SearchCommitPrefetcher commitPrefetcher;
    private final SearchCommitPrefetcherDynamicSettings prefetcherDynamicSettings;

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

    @SuppressWarnings("this-escape")
    public SearchEngine(
        EngineConfig config,
        ClosedShardService closedShardService,
        StatelessSharedBlobCacheService statelessSharedBlobCacheService,
        ClusterSettings clusterSettings,
        Executor prefetchExecutor,
        SearchCommitPrefetcherDynamicSettings prefetcherDynamicSettings
    ) {
        super(config);
        assert config.isPromotableToPrimary() == false;
        this.closedShardService = closedShardService;

        ElasticsearchDirectoryReader directoryReader = null;
        ElasticsearchReaderManager readerManager = null;
        boolean success = false;
        store.incRef();
        try {
            this.directory = SearchDirectory.unwrapDirectory(store.directory());
            directoryReader = ElasticsearchDirectoryReader.wrap(
                new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(directory, config.getLeafSorter()), Lucene.SOFT_DELETES_FIELD),
                shardId
            );
            IndexCommit initialCommit = directoryReader.getIndexCommit();
            OptionalLong primaryTerm = directory.getPrimaryTerm(initialCommit.getSegmentsFileName());
            var initialSegmentInfosAndCommit = new SegmentInfosAndCommit(
                store.readLastCommittedSegmentsInfo(),
                directory.getCurrentCommit()
            );

            // do not consider the empty commit an open reader (no data to delete from object store)
            if (primaryTerm.isPresent()) {
                trackLocalOpenReader(directoryReader, initialCommit, initialSegmentInfosAndCommit.getBCCDependenciesForCommit());
            }
            readerManager = new ElasticsearchReaderManager(directoryReader) {
                private SegmentInfosAndCommit previousSegmentInfosAndCommit;

                @Override
                protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
                    SegmentInfosAndCommit segmentInfosAndCommitCopy = segmentInfosAndCommit;
                    if (segmentInfosAndCommitCopy == previousSegmentInfosAndCommit) {
                        return null;
                    }
                    final IndexCommit indexCommit = Lucene.getIndexCommit(segmentInfosAndCommitCopy.segmentInfos(), directory);
                    ElasticsearchDirectoryReader next = (ElasticsearchDirectoryReader) DirectoryReader.openIfChanged(
                        referenceToRefresh,
                        indexCommit
                    );
                    if (next != null) {
                        addNextReader(next, segmentInfosAndCommitCopy, indexCommit);
                    }
                    previousSegmentInfosAndCommit = segmentInfosAndCommitCopy;
                    return next;
                }

                private void addNextReader(
                    ElasticsearchDirectoryReader next,
                    SegmentInfosAndCommit segmentInfosAndCommitCopy,
                    IndexCommit first
                ) throws IOException {
                    boolean added = false;
                    try {
                        trackLocalOpenReader(next, first, segmentInfosAndCommitCopy.getBCCDependenciesForCommit());
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
            this.prefetcherDynamicSettings = prefetcherDynamicSettings;
            this.commitPrefetcher = new SearchCommitPrefetcher(
                directory.getShardId(),
                statelessSharedBlobCacheService,
                directory::getCacheBlobReaderForPreFetching,
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

    private void trackLocalOpenReader(
        ElasticsearchDirectoryReader directoryReader,
        IndexCommit commit,
        Set<PrimaryTermAndGeneration> bccDependencies
    ) throws IOException {
        ElasticsearchDirectoryReader.addReaderCloseListener(directoryReader, ignored -> {
            synchronized (openReaders) {
                openReaders.remove(directoryReader);
            }
        });

        synchronized (openReaders) {
            openReaders.put(directoryReader, new OpenReaderInfo(commit.getFileNames(), bccDependencies));
        }
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
        final var termAndGens = new HashSet<>(directory.getAcquiredGenerationalFileTermAndGenerations());
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
        directory.updateLatestUploadedBcc(notification.latestUploadedBatchedCompoundCommitTermAndGen());
        directory.updateLatestCommitInfo(ccTermAndGen, notification.nodeId());

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
        var refreshExecutor = engineConfig.getThreadPool().executor(ThreadPool.Names.REFRESH);
        refreshExecutor.execute(new AbstractRunnable() {

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
                if (directory.isMarkedAsCorrupted()) {
                    logger.trace("directory is marked as corrupted, ignoring all future commit notifications");
                    failSegmentGenerationListeners();
                    return;
                }

                logger.trace("updating directory with commit {}", latestCommit);
                if (directory.updateCommit(latestCommit)) {
                    store.incRef();
                    try {
                        updateInternalState(latestCommit, current);
                    } finally {
                        store.decRef();
                    }
                }
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

            private void updateInternalState(StatelessCompoundCommit latestCommit, SegmentInfos current) {
                try {
                    doUpdateInternalState(latestCommit, current);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            private void doUpdateInternalState(StatelessCompoundCommit latestCommit, SegmentInfos current) throws IOException {
                final SegmentInfos next = Lucene.readSegmentInfos(directory);
                setSequenceNumbers(next);

                assert next.getGeneration() == latestCommit.generation();
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

                // must be after refresh for `addOrExecuteSegmentGenerationListener to work.
                currentPrimaryTermGeneration = new PrimaryTermAndGeneration(
                    primaryTerm(segmentInfosAndCommit.segmentInfos()),
                    segmentInfosAndCommit.getGeneration()
                );
                assert assertCurrentPrimaryTermGeneration(segmentInfosAndCommit, currentPrimaryTermGeneration);

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
                    directory.retainFiles(filesToRetain);
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
                        : "Directory reader commit generation ["
                            + readerCommit.getGeneration()
                            + "] does not match expected generation ["
                            + latestCommit.generation()
                            + ']';

                    assert currentSegmentInfos.getGeneration() < nextSegmentInfos.getGeneration()
                        : "SegmentInfos generation ["
                            + nextSegmentInfos.getGeneration()
                            + "] must be higher than previous generation ["
                            + currentSegmentInfos.getGeneration()
                            + ']';

                    assert primaryTerm(readerCommit) == latestCommit.primaryTerm()
                        : Strings.format(
                            "Directory reader primary term=%d doesn't match latest commit primary term=%d",
                            primaryTerm(readerCommit),
                            latestCommit.primaryTerm()
                        );
                    assert primaryTerm(readerCommit) != Engine.UNKNOWN_PRIMARY_TERM : "Directory reader primary term is not known";
                    assert primaryTerm(nextSegmentInfos) != Engine.UNKNOWN_PRIMARY_TERM : "SegmentInfos primary term is not known";
                    assert primaryTerm(readerCommit) == primaryTerm(nextSegmentInfos)
                        : Strings.format(
                            "Directory reader primary term=%d doesn't match latest SegmentInfos primary term=%d",
                            primaryTerm(readerCommit),
                            primaryTerm(nextSegmentInfos)
                        );
                } catch (IOException ioe) {
                    assert false : ioe;
                }
                return true;
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

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
                // Save any active reader information to the ClosedShardService BEFORE potentially closing the store. The ClosedShardService
                // is hooked into store closure, so we don't want to race with it!
                closedShardService.onShardClose(shardId, getAcquiredPrimaryTermAndGenerations());
                IOUtils.close(this::failSegmentGenerationListeners, readerManager, store::decRef);
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
        Function<Searcher, Searcher> searcherWrapper
    ) {
        return getFromSearcher(get, acquireSearcher("get", SearcherScope.EXTERNAL, searcherWrapper), false);
    }

    @Override
    protected void onSearcherCreation(String source, SearcherScope scope) {
        super.onSearcherCreation(source, scope);
        if (source.equals(CAN_MATCH_SEARCH_SOURCE) || source.equals(SEARCH_SOURCE)) {
            lastSearcherAcquiredTime = engineConfig.getThreadPool().relativeTimeInMillis();
        }
    }

    // visible for testing
    long getLastSearcherAcquiredTime() {
        return lastSearcherAcquiredTime;
    }

    @Override
    public SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper, SearcherScope scope) throws EngineException {
        return super.acquireSearcherSupplier(wrapper, scope);
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
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
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
     * @param listener the listener
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
        return directory.getPrimaryTerm(segmentsFileName).orElse(UNKNOWN_PRIMARY_TERM);
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
        assert segmentGenerationListeners.isEmpty();
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

    private record OpenReaderInfo(Collection<String> files, Set<PrimaryTermAndGeneration> referencedBCCs) {

        private OpenReaderInfo(Collection<String> files, Set<PrimaryTermAndGeneration> referencedBCCs) {
            this.files = Set.copyOf(files);
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
}
