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
 */

package co.elastic.elasticsearch.stateless.engine;

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
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.IOUtils;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
    private static final long SEARCH_IDLE_TIME = TimeUnit.SECONDS.toMillis(30L);

    private final Map<PrimaryTermAndGeneration, SubscribableListener<Long>> segmentGenerationListeners = ConcurrentCollections
        .newConcurrentMap();
    private final LinkedBlockingQueue<StatelessCompoundCommit> commitNotifications = new LinkedBlockingQueue<>();
    private final AtomicInteger pendingCommitNotifications = new AtomicInteger();
    private final ReferenceManager<ElasticsearchDirectoryReader> readerManager;
    private final SearchDirectory directory;

    private volatile SegmentInfos segmentInfos;
    private volatile PrimaryTermAndGeneration currentPrimaryTermGeneration;
    private volatile long maxSequenceNumber = SequenceNumbers.NO_OPS_PERFORMED;
    private volatile long processedLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;

    private final Map<DirectoryReader, OpenReaderInfo> openReaders = new ConcurrentHashMap<>();

    public SearchEngine(EngineConfig config) {
        super(config);
        assert config.isPromotableToPrimary() == false;

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
            // do not consider the empty commit an open reader (no data to delete from object store)
            if (primaryTerm.isPresent()) {
                trackOpenReader(directoryReader, primaryTerm.getAsLong(), initialCommit.getGeneration(), initialCommit);
            }
            readerManager = new ElasticsearchReaderManager(directoryReader) {
                private SegmentInfos previousSegmentInfos;

                @Override
                protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
                    SegmentInfos segmentInfosCopy = segmentInfos;
                    if (segmentInfosCopy == previousSegmentInfos) {
                        return null;
                    }
                    final IndexCommit indexCommit = Lucene.getIndexCommit(segmentInfosCopy, directory);
                    ElasticsearchDirectoryReader next = (ElasticsearchDirectoryReader) DirectoryReader.openIfChanged(
                        referenceToRefresh,
                        indexCommit
                    );
                    if (next != null) {
                        addNextReader(next, segmentInfosCopy, indexCommit);
                    }
                    previousSegmentInfos = segmentInfosCopy;
                    return next;
                }

                private void addNextReader(ElasticsearchDirectoryReader next, SegmentInfos segmentInfosCopy, IndexCommit first)
                    throws IOException {
                    boolean added = false;
                    try {
                        trackOpenReader(
                            next,
                            directory.getPrimaryTerm(segmentInfosCopy.getSegmentsFileName()).getAsLong(),
                            segmentInfosCopy.getGeneration(),
                            first
                        );
                        added = true;
                    } finally {
                        if (added == false) {
                            IOUtils.closeWhileHandlingException(next);
                        }
                    }
                }
            };
            this.segmentInfos = store.readLastCommittedSegmentsInfo();
            this.currentPrimaryTermGeneration = new PrimaryTermAndGeneration(primaryTerm(segmentInfos), segmentInfos.getGeneration());
            this.setSequenceNumbers(segmentInfos);
            this.readerManager = readerManager;
            for (ReferenceManager.RefreshListener refreshListener : config.getExternalRefreshListener()) {
                readerManager.addListener(refreshListener);
            }
            success = true;
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "Failed to create a search engine", e);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(readerManager, directoryReader, store::decRef);
            }
        }
    }

    private void trackOpenReader(ElasticsearchDirectoryReader directoryReader, long primaryTerm, long generation, IndexCommit commit)
        throws IOException {
        ElasticsearchDirectoryReader.addReaderCloseListener(directoryReader, ignored -> openReaders.remove(directoryReader));
        openReaders.put(directoryReader, new OpenReaderInfo(new PrimaryTermAndGeneration(primaryTerm, generation), commit.getFileNames()));
    }

    PrimaryTermAndGeneration getCurrentPrimaryTermAndGeneration() {
        assert this.currentPrimaryTermGeneration.generation() > 0 : currentPrimaryTermGeneration;
        return this.currentPrimaryTermGeneration;
    }

    // visible for testing
    long getPendingCommitNotifications() {
        return pendingCommitNotifications.get();
    }

    public Set<PrimaryTermAndGeneration> getAcquiredPrimaryTermAndGenerations() {
        return openReaders.values().stream().map(OpenReaderInfo::primaryTermAndGeneration).collect(Collectors.toSet());
    }

    /**
     * Process a new commit notification from the primary, and complete the provided {@code listener} when this commit (or a later commit)
     * is visible to searches.
     */
    public void onCommitNotification(StatelessCompoundCommit commit, ActionListener<Void> listener) {
        if (addOrExecuteSegmentGenerationListener(commit.primaryTermAndGeneration(), listener.map(g -> null))) {
            commitNotifications.add(commit);
            if (pendingCommitNotifications.incrementAndGet() == 1) {
                processCommitNotifications();
            }
        }
    }

    private volatile long lastSearcherAcquiredTime;

    private void processCommitNotifications() {
        var refreshExecutor = engineConfig.getThreadPool().executor(ThreadPool.Names.REFRESH);
        refreshExecutor.execute(new AbstractRunnable() {

            int batchSize = 0;
            boolean success = false;

            @Override
            protected void doRun() throws Exception {
                ensureOpen();
                batchSize = pendingCommitNotifications.get();
                assert batchSize > 0 : batchSize;

                final SegmentInfos current = segmentInfos;
                assert current.getGeneration() == currentPrimaryTermGeneration.generation()
                    : "segment info generation ["
                        + current.getGeneration()
                        + "] not in sync with current generation ["
                        + currentPrimaryTermGeneration
                        + "]";
                assert primaryTerm(current) == currentPrimaryTermGeneration.primaryTerm()
                    : Strings.format(
                        "segment info primary term [%d] not in sync with current primary term [%s]",
                        primaryTerm(current),
                        currentPrimaryTermGeneration
                    );
                StatelessCompoundCommit latestCommit = findLatestCommit(current);
                if (latestCommit == null) {
                    logger.trace("directory is on most recent commit generation [{}]", current.getGeneration());
                    // TODO should we assert that we have no segment listeners with minGen <= current.getGeneration()?
                    return;
                }
                if (directory.isMarkedAsCorrupted()) {
                    logger.trace("directory is marked as corrupted, ignoring all future commit notifications");
                    failSegmentGenerationListeners();
                    return;
                }

                store.incRef();
                try {
                    logger.trace("updating directory with commit {}", latestCommit);
                    if (directory.updateCommit(latestCommit)) {
                        // if this index has been recently searched, and this commit hasn't been superseded, then
                        // prefetch the new commit files
                        // todo: disabled pre-fetching new commits, see https://github.com/elastic/elasticsearch-serverless/issues/1006
                        if (false && engineConfig.getThreadPool().relativeTimeInMillis() - lastSearcherAcquiredTime < SEARCH_IDLE_TIME) {
                            directory.downloadCommit(
                                latestCommit,
                                new ThreadedActionListener<>(
                                    refreshExecutor,
                                    ActionListener.running(() -> updateInternalState(latestCommit, current))
                                )
                            );
                        } else {
                            updateInternalState(latestCommit, current);
                        }
                    }
                    success = true;
                } finally {
                    // store is released by #updateInternalState unless there was an exception
                    if (success == false) {
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
                if (success == false && isClosed.get() == false) {
                    doAfter();
                }
            }

            private void doAfter() {
                var remaining = pendingCommitNotifications.addAndGet(-batchSize);
                assert remaining >= 0 : remaining;
                if (remaining > 0) {
                    processCommitNotifications();
                }
            }

            private StatelessCompoundCommit findLatestCommit(SegmentInfos current) throws IOException {
                PrimaryTermAndGeneration currentPrimaryTermGeneration = new PrimaryTermAndGeneration(
                    primaryTerm(current),
                    current.getGeneration()
                );
                StatelessCompoundCommit latestCommit = null;
                for (int i = batchSize; i > 0; i--) {
                    StatelessCompoundCommit commit = commitNotifications.poll();
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
                    if (latestCommit == null || commit.primaryTermAndGeneration().compareTo(latestCommit.primaryTermAndGeneration()) > 0) {
                        latestCommit = commit;
                    }
                }
                return latestCommit;
            }

            private void updateInternalState(StatelessCompoundCommit latestCommit, SegmentInfos current) {
                try {
                    doUpdateInternalState(latestCommit, current);
                } catch (Exception e) {
                    onFailure(e);
                } finally {
                    Releasables.close(store::decRef, this::doAfter);
                }
            }

            private void doUpdateInternalState(StatelessCompoundCommit latestCommit, SegmentInfos current) throws IOException {
                final SegmentInfos next = Lucene.readSegmentInfos(directory);
                setSequenceNumbers(next);

                segmentInfos = next;

                readerManager.maybeRefreshBlocking();

                // must be after refresh for `addOrExecuteSegmentGenerationListener to work.
                currentPrimaryTermGeneration = new PrimaryTermAndGeneration(primaryTerm(segmentInfos), segmentInfos.getGeneration());

                var reader = readerManager.acquire();
                try {
                    assert assertSegmentInfosAndCommits(reader, latestCommit, current, next);
                    directory.retainFiles(
                        openReaders.values().stream().flatMap(openReaderInfo -> openReaderInfo.files().stream()).collect(Collectors.toSet())
                    );
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
        assert commit.maxSeqNo >= maxSequenceNumber
            : "Commit [" + commit + "] max sequence number less than tracked max sequence number [" + maxSequenceNumber + "]";
        maxSequenceNumber = commit.maxSeqNo;
        assert commit.localCheckpoint >= processedLocalCheckpoint
            : "Commit [" + commit + "] local checkpoint less than tracked local checkpoint [" + processedLocalCheckpoint + "]";
        processedLocalCheckpoint = commit.localCheckpoint;
    }

    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        if (isClosed.compareAndSet(false, true)) {
            try {
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
        return segmentInfos;
    }

    @Override
    public String getHistoryUUID() {
        return segmentInfos.getUserData().get(Engine.HISTORY_UUID_KEY);
    }

    @Override
    public Translog.Location getTranslogLastWriteLocation() {
        return new Translog.Location(0L, 0L, 0);
    }

    private SequenceNumbers.CommitInfo getSequenceNumbersCommitInfo() {
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
        return processedLocalCheckpoint;
    }

    @Override
    public long getWritingBytes() {
        return 0;
    }

    @Override
    public CompletionStats completionStats(String... fieldNamePatterns) {
        return new CompletionStatsCache(() -> acquireSearcher("completion_stats")).get(fieldNamePatterns);
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
    public SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper, SearcherScope scope) throws EngineException {
        lastSearcherAcquiredTime = engineConfig.getThreadPool().relativeTimeInMillis();
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
        boolean accessStats
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
        var commitInfo = getSequenceNumbersCommitInfo();
        return new SeqNoStats(commitInfo.maxSeqNo, commitInfo.localCheckpoint, config().getGlobalCheckpointSupplier().getAsLong());
    }

    @Override
    public long getIndexBufferRAMBytesUsed() {
        return 0;
    }

    @Override
    public List<Segment> segments() {
        ensureOpen();
        final SegmentInfos current = this.segmentInfos;
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
        listener.onResponse(FlushResult.NO_FLUSH);
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
        return null;
    }

    @Override
    public IndexCommitRef acquireSafeIndexCommit() throws EngineException {
        return null;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return new SafeCommitInfo(getSequenceNumbersCommitInfo().localCheckpoint, segmentInfos.totalMaxDoc());
    }

    @Override
    public void activateThrottling() {

    }

    @Override
    public void deactivateThrottling() {

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

    private static UnsupportedOperationException unsupportedException() {
        assert false : "this operation is not supported and should have not be called";
        return new UnsupportedOperationException("Search engine does not support this operation");
    }

    private record OpenReaderInfo(PrimaryTermAndGeneration primaryTermAndGeneration, Collection<String> files) {

        private OpenReaderInfo(PrimaryTermAndGeneration primaryTermAndGeneration, Collection<String> files) {
            this.primaryTermAndGeneration = primaryTermAndGeneration;
            this.files = Set.copyOf(files);
        }
    }
}
