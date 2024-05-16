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

import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.commits.CommitBCCResolver;
import co.elastic.elasticsearch.stateless.commits.IndexEngineLocalReaderListener;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.engine.ElasticsearchReaderManager;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineCreationFailureException;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.LiveVersionMapArchive;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.INDEX_FAST_REFRESH_SETTING;

/**
 * {@link Engine} implementation for index shards
 */
public class IndexEngine extends InternalEngine {

    public static final String TRANSLOG_RECOVERY_START_FILE = "translog_recovery_start_file";
    // A flag for whether the flush call is originated from a refresh
    private static final ThreadLocal<Boolean> IS_FLUSH_BY_REFRESH = ThreadLocal.withInitial(() -> false);

    private final TranslogReplicator translogReplicator;
    private final StatelessCommitService statelessCommitService;
    private final Function<String, BlobContainer> translogBlobContainer;
    private final boolean fastRefresh;
    private final RefreshThrottler refreshThrottler;
    private final IndexEngineLocalReaderListener localReaderListener;
    private final CommitBCCResolver commitBCCResolver;
    private final DocumentParsingProvider documentParsingProvider;
    // This is written and then accessed on the same thread under the flush lock. So not need for volatile
    private long translogStartFileForNextCommit = 0;

    // The values of this map are sets of BCCs referenced by the reader. This map is guarded by the openReaders monitor.
    private final Map<DirectoryReader, Set<PrimaryTermAndGeneration>> openReaders = new HashMap<>();

    private final AtomicBoolean ongoingFlushMustUpload = new AtomicBoolean(false);

    public IndexEngine(
        EngineConfig engineConfig,
        TranslogReplicator translogReplicator,
        Function<String, BlobContainer> translogBlobContainer,
        StatelessCommitService statelessCommitService,
        RefreshThrottler.Factory refreshThrottlerFactory,
        IndexEngineLocalReaderListener localReaderListener,
        CommitBCCResolver commitBCCResolver,
        DocumentParsingProvider documentParsingProvider
    ) {
        super(engineConfig);
        assert engineConfig.isPromotableToPrimary();
        this.translogReplicator = translogReplicator;
        this.translogBlobContainer = translogBlobContainer;
        this.statelessCommitService = statelessCommitService;
        this.fastRefresh = INDEX_FAST_REFRESH_SETTING.get(config().getIndexSettings().getSettings());
        this.refreshThrottler = refreshThrottlerFactory.create(this::doExternalRefresh);
        this.localReaderListener = localReaderListener;
        this.commitBCCResolver = commitBCCResolver;
        this.documentParsingProvider = documentParsingProvider;

        // We have to track the initial BCC references held by local readers at this point instead of doing it in
        // #createInternalReaderManager because that method is called from the super constructor and at that point,
        // commitBCCResolver field is not set yet.
        var referenceManager = getReferenceManager(SearcherScope.INTERNAL);
        try {
            ElasticsearchDirectoryReader directoryReader = referenceManager.acquire();
            try {
                trackOpenLocalReader(directoryReader);
            } finally {
                referenceManager.release(directoryReader);
            }
        } catch (IOException e) {
            throw new EngineCreationFailureException(engineConfig.getShardId(), "Failed to create an index engine", e);
        }
    }

    @Override
    protected LongConsumer translogPersistedSeqNoConsumer() {
        return seqNo -> {};
    }

    public LongConsumer objectStorePersistedSeqNoConsumer() {
        return seqNo -> {
            final LocalCheckpointTracker tracker = getLocalCheckpointTracker();
            if (tracker != null) {
                tracker.markSeqNoAsPersisted(seqNo);
            }
        };
    }

    @Override
    protected ElasticsearchReaderManager createInternalReaderManager(ElasticsearchDirectoryReader directoryReader) {
        return new ElasticsearchReaderManager(directoryReader) {
            @Override
            protected ElasticsearchDirectoryReader refreshIfNeeded(ElasticsearchDirectoryReader referenceToRefresh) throws IOException {
                ElasticsearchDirectoryReader next = super.refreshIfNeeded(referenceToRefresh);
                if (next == null) {
                    return null;
                }
                boolean success = false;
                try {
                    trackOpenLocalReader(next);
                    success = true;
                } finally {
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(next);
                    }
                }
                return next;
            }
        };
    }

    private void trackOpenLocalReader(ElasticsearchDirectoryReader directoryReader) {
        long generation = getLatestCommittedGeneration(directoryReader);

        var referencedBCCsForCommit = commitBCCResolver.resolveReferencedBCCsForCommit(generation);
        // If the set of referenced BCC commits is empty it means that the shard has been relocated or closed,
        // in that case we're not interested tracking this newly open reader for file deletions.
        if (referencedBCCsForCommit.isEmpty()) {
            return;
        }

        ElasticsearchDirectoryReader.addReaderCloseListener(directoryReader, ignored -> onLocalReaderClosed(directoryReader));
        synchronized (openReaders) {
            openReaders.put(directoryReader, referencedBCCsForCommit);
        }
    }

    private void onLocalReaderClosed(DirectoryReader reader) {
        Set<PrimaryTermAndGeneration> bccDependencies;
        Set<PrimaryTermAndGeneration> remainingReferencedBCCs;
        // CHM iterators are weakly consistent, meaning that we're not guaranteed to see new insertions while we compute
        // the set of remainingReferencedBCCs, that's why we use a regular HashMap with synchronized.
        synchronized (openReaders) {
            bccDependencies = openReaders.remove(reader);
            assert bccDependencies != null : openReaders + " -> " + reader;
            assert bccDependencies.isEmpty() == false;
            remainingReferencedBCCs = openReaders.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
        }

        long bccHoldingCommit = bccDependencies.stream().max(PrimaryTermAndGeneration::compareTo).get().generation();
        localReaderListener.onLocalReaderClosed(bccHoldingCommit, remainingReferencedBCCs);
    }

    static long getLatestCommittedGeneration(DirectoryReader directoryReader) {
        if (FilterDirectoryReader.unwrap(directoryReader) instanceof StandardDirectoryReader standardDirectoryReader) {
            // If there's a concurrent flush while the refresh is executed, the generation from
            // getIndexCommit().getGeneration() will point to the yet to be committed Lucene commit generation
            // therefore, we need to fetch the last generation that refers to the latest successful Lucene commit
            return standardDirectoryReader.getSegmentInfos().getLastGeneration();
        }

        try {
            assert false;
            return directoryReader.getIndexCommit().getGeneration();
        } catch (IOException e) {
            assert false;
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean refreshNeeded() {
        if (fastRefresh) {
            return super.refreshNeeded();
        } else {
            // It is possible that the index writer has uncommitted changes. We could check here, but we will check before actually
            // triggering the flush anyway.
            return hasUncommittedChanges() || super.refreshNeeded();
        }
    }

    @Override
    protected void flushHoldingLock(boolean force, boolean waitIfOngoing, ActionListener<FlushResult> listener) throws EngineException {
        // A regular flush, i.e. not converted from refresh, must trigger to a commit generation to be uploaded
        // (by increase maxGenerationToUploadDueToFlush).
        // We set ongoingFlushMustUpload to true so that if this thread does not flush on its own (because the
        // flush lock is held by another thread and this thread does not wait for it), some other concurrent
        // flushing thread will promise to do it.
        // This protocol does not care exactly which thread ends up doing the job. It could be any of the concurrent
        // flush threads including this one. The setMaxGenerationToUploadDueToFlush method will be called
        // exactly once to a generation processed by one of the threads.
        // If the flush thread errors before ongoingFlushMustUpload can be cleared, the next flush will handle it.
        // Note the behaviour is still same if we just check `IS_FLUSH_BY_REFRESH.get() == false`.
        // However this may in some cases trigger more than one uploads, e.g. another thread may see the flag and
        // trigger upload immediately while this thread creates a new commit and should also upload.
        if (IS_FLUSH_BY_REFRESH.get()) {
            logger.trace("flush-by-refresh for {}", shardId);
        } else {
            logger.trace("flush for {}", shardId);
            if (force == false && waitIfOngoing == false) {
                ongoingFlushMustUpload.set(true);
            }
        }
        super.flushHoldingLock(force, waitIfOngoing, listener);
    }

    @Override
    protected void commitIndexWriter(IndexWriter writer, Translog translog) throws IOException {
        // We must fetch the max uploaded translog file BEFORE performing the commit. Since all of those operations were written to
        // Lucene at this point, it is safe to start with the next file. The flush thread synchronously kicks of the commit upload
        // process, so for now we just store the start file as a thread local.
        translogStartFileForNextCommit = translogReplicator.getMaxUploadedFile() + 1;
        super.commitIndexWriter(writer, translog);
    }

    @Override
    protected void afterFlush(long generation) {
        assert isFlushLockIsHeldByCurrentThread() == false;
        if (ongoingFlushMustUpload.compareAndSet(true, false) || IS_FLUSH_BY_REFRESH.get() == false) {
            logger.trace("flush sets max generation of {} to generation [{}]", shardId, generation);
            statelessCommitService.ensureMaxGenerationToUploadForFlush(shardId, generation);
        }
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        DocumentSizeReporter documentParsingReporter = documentParsingProvider.newDocumentSizeReporter(shardId.getIndexName());

        IndexResult result = super.index(index);

        if (result.getResultType() == Result.Type.SUCCESS) {
            documentParsingReporter.onIndexingCompleted(index.parsedDoc());
        }
        return result;
    }

    @Override
    protected Map<String, String> getCommitExtraUserData() {
        return Map.of(TRANSLOG_RECOVERY_START_FILE, Long.toString(translogStartFileForNextCommit));
    }

    @Override
    protected RefreshResult refreshInternalSearcher(String source, boolean block) throws EngineException {
        if (source.equals(REAL_TIME_GET_REFRESH_SOURCE) || source.equals(UNSAFE_VERSION_MAP_REFRESH_SOURCE)) {
            try {
                IS_FLUSH_BY_REFRESH.set(true);
                // TODO: Eventually the Refresh API will also need to transition (maybe) to an async API here.
                flush(true, true);
            } finally {
                IS_FLUSH_BY_REFRESH.set(false);
            }
        }
        // TODO: could we avoid this refresh if we have flushed above?
        return super.refreshInternalSearcher(source, block);
    }

    // visible for testing
    public long getCurrentGeneration() {
        return getLastCommittedSegmentInfos().getGeneration();
    }

    // visible for testing
    Map<DirectoryReader, Set<PrimaryTermAndGeneration>> getOpenReaders() {
        synchronized (openReaders) {
            return Map.copyOf(openReaders);
        }
    }

    @Override
    public boolean allowSearchIdleOptimization() {
        return false;
    }

    @Override
    public void externalRefresh(String source, ActionListener<RefreshResult> listener) {
        // TODO: should we first check if a flush/refresh is needed or not? If not we could simply not go
        // through the throttler.
        refreshThrottler.maybeThrottle(new RefreshThrottler.Request(source, listener));
    }

    @Override
    public void maybeRefresh(String source, ActionListener<RefreshResult> listener) throws EngineException {
        if (fastRefresh) {
            super.maybeRefresh(source, listener);
        } else {
            try {
                IS_FLUSH_BY_REFRESH.set(true);
                Thread originalThread = Thread.currentThread();
                // Maybe refresh is called on scheduled periodic refreshes and needs to flush so that the search shards received the data.
                flush(false, false, listener.delegateFailure((l, flushResult) -> {
                    ActionRunnable<RefreshResult> refreshRunnable = new ActionRunnable<>(listener) {

                        @Override
                        protected void doRun() {
                            IndexEngine.super.maybeRefresh(source, listener);
                        }
                    };

                    dispatchRefreshRunnable(originalThread, refreshRunnable);
                }));
            } finally {
                IS_FLUSH_BY_REFRESH.set(false);
            }
        }

    }

    private void doExternalRefresh(RefreshThrottler.Request request) {
        if (fastRefresh) {
            IndexEngine.super.externalRefresh(request.source(), request.listener());
        } else {
            try {
                IS_FLUSH_BY_REFRESH.set(true);
                Thread originalThread = Thread.currentThread();
                flush(true, true, request.listener().delegateFailure((l, flushResult) -> {
                    ActionRunnable<RefreshResult> refreshRunnable = new ActionRunnable<>(l) {

                        @Override
                        protected void doRun() {
                            IndexEngine.super.externalRefresh(request.source(), listener);
                        }
                    };
                    dispatchRefreshRunnable(originalThread, refreshRunnable);
                }));
            } finally {
                IS_FLUSH_BY_REFRESH.set(false);
            }
        }

    }

    private void dispatchRefreshRunnable(Thread originalThread, ActionRunnable<RefreshResult> refreshRunnable) {
        // Sometimes a flush will have been performed meaning we are likely on the object store thread pool now. Dispatch back if the thread
        // has changed
        ThreadPool threadPool = engineConfig.getThreadPool();
        if (Thread.currentThread() == originalThread) {
            refreshRunnable.run();
        } else {
            threadPool.executor(ThreadPool.Names.REFRESH).execute(refreshRunnable);
        }
    }

    @Override
    public void asyncEnsureTranslogSynced(Translog.Location location, Consumer<Exception> listener) {
        super.asyncEnsureTranslogSynced(location, e -> {
            if (e != null) {
                listener.accept(e);
            } else {
                translogReplicator.sync(shardId, location, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        listener.accept(null);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.accept(e);
                    }
                });
            }
        });
    }

    @Override
    public boolean isTranslogSyncNeeded() {
        return super.isTranslogSyncNeeded() || translogReplicator.isSyncNeeded(shardId);
    }

    @Override
    public void syncTranslog() throws IOException {
        assert Thread.currentThread().getName().contains("[" + ThreadPool.Names.WRITE + "]") == false
            : "Expected current thread [" + Thread.currentThread() + "] to not be on a write thread. Reason: [syncTranslog]";
        super.syncTranslog();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        translogReplicator.syncAll(shardId, future);
        try {
            future.actionGet();
        } catch (Exception e) {
            throw new IOException("Exception while syncing translog remotely", e);
        }
    }

    @Override
    public void flushAndClose() throws IOException {
        // Don't flush on closing to avoid doing blobstore IO for reading back the latest commit from the repository
        // if it's not cached or doing an actual flush if there's outstanding translog operations.
        close();
    }

    public void readVirtualBatchedCompoundCommitChunk(final GetVirtualBatchedCompoundCommitChunkRequest request, final StreamOutput output)
        throws IOException {
        PrimaryTermAndGeneration vbccTermGen = new PrimaryTermAndGeneration(
            request.getPrimaryTerm(),
            request.getVirtualBatchedCompoundCommitGeneration()
        );
        var vbcc = statelessCommitService.getVirtualBatchedCompoundCommit(shardId, vbccTermGen);
        if (vbcc == null) {
            // If the VBCC was not found, then it is already uploaded, so let the search shard query the blob store
            throw VirtualBatchedCompoundCommit.buildResourceNotFoundException(shardId, vbccTermGen);
        } else {
            // This length adjustment is needed because the last CC is not padded in a vBCC
            long length = Math.min(request.getLength(), vbcc.getTotalSizeInBytes() - request.getOffset());
            vbcc.getBytesByRange(request.getOffset(), length, output);
        }
    }

    @Override
    public LiveVersionMapArchive createLiveVersionMapArchive() {
        return new StatelessLiveVersionMapArchive(this::getPreCommitSegmentGeneration);
    }

    public void commitSuccess(long generation) {
        ((StatelessLiveVersionMapArchive) getLiveVersionMapArchive()).afterUnpromotablesRefreshed(generation);
    }

    @Override
    protected Translog.Snapshot newTranslogSnapshot(long fromSeqNo, long toSeqNo) throws IOException {
        SearchDirectory searchDirectory = SearchDirectory.unwrapDirectory(this.store.directory());
        Optional<String> nodeEphemeralId = searchDirectory.getCurrentMetadataNodeEphemeralId();
        long translogRecoveryStartFile = searchDirectory.getTranslogRecoveryStartFile();

        if (nodeEphemeralId.isPresent()) {
            logger.debug("new translog snapshot seqnos [{}]-[{}] and node ephemeral id [{}]", fromSeqNo, toSeqNo, nodeEphemeralId.get());
            BlobContainer translogBlobContainer = this.translogBlobContainer.apply(nodeEphemeralId.get());
            TranslogReplicatorReader reader = new TranslogReplicatorReader(
                translogBlobContainer,
                shardId,
                fromSeqNo,
                toSeqNo,
                translogRecoveryStartFile,
                this::isClosing
            );
            return new Translog.Snapshot() {
                @Override
                public int totalOperations() {
                    return reader.totalOperations();
                }

                @Override
                public Translog.Operation next() throws IOException {
                    Translog.Operation next = reader.next();
                    if (next != null) {
                        advanceMaxSeqNoOfUpdatesOrDeletes(next.seqNo());
                    }
                    return next;
                }

                @Override
                public void close() throws IOException {
                    reader.close();
                }
            };
        } else {
            return Translog.Snapshot.EMPTY;
        }
    }

    @Override
    protected void waitForCommitDurability(long generation, ActionListener<Void> listener) {
        try {
            ensureOpen();
        } catch (AlreadyClosedException e) {
            listener.onFailure(e);
            return;
        }
        if (getLastCommittedSegmentInfos().getGeneration() < generation) {
            listener.onFailure(new IllegalStateException("Cannot wait on generation which has not been committed"));
        } else {
            // Wait for upload to complete only for true flushes, i.e. _not_ converted from refreshes, which guarantee
            // a commit to be uploaded.
            if (statelessCommitService.isStatelessUploadDelayed() == false || IS_FLUSH_BY_REFRESH.get() == false) {
                statelessCommitService.addListenerForUploadedGeneration(shardId, generation, listener);
            } else {
                logger.trace(() -> Strings.format("no need to wait for non-uploaded generation [%s]", generation));
                listener.onResponse(null);
            }
        }
    }

    @Override
    protected void reclaimVersionMapMemory() {
        // For Stateless LVM, we need to refresh AND flush as a refresh by itself doesn't decrease the memory usage of the version map.
        refresh("write indexing buffer", SearcherScope.INTERNAL, false);
        flush(false, false, ActionListener.noop());
    }

    // package private for testing

    RefreshThrottler getRefreshThrottler() {
        return refreshThrottler;
    }

    public StatelessCommitService getStatelessCommitService() {
        return statelessCommitService;
    }
}
