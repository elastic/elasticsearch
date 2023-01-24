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

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.engine.ElasticsearchReaderManager;
import org.elasticsearch.index.engine.SafeCommitInfo;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.MultiFileWriter;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

// TODO: Add unit level tests for the commit download process
public class StatelessReaderManager extends AbstractRefCounted implements Closeable {

    private final Logger logger;
    private final ObjectStoreService objectStoreService;
    private final PriorityQueue<NewCommit> commitsToDownload = new PriorityQueue<>();
    private final ShardId shardId;
    private final Store store;
    private final ThreadPool threadPool;
    private final LongSupplier globalCheckpointSupplier;
    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile CurrentState currentState;
    // TODO: ensure we release the listeners when the search shard is relocated.
    private List<SegmentGenerationListener> segmentGenerationListeners = new ArrayList<>();

    public StatelessReaderManager(
        ObjectStoreService objectStoreService,
        ShardId shardId,
        Store store,
        ThreadPool threadPool,
        LongSupplier globalCheckpointSupplier
    ) {
        this.logger = Loggers.getLogger(StatelessReaderManager.class, shardId);
        this.objectStoreService = objectStoreService;
        this.shardId = shardId;
        this.store = store;
        this.threadPool = threadPool;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.store.incRef();
    }

    synchronized void reloadReaderManager() throws IOException {
        incRef();
        try {
            Directory directory = store.directory();
            ElasticsearchReaderManager readerManager;
            if (currentState != null) {
                readerManager = currentState.readerManager;
            } else {
                ElasticsearchDirectoryReader reader = ElasticsearchDirectoryReader.wrap(
                    new SoftDeletesDirectoryReaderWrapper(DirectoryReader.open(directory), Lucene.SOFT_DELETES_FIELD),
                    shardId
                );
                readerManager = new ElasticsearchReaderManager(reader);
            }

            boolean success = false;
            SegmentInfos newLastCommittedSegmentInfos;
            SeqNoStats newSeqNoStats;
            SafeCommitInfo newSafeCommitInfo;
            IndexCommit newIndexCommit;
            try {
                readerManager.maybeRefresh();
                newLastCommittedSegmentInfos = Lucene.readSegmentInfos(directory);
                assert currentState == null
                    || newLastCommittedSegmentInfos.getGeneration() >= currentState.lastCommittedSegmentInfos.getGeneration();
                final SequenceNumbers.CommitInfo seqNoStats = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(
                    newLastCommittedSegmentInfos.userData.entrySet()
                );
                long maxSeqNo = seqNoStats.maxSeqNo;
                long localCheckpoint = seqNoStats.localCheckpoint;
                newSeqNoStats = new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpointSupplier.getAsLong());
                newSafeCommitInfo = new SafeCommitInfo(newSeqNoStats.getLocalCheckpoint(), newLastCommittedSegmentInfos.totalMaxDoc());
                newIndexCommit = Lucene.getIndexCommit(newLastCommittedSegmentInfos, directory);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.close(readerManager);
                    // Here we drop the currentState if we failed to reload the latest one, we should improve this.
                    // TODO https://elasticco.atlassian.net/browse/ES-5301
                    currentState = null;
                }
            }
            logger.debug("{} refreshing current directory reader with generation [{}]", shardId, newIndexCommit.getGeneration());
            currentState = new CurrentState(newLastCommittedSegmentInfos, newSeqNoStats, newIndexCommit, readerManager, newSafeCommitInfo);
            callSegmentGenerationListeners(newIndexCommit.getGeneration());
        } finally {
            decRef();
        }
    }

    public void onNewCommit(
        final long primaryTerm,
        final long generation,
        final Map<String, StoreFileMetadata> commitFiles,
        ActionListener<Void> listener
    ) {
        synchronized (commitsToDownload) {
            NewCommit currentlyDownloadingCommit = commitsToDownload.peek();
            if (currentlyDownloadingCommit != null && currentlyDownloadingCommit.generation >= generation) {
                // Delay notification of the listener once the commit is downloaded
                addSegmentGenerationListener(generation, listener.map(ignored -> null));
                return;
            } else if (closed.get()) {
                listener.onFailure(new AlreadyClosedException("Stateless reader manager is closed"));
                return;
            }
            try {
                incRef();
                commitsToDownload.add(new NewCommit(primaryTerm, generation, commitFiles, ActionListener.runAfter(listener, this::decRef)));
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            if (currentlyDownloadingCommit != null) {
                // Already downloading a commit, do not schedule
                return;
            }
        }
        scheduleNextDownload();
    }

    private void scheduleNextDownload() {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {

            private void finish(Exception e) {
                synchronized (commitsToDownload) {
                    NewCommit commit = commitsToDownload.remove();
                    if (e == null) {
                        commit.listener().onResponse(null);
                    } else {
                        commit.listener().onFailure(e);
                    }
                    if (commitsToDownload.isEmpty()) {
                        return;
                    }
                }
                scheduleNextDownload();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Exception when attempting to load new commit.", e);
                finish(e);
            }

            @Override
            protected void doRun() throws Exception {
                if (closed.get()) {
                    throw new AlreadyClosedException("Stateless reader manager is closed");
                }

                final NewCommit commit = commitsToDownload.peek();
                assert commit != null;

                var toDownload = new HashMap<>(commit.commitFiles());
                store.incRef();
                try {
                    Store.MetadataSnapshot target = store.getMetadata(null);
                    var localFiles = new HashMap<>(target.fileMetadataMap());
                    var currentSegmentFile = target.getSegmentsFile();
                    if (currentSegmentFile != null) {
                        // TODO: Bootstrap workaround
                        // We always download the current segment file to avoid scenarios where the existing segment files
                        // is from the local bootstrap process. Hopefully we can remove this once bootstrap is cleaner.
                        localFiles.remove(currentSegmentFile.name());
                    }
                    toDownload.keySet().removeAll(localFiles.keySet());
                } finally {
                    store.decRef();
                }

                RecoveryState.Index indexState = new RecoveryState.Index();
                final String tempFilePrefix = "new_commit_" + commit.generation() + "_download_" + UUIDs.randomBase64UUID();
                MultiFileWriter multiFileWriter = new MultiFileWriter(store, indexState, tempFilePrefix, logger, () -> {});

                for (StoreFileMetadata fileMetadata : toDownload.values()) {
                    indexState.addFileDetail(fileMetadata.name(), fileMetadata.length(), false);
                }

                incRef();
                objectStoreService.onNewCommitReceived(
                    shardId,
                    commit.primaryTerm(),
                    commit.generation(),
                    toDownload,
                    multiFileWriter,
                    ActionListener.runAfter(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            boolean success = false;
                            try {
                                multiFileWriter.renameAllTempFiles();
                                reloadReaderManager();
                                success = true;
                            } catch (Exception e) {
                                logger.error("failed to reload reader after new commit", e);
                                success = false;
                                finish(e);
                            } finally {
                                IOUtils.closeWhileHandlingException(multiFileWriter);
                                if (success) {
                                    finish(null);
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                finish(e);
                            } finally {
                                IOUtils.closeWhileHandlingException(multiFileWriter);
                            }
                        }
                    }, () -> decRef())
                );
            }
        });
    }

    public ReferenceManager<ElasticsearchDirectoryReader> getReaderManager() {
        return currentState.readerManager();
    }

    SeqNoStats getSeqNoStats() {
        return currentState.seqNoStats();
    }

    SegmentInfos getSegmentInfos() {
        return currentState.lastCommittedSegmentInfos();
    }

    SafeCommitInfo getSafeCommitInfo() {
        return currentState.safeCommitInfo();
    }

    @Override
    public void close() {
        final boolean success = closed.compareAndSet(false, true);
        assert success : "stateless reader manager is already closed";
        if (success) {
            decRef();
        }
    }

    @Override
    protected void closeInternal() {
        try {
            final List<Closeable> closeables = new ArrayList<>();
            if (currentState != null) {
                closeables.add(currentState.readerManager());
            }
            closeables.add(() -> failSegmentGenerationListeners(new AlreadyClosedException("Stateless reader manager is closing")));
            closeables.add(store::decRef);
            IOUtils.close(closeables);
            assert segmentGenerationListeners.isEmpty() : segmentGenerationListeners;
            this.currentState = null;
        } catch (Exception e) {
            assert false : e;
            throw new ElasticsearchException("Failed to close stateless reader manager", e);
        }
    }

    public synchronized void addSegmentGenerationListener(long minGeneration, ActionListener<Long> listener) {
        try {
            if (closed.get()) {
                throw new AlreadyClosedException("Stateless reader manager is closed");
            }
            incRef();
            try {
                // we consider that the current generation is 0 when the currentState is unknown, either because it is not yet downloaded
                // or the last reload failed. We should improve this.
                // TODO https://elasticco.atlassian.net/browse/ES-5301
                var currentGeneration = currentState != null ? currentState.newIndexCommit().getGeneration() : 0L;
                assert minGeneration > 0 : minGeneration;
                if (minGeneration > currentGeneration) {
                    segmentGenerationListeners.add(new SegmentGenerationListener(minGeneration, listener));
                } else {
                    listener.onResponse(currentGeneration);
                }
            } finally {
                decRef();
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void callSegmentGenerationListeners(long currentGen) {
        assert Thread.holdsLock(this);
        List<SegmentGenerationListener> pendingListeners = new ArrayList<>();
        List<SegmentGenerationListener> listenersToCall = new ArrayList<>();
        for (SegmentGenerationListener listener : segmentGenerationListeners) {
            if (listener.minGeneration() <= currentGen) {
                listenersToCall.add(listener);
            } else {
                pendingListeners.add(listener);
            }
        }
        segmentGenerationListeners = pendingListeners;
        for (SegmentGenerationListener listener : listenersToCall) {
            try {
                listener.listener().onResponse(currentGen);
            } catch (Exception e) {
                logger.warn(() -> "segment generation listener [" + listener.minGeneration() + "][" + listener + "] failed", e);
                assert false : e;
            }
        }
    }

    private void failSegmentGenerationListeners(Exception e) {
        assert closed.get() : "stateless reader manager for " + shardId + " should be closed";
        final List<SegmentGenerationListener> listeners;
        synchronized (this) {
            listeners = List.copyOf(segmentGenerationListeners);
            this.segmentGenerationListeners = List.of();
        }
        for (SegmentGenerationListener listener : listeners) {
            try {
                listener.listener().onFailure(e);
            } catch (Exception e2) {
                e2.addSuppressed(e);
                logger.warn(() -> "segment generation listener [" + listener.minGeneration() + "][" + listener + "] failed", e2);
                assert false : e2;
            }
        }
    }

    private record SegmentGenerationListener(long minGeneration, ActionListener<Long> listener) {}

    private record NewCommit(long primaryTerm, long generation, Map<String, StoreFileMetadata> commitFiles, ActionListener<Void> listener)
        implements
            Comparable<NewCommit> {

        @Override
        public int compareTo(NewCommit o) {
            return Long.compare(generation, o.generation);
        }
    }

    private record CurrentState(
        SegmentInfos lastCommittedSegmentInfos,
        SeqNoStats seqNoStats,
        IndexCommit newIndexCommit,
        ElasticsearchReaderManager readerManager,
        SafeCommitInfo safeCommitInfo
    ) {}
}
