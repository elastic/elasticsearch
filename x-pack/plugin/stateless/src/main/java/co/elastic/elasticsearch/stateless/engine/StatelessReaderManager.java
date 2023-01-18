package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.ObjectStoreService;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.LongSupplier;

// TODO: Add unit level tests for the commit download process
public class StatelessReaderManager implements Closeable {

    private final Logger logger;
    private final ObjectStoreService objectStoreService;
    private final PriorityQueue<NewCommit> commitsToDownload = new PriorityQueue<>();
    private final ShardId shardId;
    private final Store store;
    private final ThreadPool threadPool;
    private final LongSupplier globalCheckpointSupplier;
    private volatile CurrentState currentState;

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
    }

    synchronized void reloadReaderManager() throws IOException {
        logger.debug(
            "refreshing current directory reader with generation [{}] using new generation [{}]",
            currentState != null ? currentState.lastCommittedSegmentInfos().getGeneration() : "",
            currentState != null ? currentState.lastCommittedSegmentInfos().getGeneration() : ""
        );

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
                currentState = null;
            }
        }

        currentState = new CurrentState(newLastCommittedSegmentInfos, newSeqNoStats, newIndexCommit, readerManager, newSafeCommitInfo);
    }

    public void onNewCommit(
        final long primaryTerm,
        final long generation,
        final Map<String, StoreFileMetadata> commitFiles,
        ActionListener<Void> listener
    ) throws IOException {
        synchronized (commitsToDownload) {
            NewCommit currentlyDownloadingCommit = commitsToDownload.peek();
            if (currentlyDownloadingCommit != null && currentlyDownloadingCommit.generation >= generation) {
                // Already downloading a newer commit. Just ignore
                listener.onResponse(null);
                return;
            }
            commitsToDownload.add(new NewCommit(primaryTerm, generation, commitFiles, listener));
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
                NewCommit commit = commitsToDownload.peek();
                assert commit != null;

                Store.MetadataSnapshot target = store.getMetadata(null);
                var toDownload = new HashMap<>(commit.commitFiles());
                var localFiles = new HashMap<>(target.fileMetadataMap());
                var currentSegmentFile = target.getSegmentsFile();
                if (currentSegmentFile != null) {
                    // TODO: Bootstrap workaround
                    // We always download the current segment file to avoid scenarios where the existing segment files
                    // is from the local bootstrap process. Hopefully we can remove this once bootstrap is cleaner.
                    localFiles.remove(currentSegmentFile.name());
                }

                toDownload.keySet().removeAll(localFiles.keySet());

                RecoveryState.Index indexState = new RecoveryState.Index();
                final String tempFilePrefix = "new_commit_" + commit.generation() + "_download_" + UUIDs.randomBase64UUID();
                MultiFileWriter multiFileWriter = new MultiFileWriter(store, indexState, tempFilePrefix, logger, () -> {});

                for (StoreFileMetadata fileMetadata : toDownload.values()) {
                    indexState.addFileDetail(fileMetadata.name(), fileMetadata.length(), false);
                }

                store.incRef();
                objectStoreService.onNewCommitReceived(
                    shardId,
                    commit.primaryTerm(),
                    commit.generation(),
                    toDownload,
                    multiFileWriter,
                    ActionListener.runAfter(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            try (multiFileWriter) {
                                multiFileWriter.renameAllTempFiles();
                            } catch (IOException e) {
                                logger.error("failed to rename temporary commit files", e);
                            }
                            try {
                                reloadReaderManager();
                            } catch (IOException e) {
                                logger.error("failed to reload reader manager", e);
                            }
                            finish(null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            multiFileWriter.close();
                            finish(e);
                        }
                    }, store::decRef)
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
    public void close() throws IOException {
        IOUtils.close(currentState.readerManager());
        currentState = null;
    }

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
