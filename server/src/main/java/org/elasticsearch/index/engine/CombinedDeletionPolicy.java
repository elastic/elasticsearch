/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.ObjectIntHashMap;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * An {@link IndexDeletionPolicy} that coordinates between Lucene's commits and the retention of translog generation files,
 * making sure that all translog files that are needed to recover from the Lucene commit are not deleted.
 * <p>
 * In particular, this policy will delete index commits whose max sequence number is at most
 * the current global checkpoint except the index commit which has the highest max sequence number among those.
 */
public final class CombinedDeletionPolicy extends IndexDeletionPolicy {
    private final Logger logger;
    private final TranslogDeletionPolicy translogDeletionPolicy;
    private final EngineConfig.OpenMode openMode;
    private final LongSupplier globalCheckpointSupplier;
    private final IndexCommit startingCommit;
    private final ObjectIntHashMap<IndexCommit> snapshottedCommits; // Number of snapshots held against each commit point.
    private IndexCommit safeCommit; // the most recent safe commit point - its max_seqno at most the persisted global checkpoint.
    private IndexCommit lastCommit; // the most recent commit point

    CombinedDeletionPolicy(EngineConfig.OpenMode openMode, Logger logger, TranslogDeletionPolicy translogDeletionPolicy,
                           LongSupplier globalCheckpointSupplier, IndexCommit startingCommit) {
        this.openMode = openMode;
        this.logger = logger;
        this.translogDeletionPolicy = translogDeletionPolicy;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.startingCommit = startingCommit;
        this.snapshottedCommits = new ObjectIntHashMap<>();
    }

    @Override
    public synchronized void onInit(List<? extends IndexCommit> commits) throws IOException {
        switch (openMode) {
            case CREATE_INDEX_AND_TRANSLOG:
                assert startingCommit == null : "CREATE_INDEX_AND_TRANSLOG must not have starting commit; commit [" + startingCommit + "]";
                break;
            case OPEN_INDEX_CREATE_TRANSLOG:
            case OPEN_INDEX_AND_TRANSLOG:
                assert commits.isEmpty() == false : "index is opened, but we have no commits";
                assert startingCommit != null && commits.contains(startingCommit) : "Starting commit not in the existing commit list; "
                    + "startingCommit [" + startingCommit + "], commit list [" + commits + "]";
                keepOnlyStartingCommitOnInit(commits);
                // OPEN_INDEX_CREATE_TRANSLOG can open an index commit from other shard with a different translog history,
                // We therefore should not use that index commit to update the translog deletion policy.
                if (openMode == EngineConfig.OpenMode.OPEN_INDEX_AND_TRANSLOG) {
                    updateTranslogDeletionPolicy();
                }
                break;
            default:
                throw new IllegalArgumentException("unknown openMode [" + openMode + "]");
        }
    }

    /**
     * Keeping existing unsafe commits when opening an engine can be problematic because these commits are not safe
     * at the recovering time but they can suddenly become safe in the future.
     * The following issues can happen if unsafe commits are kept oninit.
     * <p>
     * 1. Replica can use unsafe commit in peer-recovery. This happens when a replica with a safe commit c1(max_seqno=1)
     * and an unsafe commit c2(max_seqno=2) recovers from a primary with c1(max_seqno=1). If a new document(seqno=2)
     * is added without flushing, the global checkpoint is advanced to 2; and the replica recovers again, it will use
     * the unsafe commit c2(max_seqno=2 at most gcp=2) as the starting commit for sequenced-based recovery even the
     * commit c2 contains a stale operation and the document(with seqno=2) will not be replicated to the replica.
     * <p>
     * 2. Min translog gen for recovery can go backwards in peer-recovery. This happens when are replica with a safe commit
     * c1(local_checkpoint=1, recovery_translog_gen=1) and an unsafe commit c2(local_checkpoint=2, recovery_translog_gen=2).
     * The replica recovers from a primary, and keeps c2 as the last commit, then sets last_translog_gen to 2. Flushing a new
     * commit on the replica will cause exception as the new last commit c3 will have recovery_translog_gen=1. The recovery
     * translog generation of a commit is calculated based on the current local checkpoint. The local checkpoint of c3 is 1
     * while the local checkpoint of c2 is 2.
     * <p>
     * 3. Commit without translog can be used in recovery. An old index, which was created before multiple-commits is introduced
     * (v6.2), may not have a safe commit. If that index has a snapshotted commit without translog and an unsafe commit,
     * the policy can consider the snapshotted commit as a safe commit for recovery even the commit does not have translog.
     */
    private void keepOnlyStartingCommitOnInit(List<? extends IndexCommit> commits) throws IOException {
        for (IndexCommit commit : commits) {
            if (startingCommit.equals(commit) == false) {
                this.deleteCommit(commit);
            }
        }
        assert startingCommit.isDeleted() == false : "Starting commit must not be deleted";
        lastCommit = startingCommit;
        safeCommit = startingCommit;
    }

    @Override
    public synchronized void onCommit(List<? extends IndexCommit> commits) throws IOException {
        final int keptPosition = indexOfKeptCommits(commits, globalCheckpointSupplier.getAsLong());
        lastCommit = commits.get(commits.size() - 1);
        safeCommit = commits.get(keptPosition);
        for (int i = 0; i < keptPosition; i++) {
            if (snapshottedCommits.containsKey(commits.get(i)) == false) {
                deleteCommit(commits.get(i));
            }
        }
        updateTranslogDeletionPolicy();
    }

    private void deleteCommit(IndexCommit commit) throws IOException {
        assert commit.isDeleted() == false : "Index commit [" + commitDescription(commit) + "] is deleted twice";
        logger.debug("Delete index commit [{}]", commitDescription(commit));
        commit.delete();
        assert commit.isDeleted() : "Deletion commit [" + commitDescription(commit) + "] was suppressed";
    }

    private void updateTranslogDeletionPolicy() throws IOException {
        assert Thread.holdsLock(this);
        logger.debug("Safe commit [{}], last commit [{}]", commitDescription(safeCommit), commitDescription(lastCommit));
        assert safeCommit.isDeleted() == false : "The safe commit must not be deleted";
        final long minRequiredGen = Long.parseLong(safeCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));
        assert lastCommit.isDeleted() == false : "The last commit must not be deleted";
        final long lastGen = Long.parseLong(lastCommit.getUserData().get(Translog.TRANSLOG_GENERATION_KEY));

        assert minRequiredGen <= lastGen : "minRequiredGen must not be greater than lastGen";
        translogDeletionPolicy.setTranslogGenerationOfLastCommit(lastGen);
        translogDeletionPolicy.setMinTranslogGenerationForRecovery(minRequiredGen);
    }

    /**
     * Captures the most recent commit point {@link #lastCommit} or the most recent safe commit point {@link #safeCommit}.
     * Index files of the capturing commit point won't be released until the commit reference is closed.
     *
     * @param acquiringSafeCommit captures the most recent safe commit point if true; otherwise captures the most recent commit point.
     */
    synchronized IndexCommit acquireIndexCommit(boolean acquiringSafeCommit) {
        assert safeCommit != null : "Safe commit is not initialized yet";
        assert lastCommit != null : "Last commit is not initialized yet";
        final IndexCommit snapshotting = acquiringSafeCommit ? safeCommit : lastCommit;
        snapshottedCommits.addTo(snapshotting, 1); // increase refCount
        return new SnapshotIndexCommit(snapshotting);
    }

    /**
     * Releases an index commit that acquired by {@link #acquireIndexCommit(boolean)}.
     */
    synchronized void releaseCommit(final IndexCommit snapshotCommit) {
        final IndexCommit releasingCommit = ((SnapshotIndexCommit) snapshotCommit).delegate;
        assert snapshottedCommits.containsKey(releasingCommit) : "Release non-snapshotted commit;" +
            "snapshotted commits [" + snapshottedCommits + "], releasing commit [" + releasingCommit + "]";
        final int refCount = snapshottedCommits.addTo(releasingCommit, -1); // release refCount
        assert refCount >= 0 : "Number of snapshots can not be negative [" + refCount + "]";
        if (refCount == 0) {
            snapshottedCommits.remove(releasingCommit);
        }
    }

    /**
     * Find a safe commit point from a list of existing commits based on the supplied global checkpoint.
     * The max sequence number of a safe commit point should be at most the global checkpoint.
     * If an index was created before v6.2, and we haven't retained a safe commit yet, this method will return the oldest commit.
     *
     * @param commits          a list of existing commit points
     * @param globalCheckpoint the persisted global checkpoint from the translog, see {@link Translog#readGlobalCheckpoint(Path, String)}
     * @return a safe commit or the oldest commit if a safe commit is not found
     */
    public static IndexCommit findSafeCommitPoint(List<IndexCommit> commits, long globalCheckpoint) throws IOException {
        if (commits.isEmpty()) {
            throw new IllegalArgumentException("Commit list must not empty");
        }
        final int keptPosition = indexOfKeptCommits(commits, globalCheckpoint);
        return commits.get(keptPosition);
    }

    /**
     * Find the highest index position of a safe index commit whose max sequence number is not greater than the global checkpoint.
     * Index commits with different translog UUID will be filtered out as they don't belong to this engine.
     */
    private static int indexOfKeptCommits(List<? extends IndexCommit> commits, long globalCheckpoint) throws IOException {
        final String expectedTranslogUUID = commits.get(commits.size() - 1).getUserData().get(Translog.TRANSLOG_UUID_KEY);

        // Commits are sorted by age (the 0th one is the oldest commit).
        for (int i = commits.size() - 1; i >= 0; i--) {
            final Map<String, String> commitUserData = commits.get(i).getUserData();
            // Ignore index commits with different translog uuid.
            if (expectedTranslogUUID.equals(commitUserData.get(Translog.TRANSLOG_UUID_KEY)) == false) {
                return i + 1;
            }
            // 5.x commits do not contain MAX_SEQ_NO, we should not keep it and the older commits.
            if (commitUserData.containsKey(SequenceNumbers.MAX_SEQ_NO) == false) {
                return Math.min(commits.size() - 1, i + 1);
            }
            final long maxSeqNoFromCommit = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO));
            // If a 6.x node with a 5.x index is promoted to be a primary, it will flush a new index commit to
            // make sure translog operations without seqno will never be replayed (see IndexShard#updateShardState).
            // However the global checkpoint is still UNASSIGNED and the max_seqno of both commits are NO_OPS_PERFORMED.
            // If this policy considers the first commit as a safe commit, we will send the first commit without replaying
            // translog between these commits to the replica in a peer-recovery. This causes the replica missing those operations.
            // To prevent this, we should not keep more than one commit whose max_seqno is NO_OPS_PERFORMED.
            // Once we can retain a safe commit, a NO_OPS_PERFORMED commit will be deleted just as other commits.
            if (maxSeqNoFromCommit == SequenceNumbers.NO_OPS_PERFORMED) {
                return i;
            }
            if (maxSeqNoFromCommit <= globalCheckpoint) {
                return i;
            }
        }
        /*
         * We may reach to this point in these cases:
         * 1. In the previous 6.x, we keep only the last commit - which is likely not a safe commit if writes are in progress.
         * Thus, after upgrading, we may not find a safe commit until we can reserve one.
         * 2. In peer-recovery, if the file-based happens, a replica will be received the latest commit from a primary.
         * However, that commit may not be a safe commit if writes are in progress in the primary.
         */
        return 0;
    }

    /**
     * Returns a description for a given {@link IndexCommit}. This should be only used for logging and debugging.
     */
    public static String commitDescription(IndexCommit commit) throws IOException {
        return String.format(Locale.ROOT, "CommitPoint{segment[%s], userData[%s]}", commit.getSegmentsFileName(), commit.getUserData());
    }

    /**
     * A wrapper of an index commit that prevents it from being deleted.
     */
    private static class SnapshotIndexCommit extends IndexCommit {
        private final IndexCommit delegate;

        SnapshotIndexCommit(IndexCommit delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getSegmentsFileName() {
            return delegate.getSegmentsFileName();
        }

        @Override
        public Collection<String> getFileNames() throws IOException {
            return delegate.getFileNames();
        }

        @Override
        public Directory getDirectory() {
            return delegate.getDirectory();
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("A snapshot commit does not support deletion");
        }

        @Override
        public boolean isDeleted() {
            return delegate.isDeleted();
        }

        @Override
        public int getSegmentCount() {
            return delegate.getSegmentCount();
        }

        @Override
        public long getGeneration() {
            return delegate.getGeneration();
        }

        @Override
        public Map<String, String> getUserData() throws IOException {
            return delegate.getUserData();
        }

        @Override
        public String toString() {
            return "SnapshotIndexCommit{" + delegate + "}";
        }
    }
}
