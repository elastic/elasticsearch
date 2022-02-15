/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import com.carrotsearch.hppc.ObjectIntHashMap;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.SegmentInfos;
import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.nio.file.Path;
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
public class CombinedDeletionPolicy extends IndexDeletionPolicy {
    private final Logger logger;
    private final TranslogDeletionPolicy translogDeletionPolicy;
    private final SoftDeletesPolicy softDeletesPolicy;
    private final LongSupplier globalCheckpointSupplier;
    private final ObjectIntHashMap<IndexCommit> snapshottedCommits; // Number of snapshots held against each commit point.
    private volatile IndexCommit safeCommit; // the most recent safe commit point - its max_seqno at most the persisted global checkpoint.
    private volatile long maxSeqNoOfNextSafeCommit;
    private volatile IndexCommit lastCommit; // the most recent commit point
    private volatile SafeCommitInfo safeCommitInfo = SafeCommitInfo.EMPTY;

    CombinedDeletionPolicy(
        Logger logger,
        TranslogDeletionPolicy translogDeletionPolicy,
        SoftDeletesPolicy softDeletesPolicy,
        LongSupplier globalCheckpointSupplier
    ) {
        this.logger = logger;
        this.translogDeletionPolicy = translogDeletionPolicy;
        this.softDeletesPolicy = softDeletesPolicy;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.snapshottedCommits = new ObjectIntHashMap<>();
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        assert commits.isEmpty() == false : "index is opened, but we have no commits";
        onCommit(commits);
        if (safeCommit != commits.get(commits.size() - 1)) {
            throw new IllegalStateException(
                "Engine is opened, but the last commit isn't safe. Global checkpoint ["
                    + globalCheckpointSupplier.getAsLong()
                    + "], seqNo is last commit ["
                    + SequenceNumbers.loadSeqNoInfoFromLuceneCommit(lastCommit.getUserData().entrySet())
                    + "], "
                    + "seqNos in safe commit ["
                    + SequenceNumbers.loadSeqNoInfoFromLuceneCommit(safeCommit.getUserData().entrySet())
                    + "]"
            );
        }
    }

    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        final IndexCommit safeCommit;
        synchronized (this) {
            final int keptPosition = indexOfKeptCommits(commits, globalCheckpointSupplier.getAsLong());
            this.safeCommitInfo = SafeCommitInfo.EMPTY;
            this.lastCommit = commits.get(commits.size() - 1);
            this.safeCommit = commits.get(keptPosition);
            for (int i = 0; i < keptPosition; i++) {
                if (snapshottedCommits.containsKey(commits.get(i)) == false) {
                    deleteCommit(commits.get(i));
                }
            }
            updateRetentionPolicy();
            if (keptPosition == commits.size() - 1) {
                this.maxSeqNoOfNextSafeCommit = Long.MAX_VALUE;
            } else {
                this.maxSeqNoOfNextSafeCommit = Long.parseLong(commits.get(keptPosition + 1).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
            }
            safeCommit = this.safeCommit;
        }

        assert Thread.holdsLock(this) == false : "should not block concurrent acquire or relesase";
        safeCommitInfo = new SafeCommitInfo(
            Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)),
            getDocCountOfCommit(safeCommit)
        );

        // This is protected from concurrent calls by a lock on the IndexWriter, but this assertion makes sure that we notice if that ceases
        // to be true in future. It is not disastrous if safeCommitInfo refers to an older safeCommit, it just means that we might retain a
        // bit more history and do a few more ops-based recoveries than we would otherwise.
        final IndexCommit newSafeCommit = this.safeCommit;
        assert safeCommit == newSafeCommit
            : "onCommit called concurrently? " + safeCommit.getGeneration() + " vs " + newSafeCommit.getGeneration();
    }

    private void deleteCommit(IndexCommit commit) throws IOException {
        assert commit.isDeleted() == false : "Index commit [" + commitDescription(commit) + "] is deleted twice";
        logger.debug("Delete index commit [{}]", commitDescription(commit));
        commit.delete();
        assert commit.isDeleted() : "Deletion commit [" + commitDescription(commit) + "] was suppressed";
    }

    private void updateRetentionPolicy() throws IOException {
        assert Thread.holdsLock(this);
        logger.debug("Safe commit [{}], last commit [{}]", commitDescription(safeCommit), commitDescription(lastCommit));
        assert safeCommit.isDeleted() == false : "The safe commit must not be deleted";
        assert lastCommit.isDeleted() == false : "The last commit must not be deleted";
        final long localCheckpointOfSafeCommit = Long.parseLong(safeCommit.getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
        softDeletesPolicy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
        translogDeletionPolicy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
    }

    protected int getDocCountOfCommit(IndexCommit indexCommit) throws IOException {
        return SegmentInfos.readCommit(indexCommit.getDirectory(), indexCommit.getSegmentsFileName()).totalMaxDoc();
    }

    SafeCommitInfo getSafeCommitInfo() {
        return safeCommitInfo;
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
     *
     * @return true if the snapshotting commit can be clean up.
     */
    synchronized boolean releaseCommit(final IndexCommit snapshotCommit) {
        final IndexCommit releasingCommit = ((SnapshotIndexCommit) snapshotCommit).getIndexCommit();
        assert snapshottedCommits.containsKey(releasingCommit)
            : "Release non-snapshotted commit;"
                + "snapshotted commits ["
                + snapshottedCommits
                + "], releasing commit ["
                + releasingCommit
                + "]";
        final int refCount = snapshottedCommits.addTo(releasingCommit, -1); // release refCount
        assert refCount >= 0 : "Number of snapshots can not be negative [" + refCount + "]";
        if (refCount == 0) {
            snapshottedCommits.remove(releasingCommit);
        }
        // The commit can be clean up only if no pending snapshot and it is neither the safe commit nor last commit.
        return refCount == 0 && releasingCommit.equals(safeCommit) == false && releasingCommit.equals(lastCommit) == false;
    }

    /**
     * Find a safe commit point from a list of existing commits based on the supplied global checkpoint.
     * The max sequence number of a safe commit point should be at most the global checkpoint.
     * If an index was created before 6.2 or recovered from remote, we might not have a safe commit.
     * In this case, this method will return the oldest index commit.
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
            final long maxSeqNoFromCommit = Long.parseLong(commitUserData.get(SequenceNumbers.MAX_SEQ_NO));
            if (maxSeqNoFromCommit <= globalCheckpoint) {
                return i;
            }
        }
        // If an index was created before 6.2 or recovered from remote, we might not have a safe commit.
        // In this case, we return the oldest index commit instead.
        return 0;
    }

    /**
     * Checks whether the deletion policy is holding on to snapshotted commits
     */
    synchronized boolean hasSnapshottedCommits() {
        return snapshottedCommits.isEmpty() == false;
    }

    /**
     * Checks if the deletion policy can delete some index commits with the latest global checkpoint.
     */
    boolean hasUnreferencedCommits() {
        return maxSeqNoOfNextSafeCommit <= globalCheckpointSupplier.getAsLong();
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
    private static class SnapshotIndexCommit extends FilterIndexCommit {
        SnapshotIndexCommit(IndexCommit delegate) {
            super(delegate);
        }

        @Override
        public void delete() {
            throw new UnsupportedOperationException("A snapshot commit does not support deletion");
        }
    }
}
