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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.IndexEngine;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.common.lucene.FilterIndexCommit;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.engine.ElasticsearchIndexDeletionPolicy;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SafeCommitInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Index deletion policy used in {@link IndexEngine}.
 * <p>
 * This policy allows to retain Lucene commits across engine resets: it uses an instance of {@link ShardLocalCommitsRefs},
 * created once when the index shard is created, to keep track of the acquired commits. When an {@link IndexEngine} is unhollow, its new
 * {@link IndexEngineDeletionPolicy} receives the same {@link ShardLocalCommitsRefs} instance and therefore can know the commits that were
 * previously acquired by a different engine instance.
 *
 * <p>
 * This policy delegates the deletion of local commits to the default CombinedDeletionPolicy policy. To prevent the default policy from
 * deleting a commit from the disk that is retained at the shard level by {@link ShardLocalCommitsRefs}, the policy wraps the commits as
 * {@link SoftDeleteIndexCommit} before passing them to the default policy. If the default policy deletes the local commit by calling
 * {@link IndexCommit#delete()} on the soft-delete index commit, the deletion of the commit files from disk is suppressed and the commit is
 * marked as soft-deleted. Once a soft-deleted commit is fully released (once their refcount in {@link ShardLocalCommitsRefs} is zeroed),
 * the {@link ShardLocalCommitsRefs} tries to delete the commit from disk by calling the {@link IndexCommit#delete()} on the
 * original commit.
 *
 * <p>
 * This policy also calls specific consumers when a new commit is created or an existing commit deleted.
 */
public class IndexEngineDeletionPolicy extends ElasticsearchIndexDeletionPolicy {

    private final ShardLocalCommitsRefs commitsRefs;
    private final ElasticsearchIndexDeletionPolicy delegate;
    private final CommitsListener commitsListener;

    private IndexCommit lastCommit = null;
    private boolean initialized;

    public IndexEngineDeletionPolicy(
        ShardLocalCommitsRefs shardLocalCommitsRefs,
        ElasticsearchIndexDeletionPolicy policy,
        CommitsListener commitsListener
    ) {
        this.commitsRefs = Objects.requireNonNull(shardLocalCommitsRefs);
        this.delegate = Objects.requireNonNull(policy);
        this.commitsListener = commitsListener;
    }

    @Override
    public synchronized void onInit(List<? extends IndexCommit> commits) throws IOException {
        assert initialized == false;
        processCommits(commits, delegate::onInit);
        initialized = true;
    }

    @Override
    public synchronized void onCommit(List<? extends IndexCommit> commits) throws IOException {
        assert initialized;
        processCommits(commits, delegate::onCommit);
    }

    private void processCommits(List<? extends IndexCommit> commits, CheckedConsumer<List<? extends IndexCommit>, IOException> consumer)
        throws IOException {
        assert Thread.holdsLock(this);

        // Acquire and wrap all commits as SoftDeleteIndexCommits before calling the default policy.
        var wrappedCommits = new ArrayList<SoftDeleteIndexCommit>(commits.size());
        for (var commit : commits) {
            wrappedCommits.add(commitsRefs.incRef(commit));
        }

        // Call the default policy
        consumer.accept(wrappedCommits);

        // Acquire and update the latest commit, then call the onCommitCreate listener
        if (wrappedCommits.isEmpty() == false) {
            final var previous = this.lastCommit;
            this.lastCommit = wrappedCommits.getLast();
            if (commitsListener != null && (previous == null || previous.getGeneration() != lastCommit.getGeneration())) {
                var additionalFiles = new HashSet<>(lastCommit.getFileNames());
                if (previous != null) {
                    assert previous.getGeneration() <= lastCommit.getGeneration();
                    additionalFiles.removeAll(previous.getFileNames());
                }
                var newCommit = acquireIndexCommit(false, true);
                var successfulNotification = false;
                try {
                    commitsListener.onNewCommit(new Engine.IndexCommitRef(newCommit, () -> releaseIndexCommit(newCommit)), additionalFiles);
                    successfulNotification = true;
                } finally {
                    if (successfulNotification == false) {
                        releaseIndexCommit(newCommit);
                    }
                }
            }
        }

        // Release all commits, and delete the ones that are not acquired by {@link LocalCommitsRefs} and that have been soft-deleted by the
        // default policy. Also call the onCommitDelete listener for the every deleted commit.
        for (var wrappedCommit : wrappedCommits) {
            if (commitsRefs.decRef(wrappedCommit)) {
                // It's possible that the RC=0 and the commit is not deleted if the wrapped policy decides to retain
                // that commit (i.e. it can be the safe commit or the last commit). Therefore, we have to honour that
                // decision and just don't delete the commit, even if there are no outstanding references held.
                if (wrappedCommit.isSoftDeleted()) {
                    deleteCommit(wrappedCommit);
                }
            }
        }
    }

    private void deleteCommit(SoftDeleteIndexCommit softDeleteIndexCommit) {
        try {
            // CombinedDeletionPolicy also wraps commits, so we need to unwrap to delete the original CommitPoint
            var commitPoint = FilterIndexCommit.unwrap(softDeleteIndexCommit.getIndexCommit());
            commitPoint.delete();
            commitsListener.onCommitDeleted(softDeleteIndexCommit);
        } catch (Exception e) {
            assert false : e;
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized SoftDeleteIndexCommit acquireIndexCommit(boolean acquiringSafeCommit) {
        return acquireIndexCommit(acquiringSafeCommit, false);
    }

    private synchronized SoftDeleteIndexCommit acquireIndexCommit(boolean acquiringSafeCommit, boolean acquiredForCommitListener) {
        return commitsRefs.incRef(delegate.acquireIndexCommit(acquiringSafeCommit), acquiredForCommitListener);
    }

    @Override
    public synchronized boolean releaseIndexCommit(IndexCommit acquiredIndexCommit) {
        assert acquiredIndexCommit instanceof SoftDeleteIndexCommit : acquiredIndexCommit;
        var softDeleteIndexCommit = (SoftDeleteIndexCommit) acquiredIndexCommit;
        // Release the original commit on the default policy
        boolean released = delegate.releaseIndexCommit(softDeleteIndexCommit.getIndexCommit());
        if (commitsRefs.decRef(softDeleteIndexCommit)) {
            // Released might be false if `acquiredIndexCommit` is either the latest or safe commit as these cannot be deleted
            return released;
        }
        return false;
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return delegate.getSafeCommitInfo();
    }

    @Override
    public boolean hasAcquiredIndexCommitsForTesting() {
        return commitsRefs.hasAcquiredIndexCommitsForTesting();
    }

    @Override
    public boolean hasUnreferencedCommits() {
        return delegate.hasUnreferencedCommits();
    }

    public interface CommitsListener {
        void onNewCommit(Engine.IndexCommitRef indexCommitRef, Set<String> additionalFiles);

        void onCommitDeleted(IndexCommit indexCommit);
    }
}
