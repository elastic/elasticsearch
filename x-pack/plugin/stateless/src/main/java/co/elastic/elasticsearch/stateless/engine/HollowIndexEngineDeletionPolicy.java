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

import co.elastic.elasticsearch.stateless.commits.LocalCommitsRefs;
import co.elastic.elasticsearch.stateless.commits.SoftDeleteIndexCommit;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.index.engine.ElasticsearchIndexDeletionPolicy;
import org.elasticsearch.index.engine.SafeCommitInfo;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Index deletion policy used in {@link HollowIndexEngine}.
 * <p>
 * This policy allows to retain Lucene commits across engine resets: it uses an instance of {@link LocalCommitsRefs}, created once when the
 * index shard is created, to keep track of the acquired commits. When an {@link HollowIndexEngine} is created, its new
 * {@link IndexEngineDeletionPolicy} receives the same {@link LocalCommitsRefs} instance and therefore can know the commits that were
 * previously acquired by a different engine instance.
 * <p>
 * This policy does not delete commits locally. A commit can be deleted in the future when the shard is recovered (when we search for
 * unused commits) or the next time the shard is unhollowed.
 * <p>
 * Note: this policy is not intended to be used in an {@link org.apache.lucene.index.IndexWriter}, but it implements
 * {@link ElasticsearchIndexDeletionPolicy} so that it can be used within an {@link HollowIndexEngine} in a similar way other engine
 * implementations use index deletion policies.
 */
public class HollowIndexEngineDeletionPolicy extends ElasticsearchIndexDeletionPolicy {

    private final LocalCommitsRefs commitsRefs;

    private IndexCommit commit;
    private boolean initialized;
    private SafeCommitInfo safeCommitInfo;

    public HollowIndexEngineDeletionPolicy(LocalCommitsRefs localCommitsRefs) {
        this.commitsRefs = localCommitsRefs;
    }

    synchronized void onInit(IndexCommit commit, SafeCommitInfo safeCommitInfo) throws IOException {
        assert initialized == false;
        this.commit = Objects.requireNonNull(commit);
        this.safeCommitInfo = Objects.requireNonNull(safeCommitInfo);
        this.initialized = true;
    }

    @Override
    public synchronized void onInit(List<? extends IndexCommit> commits) {
        assert false : "should never be called";
    }

    @Override
    public synchronized void onCommit(List<? extends IndexCommit> commits) {
        assert false : "should never be called";
    }

    @Override
    public synchronized IndexCommit acquireIndexCommit(boolean acquiringSafeCommit) {
        assert initialized;
        return commitsRefs.incRef(this.commit);
    }

    @Override
    public synchronized boolean releaseIndexCommit(IndexCommit acquiredIndexCommit) {
        assert acquiredIndexCommit instanceof SoftDeleteIndexCommit;
        return commitsRefs.decRef(acquiredIndexCommit);
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return safeCommitInfo;
    }

    @Override
    public boolean hasAcquiredIndexCommitsForTesting() {
        return false;
    }

    @Override
    public boolean hasUnreferencedCommits() {
        return false;
    }
}
