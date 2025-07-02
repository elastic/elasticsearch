/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;

import java.util.Set;

public abstract class ElasticsearchIndexDeletionPolicy extends IndexDeletionPolicy {

    /**
     * Captures the most recent commit point or the most recent safe commit point.
     * Index files of the capturing commit point won't be released until the commit reference is closed.
     *
     * @param acquiringSafeCommit captures the most recent safe commit point if true; otherwise captures the most recent commit point.
     */
    public abstract IndexCommit acquireIndexCommit(boolean acquiringSafeCommit);

    /**
     * Releases an index commit that acquired by {@link #acquireIndexCommit(boolean)}.
     *
     * @return true if the acquired commit can be clean up.
     */
    public abstract boolean releaseIndexCommit(IndexCommit acquiredIndexCommit);

    /**
     * @return information about the safe commit
     */
    public abstract SafeCommitInfo getSafeCommitInfo();

    public abstract boolean hasAcquiredIndexCommitsForTesting();

    public abstract boolean hasUnreferencedCommits();

    public interface CommitsListener {

        void onNewAcquiredCommit(IndexCommit commit, Set<String> additionalFiles);

        void onDeletedCommit(IndexCommit commit);
    }
}
