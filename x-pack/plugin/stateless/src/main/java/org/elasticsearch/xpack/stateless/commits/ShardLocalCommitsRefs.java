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

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.core.Assertions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages acquired {@link IndexCommit} references for a shard, ensuring they remain
 * valid even when the underlying {@link org.elasticsearch.index.engine.Engine} changes.
 *
 * <p>Once a reference is acquired (e.g., for snapshots), the commit is protected from
 * deletion until all references are released, preventing data corruption during
 * long-running operations.
 */
public class ShardLocalCommitsRefs {
    /**
     * Holds the references count for a commit
     */
    private final Map<Long, Integer> acquiredGenerations;
    // Index commits internally acquired by the commits listener. We want to track them separately to be able to disregard them
    // when checking for externally acquired index commits that haven't been released during testing
    private final Map<Long, Integer> acquiredCommitGenerationsForCommitsListener = Assertions.ENABLED ? new ConcurrentHashMap<>() : null;

    public ShardLocalCommitsRefs() {
        this.acquiredGenerations = new ConcurrentHashMap<>();
    }

    SoftDeleteIndexCommit incRef(IndexCommit indexCommit) {
        return incRef(indexCommit, false);
    }

    SoftDeleteIndexCommit incRef(IndexCommit indexCommit, boolean acquiredForCommitListener) {
        if (Assertions.ENABLED && acquiredForCommitListener) {
            incRefGeneration(acquiredCommitGenerationsForCommitsListener, indexCommit.getGeneration());
        }
        incRefGeneration(acquiredGenerations, indexCommit.getGeneration());
        return SoftDeleteIndexCommit.wrap(indexCommit, acquiredForCommitListener);
    }

    void incRefGeneration(Map<Long, Integer> counters, long generation) {
        counters.merge(generation, 1, Integer::sum);
    }

    /**
     * Decrements the reference count for the IndexCommit and returns whether it can be deleted.
     *
     * @param indexCommit the IndexCommit to decrement
     * @return {@code true} if the IndexCommit can be safely deleted, {@code false} otherwise
     */
    boolean decRef(IndexCommit indexCommit) {
        assert indexCommit instanceof SoftDeleteIndexCommit;
        if (Assertions.ENABLED && ((SoftDeleteIndexCommit) indexCommit).isAcquiredForCommitListener()) {
            decRefGeneration(acquiredCommitGenerationsForCommitsListener, indexCommit.getGeneration());
        }
        return decRefGeneration(acquiredGenerations, indexCommit.getGeneration());
    }

    private boolean decRefGeneration(Map<Long, Integer> counters, long generation) {
        assert counters.containsKey(generation) : generation;
        var refCount = counters.compute(generation, (ignored, value) -> {
            assert value != null : "already fully released";
            if (value == 1) {
                return null;
            }
            return value - 1;
        });
        assert refCount == null || refCount > 0;
        return refCount == null;
    }

    boolean hasAcquiredIndexCommitsForTesting() {
        // We explicitly check only external commits and disregard internal commits acquired by the commits listener
        for (var e : acquiredGenerations.entrySet()) {
            var commitListenerCount = acquiredCommitGenerationsForCommitsListener.get(e.getKey());
            if (commitListenerCount == null || e.getValue() > commitListenerCount) {
                return true;
            }
        }
        return false;
    }
}
