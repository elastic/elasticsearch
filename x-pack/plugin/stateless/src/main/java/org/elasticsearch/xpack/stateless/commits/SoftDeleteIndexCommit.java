/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.common.lucene.FilterIndexCommit;

import java.util.concurrent.atomic.AtomicBoolean;

public class SoftDeleteIndexCommit extends FilterIndexCommit {

    private final AtomicBoolean softDelete = new AtomicBoolean();
    private final boolean acquiredForCommitListener;

    private SoftDeleteIndexCommit(IndexCommit in, boolean acquiredForCommitListener) {
        super(in);
        this.acquiredForCommitListener = acquiredForCommitListener;
    }

    public boolean isSoftDeleted() {
        return softDelete.get();
    }

    @Override
    public void delete() {
        softDelete.compareAndSet(false, true);
        // Suppress any deletion executed by a wrapped index deletion policy.
        // We do not call super.delete() here to avoid deleting the commit immediately,
        // the commit is deleted once all references to it are released.
    }

    boolean isAcquiredForCommitListener() {
        return acquiredForCommitListener;
    }

    @Override
    public String toString() {
        return "SoftDeleteIndexCommit[" + in.getGeneration() + (isSoftDeleted() ? "](soft deleted)" : "]");
    }

    public static SoftDeleteIndexCommit wrap(IndexCommit commit, boolean acquiredForCommitListener) {
        assert commit instanceof SoftDeleteIndexCommit == false : commit.getClass().getName();
        return new SoftDeleteIndexCommit(commit, acquiredForCommitListener);
    }

    public static IndexCommit unwrap(IndexCommit commit) {
        if (commit instanceof SoftDeleteIndexCommit softDeleteIndexCommit) {
            return softDeleteIndexCommit.getIndexCommit();
        }
        var error = "[" + commit.getClass().getName() + "] is not an instance of [" + SoftDeleteIndexCommit.class.getName() + ']';
        assert false : error;
        throw new IllegalStateException(error);
    }
}
