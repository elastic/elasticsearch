/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.engine.Engine;

/**
 * A (closeable) {@link IndexCommit} plus ref-counting to keep track of active users, and with the facility to drop the "main" initial ref
 * early if the shard snapshot is aborted.
 */
public class SnapshotIndexCommit extends AbstractRefCounted {

    private final Engine.IndexCommitRef commitRef;
    private final Runnable releaseInitialRef;
    @Nullable
    private Exception closeException;

    public SnapshotIndexCommit(Engine.IndexCommitRef commitRef) {
        this.commitRef = commitRef;
        this.releaseInitialRef = new RunOnce(this::decRef);
    }

    @Override
    protected void closeInternal() {
        assert closeException == null : closeException;
        try {
            commitRef.close();
        } catch (Exception e) {
            closeException = e;
        }
    }

    /**
     * Called after all other refs are released, to release the initial ref (if not already released) and re-throw any exception thrown
     * when the inner {@link IndexCommit} was closed.
     */
    public void onCompletion() throws Exception {
        releaseInitialRef.run();
        assert hasReferences() == false;
        // closeInternal happens-before here so no need for synchronization
        if (closeException != null) {
            throw closeException;
        }
    }

    /**
     * Called to abort the snapshot while it's running: release the initial ref (if not already released).
     */
    public void onAbort() {
        releaseInitialRef.run();
    }

    public IndexCommit indexCommit() {
        assert hasReferences();
        return commitRef.getIndexCommit();
    }
}
