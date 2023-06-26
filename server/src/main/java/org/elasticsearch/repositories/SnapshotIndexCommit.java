/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.index.engine.Engine;

/**
 * A (closeable) {@link IndexCommit} plus ref-counting to keep track of active users, and with the facility to drop the "main" initial ref
 * early if the shard snapshot is aborted.
 */
public class SnapshotIndexCommit extends AbstractRefCounted {

    private final Engine.IndexCommitRef commitRef;
    private final Runnable releaseInitialRef;
    private final SubscribableListener<Void> completionListeners = new SubscribableListener<>();

    public SnapshotIndexCommit(Engine.IndexCommitRef commitRef) {
        this.commitRef = commitRef;
        this.releaseInitialRef = new RunOnce(this::decRef);
    }

    @Override
    protected void closeInternal() {
        assert completionListeners.isDone() == false;
        ActionListener.completeWith(completionListeners, () -> {
            commitRef.close();
            return null;
        });
    }

    /**
     * Called after all other refs are released, to release the initial ref (if not already released) and expose any exception thrown
     * when the inner {@link IndexCommit} was closed.
     */
    private void onCompletion(ActionListener<Void> completionListener) {
        releaseInitialRef.run();
        assert refCount() <= 1; // almost always zero here, but may not be if a concurrent abort has not run the last decRef() yet
        completionListeners.addListener(completionListener); // completed directly or by a concurrently-running aborting thread
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

    /**
     * Returns a listener which closes this commit before completing the delegate listener, marshalling exceptions to the delegate as
     * appropriate.
     */
    public <T> ActionListener<T> closingBefore(ActionListener<T> delegate) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T result) {
                onCompletion(delegate.map(ignored -> result));
            }

            @Override
            public void onFailure(Exception e) {
                onCompletion(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        delegate.onFailure(e);
                    }

                    @Override
                    public void onFailure(Exception e2) {
                        if (e2 != e) {
                            e.addSuppressed(e2);
                        }
                        delegate.onFailure(e);
                    }
                });
            }
        };
    }
}
