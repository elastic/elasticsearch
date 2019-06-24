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

package org.elasticsearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Manages the {@link IndexCommit} associated with the shard snapshot as well as its {@link IndexShardSnapshotStatus} instance.
 */
public abstract class ShardSnapshotContext {

    private final Store store;

    private final ActionListener<Void> listener;

    private final IndexShardSnapshotStatus status;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected ShardSnapshotContext(Store store, ActionListener<Void> listener, IndexShardSnapshotStatus status) {
        this.store = store;
        this.listener = listener;
        this.status = status;
    }

    /**
     * Create and return an {@link IndexCommit} for this shard. Repeated invocations of this method return the same {@link IndexCommit}.
     * The resources associated with this {@link IndexCommit} are released by {@link #releaseIndexCommit()} when either
     * {@link #finish(long, String, Exception)} or {@link #prepareFinalize()} is invoked.
     * @return IndexCommit index commit
     * @throws IOException on failure
     */
    public IndexCommit indexCommit() throws IOException {
        synchronized (this) {
            if (closed.get()) {
                throw new IllegalStateException("Tried to get index commit from closed context.");
            }
            return doIndexCommit();
        }
    }

    /**
     * See {@link #indexCommit()}
     */
    protected abstract IndexCommit doIndexCommit() throws IOException;

    /**
     * Release resources backing the {@link IndexCommit} returned by {@link #indexCommit()}.
     * @throws IOException on failure
     */
    private void releaseIndexCommit() throws IOException {
        if (closed.compareAndSet(false, true)) {
            synchronized (this) {
                doReleaseIndexCommit();
            }
        }
    }

    /**
     * See {@link #releaseIndexCommit()}
     */
    protected abstract void doReleaseIndexCommit() throws IOException;

    public Store store() {
        return store;
    }

    /**
     * Invoke once all writes to the repository have finished for the shard.
     * @param endTime Timestamp of when the shard snapshot's writes to the repository finished
     */
    public void finish(long endTime) {
        status.moveToDone(endTime);
        listener.onResponse(null);
    }

    /**
     * Invoke once all segments for this shard were written to the repository.
     * @return IndexSnapshotStatus right after writing all segments to the repository
     * @throws IOException On failure to release the resources backing this instance's {@link IndexCommit}
     */
    public final IndexShardSnapshotStatus.Copy prepareFinalize() throws IOException {
        final IndexShardSnapshotStatus.Copy lastSnapshotStatus = status.moveToFinalize(indexCommit().getGeneration());
        releaseIndexCommit();
        return lastSnapshotStatus;
    }

    /**
     * Invoke in case the shard's snapshot operation failed.
     * @param endTime time the shard's snapshot failed
     * @param failureMessage failure message
     * @param e Exception that caused the shard's snapshot to fail
     */
    public final void finish(long endTime, String failureMessage, Exception e) {
        status.moveToFailed(endTime, failureMessage);
        try {
            releaseIndexCommit();
        } catch (Exception ex) {
            e.addSuppressed(ex);
        }
        listener.onFailure(e);
    }

    public IndexShardSnapshotStatus status() {
        return status;
    }

    public ActionListener<Void> completionListener() {
        return listener;
    }

    public interface IndexCommitProvider extends Closeable, Supplier<IndexCommit> {
    }

    public static ShardSnapshotContext create(IndexShard indexShard, IndexShardSnapshotStatus snapshotStatus,
                                              ActionListener<Void> listener) {
        return new ShardSnapshotContext(indexShard.store(), listener, snapshotStatus) {

            private Engine.IndexCommitRef snapshotRef;

            @Override
            protected void doReleaseIndexCommit() throws IOException {
                if (snapshotRef != null) {
                    snapshotRef.close();
                }
            }

            @Override
            protected IndexCommit doIndexCommit() {
                if (snapshotRef == null) {
                    snapshotRef = indexShard.acquireLastIndexCommit(true);
                }
                return snapshotRef.getIndexCommit();
            }
        };
    }
}
