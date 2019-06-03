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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShardSnapshotContext {

    private final Store store;

    private final ActionListener<Void> listener;

    private final IndexShardSnapshotStatus status;

    public ShardSnapshotContext(Store store, ActionListener<Void> listener, IndexShardSnapshotStatus status) {
        this.store = store;
        this.listener = listener;
        this.status = status;
    }

    public abstract IndexCommit indexCommit();

    public Store store() {
        return store;
    }

    public IndexShardSnapshotStatus status() {
        return status;
    }

    public ActionListener<Void> completionListener() {
        return listener;
    }

    public abstract void releaseIndexCommit() throws IOException;

    public static ShardSnapshotContext create(IndexShard indexShard, IndexShardSnapshotStatus snapshotStatus,
                                              ActionListener<Void> listener) {
        return new ShardSnapshotContext(indexShard.store(), listener, snapshotStatus) {

            private final AtomicBoolean closed = new AtomicBoolean(false);
            private Engine.IndexCommitRef snapshotRef;

            @Override
            public void releaseIndexCommit() throws IOException {
                if (closed.compareAndSet(false, true)) {
                    synchronized (this) {
                        if (snapshotRef != null) {
                            snapshotRef.close();
                        }
                    }
                }
            }

            @Override
            public IndexCommit indexCommit() {
                synchronized (this) {
                    if (closed.get()) {
                        throw new IllegalStateException("Tried to get index commit from closed context");
                    }
                    if (snapshotRef == null) {
                        snapshotRef = indexShard.acquireLastIndexCommit(true);
                    }
                    return snapshotRef.getIndexCommit();
                }
            }
        };
    }
}
