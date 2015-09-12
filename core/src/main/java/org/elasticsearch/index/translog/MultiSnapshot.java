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

package org.elasticsearch.index.translog;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasables;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A snapshot composed out of multiple snapshots
 */
final class MultiSnapshot implements Translog.Snapshot {

    private final Translog.Snapshot[] translogs;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private final int estimatedTotalOperations;
    private int index;

    /**
     * Creates a new point in time snapshot of the given snapshots. Those snapshots are always iterated in-order.
     */
    MultiSnapshot(Translog.Snapshot[] translogs) {
        this.translogs = translogs;
        int ops = 0;
        for (Translog.Snapshot translog : translogs) {

            final int tops = translog.estimatedTotalOperations();
            if (tops == TranslogReader.UNKNOWN_OP_COUNT) {
                ops = TranslogReader.UNKNOWN_OP_COUNT;
                break;
            }
            assert tops >= 0 : "tops must be positive but was: " + tops;
            ops += tops;
        }
        estimatedTotalOperations = ops;
        index = 0;
    }


    @Override
    public int estimatedTotalOperations() {
        return estimatedTotalOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        ensureOpen();
        for (; index < translogs.length; index++) {
            final Translog.Snapshot current = translogs[index];
            Translog.Operation op = current.next();
            if (op != null) { // if we are null we move to the next snapshot
                return op;
            }
        }
        return null;
    }

    protected void ensureOpen() {
        if (closed.get()) {
            throw new AlreadyClosedException("snapshot already closed");
        }
    }

    @Override
    public void close() throws ElasticsearchException {
        if (closed.compareAndSet(false, true)) {
            Releasables.close(translogs);
        }
    }
}
