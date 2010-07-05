/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.translog.memory;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadSafe;
import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (shay.banon)
 */
@ThreadSafe
public class MemoryTranslog extends AbstractIndexShardComponent implements Translog {

    private final Object mutex = new Object();

    private final AtomicLong estimatedMemorySize = new AtomicLong();

    private volatile long id;

    private volatile Queue<Operation> operations;

    private final AtomicInteger operationCounter = new AtomicInteger();

    @Inject public MemoryTranslog(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    @Override public long currentId() {
        return this.id;
    }

    @Override public int size() {
        return operations.size();
    }

    @Override public ByteSizeValue estimateMemorySize() {
        return new ByteSizeValue(estimatedMemorySize.get(), ByteSizeUnit.BYTES);
    }

    @Override public void newTranslog(long id) {
        synchronized (mutex) {
            estimatedMemorySize.set(0);
            operations = new LinkedTransferQueue<Operation>();
            operationCounter.set(0);
            this.id = id;
        }
    }

    @Override public void add(Operation operation) throws TranslogException {
        operations.add(operation);
        operationCounter.incrementAndGet();
        estimatedMemorySize.addAndGet(operation.estimateSize() + 50);
    }

    @Override public Snapshot snapshot() {
        synchronized (mutex) {
            return new MemorySnapshot(currentId(), operations, operationCounter.get());
        }
    }

    @Override public Snapshot snapshot(Snapshot snapshot) {
        synchronized (mutex) {
            MemorySnapshot memorySnapshot = (MemorySnapshot) snapshot;
            if (currentId() != snapshot.translogId()) {
                return snapshot();
            }
            MemorySnapshot newSnapshot = new MemorySnapshot(currentId(), operations, operationCounter.get());
            newSnapshot.seekForward(memorySnapshot.length());
            return newSnapshot;
        }
    }

    @Override public void close() {
    }
}
