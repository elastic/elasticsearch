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

import com.google.inject.Inject;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.settings.Settings;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public class MemoryTranslog extends AbstractIndexShardComponent implements Translog {

    private final Object mutex = new Object();

    private final AtomicLong idGenerator = new AtomicLong();

    private final AtomicLong estimatedMemorySize = new AtomicLong();

    private volatile long id;

    private final ConcurrentLinkedQueue<Operation> operations = new ConcurrentLinkedQueue<Operation>();

    @Inject public MemoryTranslog(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
        newTranslog();
    }

    @Override public long currentId() {
        return this.id;
    }

    @Override public int size() {
        return operations.size();
    }

    @Override public SizeValue estimateMemorySize() {
        return new SizeValue(estimatedMemorySize.get(), SizeUnit.BYTES);
    }

    @Override public void newTranslog() {
        synchronized (mutex) {
            estimatedMemorySize.set(0);
            operations.clear();
            id = idGenerator.getAndIncrement();
        }
    }

    @Override public void add(Operation operation) throws TranslogException {
        operations.add(operation);
        estimatedMemorySize.addAndGet(operation.estimateSize() + 20);
    }

    @Override public Snapshot snapshot() {
        synchronized (mutex) {
            return new MemorySnapshot(currentId(), operations.toArray(new Operation[0]));
        }
    }

    @Override public Snapshot snapshot(Snapshot snapshot) {
        synchronized (mutex) {
            MemorySnapshot memorySnapshot = (MemorySnapshot) snapshot;
            if (currentId() != snapshot.translogId()) {
                return snapshot();
            }
            ArrayList<Operation> retVal = new ArrayList<Operation>();
            int counter = 0;
            int snapshotSize = memorySnapshot.operations.length;
            for (Operation operation : operations) {
                if (++counter > snapshotSize) {
                    retVal.add(operation);
                }
            }
            return new MemorySnapshot(currentId(), retVal.toArray(new Operation[retVal.size()]));
        }
    }

    @Override public void close() {
    }

}
