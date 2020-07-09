/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.atomic.AtomicLong;

public class WriteMemoryLimits {

    public static final Setting<ByteSizeValue> MAX_INDEXING_BYTES =
        Setting.memorySizeSetting("indexing_limits.memory.limit", "10%", Setting.Property.NodeScope);

    private final AtomicLong writeBytes = new AtomicLong(0);
    private final AtomicLong replicaWriteBytes = new AtomicLong(0);
    private final long writeLimits;

    public WriteMemoryLimits(Settings settings) {
        this.writeLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
    }

    public WriteMemoryLimits(Settings settings, ClusterSettings clusterSettings) {
        this.writeLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
    }

    public Releasable markWriteOperationStarted(long bytes) {
        return markWriteOperationStarted(bytes, false);
    }

    public Releasable markWriteOperationStarted(long bytes, boolean forceExecution) {
        long currentWriteLimits = this.writeLimits;
        long writeBytes = this.writeBytes.addAndGet(bytes);
        long replicaWriteBytes = this.replicaWriteBytes.get();
        long totalBytes = writeBytes + replicaWriteBytes;
        if (forceExecution == false && totalBytes > currentWriteLimits) {
            long bytesWithoutOperation = writeBytes - bytes;
            long totalBytesWithoutOperation = totalBytes - bytes;
            this.writeBytes.getAndAdd(-bytes);
            throw new EsRejectedExecutionException("rejected execution of write operation [" +
                "write_bytes=" + bytesWithoutOperation + ", " +
                "replica_write_bytes=" + replicaWriteBytes + ", " +
                "total_write_bytes=" + totalBytesWithoutOperation + ", " +
                "current_operation_bytes=" + bytes + ", " +
                "max_write_bytes=" + currentWriteLimits + "]", false);
        }
        return () -> this.writeBytes.getAndAdd(-bytes);
    }

    public long getWriteBytes() {
        return writeBytes.get();
    }

    public Releasable markReplicaWriteStarted(long bytes, boolean forceExecution) {
        long currentReplicaWriteLimits = (long) (this.writeLimits * 1.5);
        long replicaWriteBytes = this.replicaWriteBytes.getAndAdd(bytes);
        if (forceExecution == false && replicaWriteBytes > currentReplicaWriteLimits) {
            long replicaBytesWithoutOperation = replicaWriteBytes - bytes;
            this.replicaWriteBytes.getAndAdd(-bytes);
            throw new EsRejectedExecutionException("rejected execution of replica write operation [" +
                "replica_write_bytes=" + replicaBytesWithoutOperation + ", " +
                "current_replica_operation_bytes=" + bytes + ", " +
                "max_replica_write_bytes=" + currentReplicaWriteLimits + "]", false);
        }
        return () -> this.replicaWriteBytes.getAndAdd(-bytes);
    }

    public long getReplicaWriteBytes() {
        return replicaWriteBytes.get();
    }
}
