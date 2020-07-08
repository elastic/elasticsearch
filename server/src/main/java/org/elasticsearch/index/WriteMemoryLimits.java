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

package org.elasticsearch.index;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.atomic.AtomicLong;

public class WriteMemoryLimits {

    public static final Setting<ByteSizeValue> MAX_INDEXING_BYTES =
        Setting.memorySizeSetting("indexing_limits.memory.limit", "10%", Setting.Property.NodeScope);

    private final AtomicLong primaryAndCoordinatingBytes = new AtomicLong(0);
    private final AtomicLong replicaBytes = new AtomicLong(0);
    private final long primaryAndCoordinatingLimits;
    private final long replicaLimits;

    public WriteMemoryLimits(Settings settings) {
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.replicaLimits = (long) (this.primaryAndCoordinatingLimits * 1.5);
    }

    public Releasable markWriteOperationStarted(long bytes) {
        return markWriteOperationStarted(bytes, false);
    }

    public Releasable markWriteOperationStarted(long bytes, boolean forceExecution) {
        long writeBytes = this.primaryAndCoordinatingBytes.addAndGet(bytes);
        long replicaWriteBytes = this.replicaBytes.get();
        long totalBytes = writeBytes + replicaWriteBytes;
        if (forceExecution == false && totalBytes > primaryAndCoordinatingLimits) {
            long bytesWithoutOperation = writeBytes - bytes;
            long totalBytesWithoutOperation = totalBytes - bytes;
            this.primaryAndCoordinatingBytes.getAndAdd(-bytes);
            throw new EsRejectedExecutionException("rejected execution of operation [" +
                "primary_and_coordinating_bytes=" + bytesWithoutOperation + ", " +
                "replica_bytes=" + replicaWriteBytes + ", " +
                "total_bytes=" + totalBytesWithoutOperation + ", " +
                "current_operation_bytes=" + bytes + ", " +
                "max_primary_and_coordinating_bytes=" + primaryAndCoordinatingLimits + "]", false);
        }
        return () -> this.primaryAndCoordinatingBytes.getAndAdd(-bytes);
    }

    public long getPrimaryAndCoordinatingBytes() {
        return primaryAndCoordinatingBytes.get();
    }

    public Releasable markReplicaWriteStarted(long bytes, boolean forceExecution) {
        long replicaWriteBytes = this.replicaBytes.getAndAdd(bytes);
        if (forceExecution == false && replicaWriteBytes > replicaLimits) {
            long replicaBytesWithoutOperation = replicaWriteBytes - bytes;
            this.replicaBytes.getAndAdd(-bytes);
            throw new EsRejectedExecutionException("rejected execution of replica operation [" +
                "replica_bytes=" + replicaBytesWithoutOperation + ", " +
                "current_replica_operation_bytes=" + bytes + ", " +
                "max_replica_bytes=" + replicaLimits + "]", false);
        }
        return () -> this.replicaBytes.getAndAdd(-bytes);
    }

    public long getReplicaBytes() {
        return replicaBytes.get();
    }
}
