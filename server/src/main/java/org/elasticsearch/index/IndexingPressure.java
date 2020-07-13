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
import org.elasticsearch.index.stats.IndexingPressureStats;

import java.util.concurrent.atomic.AtomicLong;

public class IndexingPressure {

    public static final Setting<ByteSizeValue> MAX_INDEXING_BYTES =
        Setting.memorySizeSetting("indexing_pressure.memory.limit", "10%", Setting.Property.NodeScope);

    private final AtomicLong currentCoordinatingAndPrimaryBytes = new AtomicLong(0);
    private final AtomicLong currentReplicaBytes = new AtomicLong(0);
    private final AtomicLong totalCoordinatingAndPrimaryBytes = new AtomicLong(0);
    private final AtomicLong totalReplicaBytes = new AtomicLong(0);
    private final AtomicLong coordinatingAndPrimaryRejections = new AtomicLong(0);
    private final AtomicLong replicaRejections = new AtomicLong(0);

    private final long primaryAndCoordinatingLimits;
    private final long replicaLimits;

    public IndexingPressure(Settings settings) {
        this.primaryAndCoordinatingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.replicaLimits = (long) (this.primaryAndCoordinatingLimits * 1.5);
    }

    public Releasable markIndexingOperationStarted(long bytes) {
        return markIndexingOperationStarted(bytes, false);
    }

    public Releasable markIndexingOperationStarted(long bytes, boolean forceExecution) {
        long writeBytes = this.currentCoordinatingAndPrimaryBytes.addAndGet(bytes);
        long replicaWriteBytes = this.currentReplicaBytes.get();
        long totalBytes = writeBytes + replicaWriteBytes;
        if (forceExecution == false && totalBytes > primaryAndCoordinatingLimits) {
            long bytesWithoutOperation = writeBytes - bytes;
            long totalBytesWithoutOperation = totalBytes - bytes;
            this.currentCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
            this.coordinatingAndPrimaryRejections.getAndIncrement();
            throw new EsRejectedExecutionException("rejected execution of operation [" +
                "coordinating_and_primary_bytes=" + bytesWithoutOperation + ", " +
                "replica_bytes=" + replicaWriteBytes + ", " +
                "all_bytes=" + totalBytesWithoutOperation + ", " +
                "operation_bytes=" + bytes + ", " +
                "max_coordinating_and_primary_bytes=" + primaryAndCoordinatingLimits + "]", false);
        }
        totalCoordinatingAndPrimaryBytes.getAndAdd(bytes);
        return () -> this.currentCoordinatingAndPrimaryBytes.getAndAdd(-bytes);
    }

    public Releasable markReplicaOperationStarted(long bytes, boolean forceExecution) {
        long replicaWriteBytes = this.currentReplicaBytes.getAndAdd(bytes);
        if (forceExecution == false && replicaWriteBytes > replicaLimits) {
            long replicaBytesWithoutOperation = replicaWriteBytes - bytes;
            this.currentReplicaBytes.getAndAdd(-bytes);
            this.replicaRejections.getAndIncrement();
            throw new EsRejectedExecutionException("rejected execution of replica operation [" +
                "replica_bytes=" + replicaBytesWithoutOperation + ", " +
                "replica_operation_bytes=" + bytes + ", " +
                "max_replica_bytes=" + replicaLimits + "]", false);
        }
        totalReplicaBytes.getAndAdd(bytes);
        return () -> this.currentReplicaBytes.getAndAdd(-bytes);
    }

    public long getCurrentCoordinatingAndPrimaryBytes() {
        return currentCoordinatingAndPrimaryBytes.get();
    }

    public long getCurrentReplicaBytes() {
        return currentReplicaBytes.get();
    }

    public long getTotalCoordinatingAndPrimaryBytes() {
        return totalCoordinatingAndPrimaryBytes.get();
    }

    public long getTotalReplicaBytes() {
        return totalReplicaBytes.get();
    }

    public IndexingPressureStats stats() {
        return new IndexingPressureStats(totalCoordinatingAndPrimaryBytes.get(), totalReplicaBytes.get(),
            currentCoordinatingAndPrimaryBytes.get(), currentReplicaBytes.get(), coordinatingAndPrimaryRejections.get(),
            replicaRejections.get());
    }
}
