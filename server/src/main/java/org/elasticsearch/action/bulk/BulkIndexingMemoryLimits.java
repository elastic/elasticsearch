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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.atomic.AtomicLong;

public class BulkIndexingMemoryLimits implements CircuitBreaker {

    public static final Setting<ByteSizeValue> MAX_INDEXING_BYTES =
        Setting.memorySizeSetting("indices.memory.queued_indexing_bytes.limit", "25%", Setting.Property.NodeScope);

    private final Logger logger;
    private final AtomicLong trippedCount = new AtomicLong(0);
    private final long indexingLimits;
    private final long replicaLimits;

    private final AtomicLong pendingBytes = new AtomicLong(0);

    public BulkIndexingMemoryLimits(Settings settings) {
        this(settings, null);
    }

    public BulkIndexingMemoryLimits(Settings settings, Logger logger) {
        this.indexingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        this.logger = logger;
        this.replicaLimits = (long) (indexingLimits * 1.5);
    }

    public void markPrimaryOperationStarted(long bytes) {
        long pendingWithOperation = pendingBytes.addAndGet(bytes);

        if (pendingWithOperation > indexingLimits) {
            decrementPendingBytes(bytes);
            long pendingPreOperation = pendingWithOperation - bytes;
            throw new EsRejectedExecutionException("rejected execution of primary shard operation [" +
                "pending_bytes=" + pendingPreOperation + ", " +
                "operation_bytes=" + bytes + "," +
                "max_pending_bytes=" + indexingLimits + "]", false);
        }
    }

    public void markPrimaryOperationFinished(long bytes) {
        decrementPendingBytes(bytes);
    }

    public void markReplicaOperationStarted(long bytes) {
        pendingBytes.getAndAdd(bytes);
    }

    public void markReplicaOperationFinished(long bytes) {
        decrementPendingBytes(bytes);
    }

    private void decrementPendingBytes(long operationSizeInBytes) {
        pendingBytes.getAndAdd(-operationSizeInBytes);
    }

    public void addOperationAndMaybeBreak(long operationBytes) {
        addEstimateBytesAndMaybeBreak(operationBytes, "indexing", true);
    }

    public void addReplicationOperationAndMaybeBreak(long operationBytes) {
        addEstimateBytesAndMaybeBreak(operationBytes, "replication", true);
    }

    public void markOperationFinished(long bytes) {
        addWithoutBreaking(-bytes);
    }

    @Override
    public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        return addEstimateBytesAndMaybeBreak(bytes, label, false);
    }

    private double addEstimateBytesAndMaybeBreak(long bytes, String label, boolean isReplication) throws CircuitBreakingException {
        long byteLimit = isReplication ? replicaLimits : indexingLimits;
        long newUsed;
        long currentUsed;
        do {
            currentUsed = pendingBytes.get();
            newUsed = currentUsed + bytes;
            long newUsedWithOverhead = newUsed;
            if (logger.isTraceEnabled()) {
                logger.trace("[{}] Adding [{}][{}] to used bytes [new used: [{}], limit: {} [{}], estimate: {} [{}]]",
                    getName(),
                    new ByteSizeValue(bytes), label, new ByteSizeValue(newUsed),
                    byteLimit, new ByteSizeValue(byteLimit),
                    newUsedWithOverhead, new ByteSizeValue(newUsedWithOverhead));
            }
            if (byteLimit > 0 && newUsedWithOverhead > byteLimit) {
                logger.warn("[{}] New used memory {} [{}] for data of [{}] would be larger than configured breaker: {} [{}], breaking",
                    getName(),
                    newUsedWithOverhead, new ByteSizeValue(newUsedWithOverhead), label,
                    byteLimit, new ByteSizeValue(byteLimit));
                circuitBreak(label, newUsedWithOverhead, isReplication);
            }
            // Attempt to set the new used value, but make sure it hasn't changed
            // underneath us, if it has, keep trying until we are able to set it
        } while (!this.pendingBytes.compareAndSet(currentUsed, newUsed));
        return newUsed;
    }

    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        circuitBreak(fieldName, bytesNeeded, false);
    }

    private void circuitBreak(String fieldName, long bytesNeeded, boolean isReplication) {
        this.trippedCount.incrementAndGet();
        final long byteLimit;
        if (isReplication) {
            byteLimit = replicaLimits;
        } else {
            byteLimit = indexingLimits;
        }
        final String message = "[" + getName() + "] Data too large, data for [" + fieldName + "]" +
            " would be [" + bytesNeeded + "/" + new ByteSizeValue(bytesNeeded) + "]" +
            ", which is larger than the limit of [" +
            byteLimit + "/" + new ByteSizeValue(byteLimit) + "]";
        logger.debug("{}", message);
        throw new CircuitBreakingException(message, bytesNeeded, indexingLimits, getDurability());
    }

    @Override
    public long addWithoutBreaking(long bytes) {
        return pendingBytes.getAndAdd(bytes);
    }

    @Override
    public long getUsed() {
        return pendingBytes.get();
    }

    @Override
    public long getLimit() {
        return indexingLimits;
    }

    @Override
    public double getOverhead() {
        return 1.0;
    }

    @Override
    public long getTrippedCount() {
        return trippedCount.get();
    }

    @Override
    public String getName() {
        return CircuitBreaker.INDEXING;
    }

    @Override
    public Durability getDurability() {
        return CircuitBreaker.Durability.TRANSIENT;
    }
}
