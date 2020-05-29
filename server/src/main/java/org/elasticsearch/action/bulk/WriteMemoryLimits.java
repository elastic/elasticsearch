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

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.atomic.AtomicLong;

public class WriteMemoryLimits {

    public static final Setting<ByteSizeValue> MAX_INDEXING_BYTES =
        Setting.memorySizeSetting("indices.indexing_bytes.limit", "20%", Setting.Property.NodeScope, Setting.Property.Dynamic);

    private volatile long indexingLimits;

    private final AtomicLong pendingBytes = new AtomicLong(0);

    public WriteMemoryLimits(Settings settings, ClusterSettings clusterSettings) {
        this.indexingLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        clusterSettings.addSettingsUpdateConsumer(MAX_INDEXING_BYTES, value -> indexingLimits = value.getBytes());
    }

    public void markPrimaryOperationStarted(long bytes, boolean forceExecution) {
        long pendingWithOperation = pendingBytes.addAndGet(bytes);

        if (forceExecution== false && pendingWithOperation > indexingLimits) {
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
}
