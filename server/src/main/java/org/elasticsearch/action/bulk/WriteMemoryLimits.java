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
        Setting.memorySizeSetting("indices.write.limit", "10%", Setting.Property.NodeScope, Setting.Property.Dynamic);

    private final AtomicLong writeBytes = new AtomicLong(0);
    private final AtomicLong replicaWriteBytes = new AtomicLong(0);
    private volatile long writeLimits;

    public WriteMemoryLimits(Settings settings, ClusterSettings clusterSettings) {
        this.writeLimits = MAX_INDEXING_BYTES.get(settings).getBytes();
        clusterSettings.addSettingsUpdateConsumer(MAX_INDEXING_BYTES, value -> writeLimits = value.getBytes());
    }

    public Releasable markWriteOperationStarted(long bytes) {
        long currentBytes = writeBytes.addAndGet(bytes);
        if (currentBytes > writeLimits) {
            writeBytes.getAndAdd(-bytes);
            throw new EsRejectedExecutionException();
        }
        return () -> writeBytes.getAndAdd(-bytes);
    }

    public long getWriteBytes() {
        return writeBytes.get();
    }

    public Releasable markReplicaWriteStarted(long bytes) {
        replicaWriteBytes.getAndAdd(bytes);
        return () -> replicaWriteBytes.getAndAdd(-bytes);
    }

    public long getReplicaWriteBytes() {
        return replicaWriteBytes.get();
    }
}
