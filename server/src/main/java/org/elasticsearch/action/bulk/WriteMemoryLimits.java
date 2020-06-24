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

import java.util.concurrent.atomic.AtomicLong;

public class WriteMemoryLimits {

    private final AtomicLong coordinatingBytes = new AtomicLong(0);
    private final AtomicLong primaryBytes = new AtomicLong(0);
    private final AtomicLong replicaBytes = new AtomicLong(0);

    public Releasable markCoordinatingOperationStarted(long bytes) {
        coordinatingBytes.addAndGet(bytes);
        return () -> coordinatingBytes.getAndAdd(-bytes);
    }

    public long getCoordinatingBytes() {
        return coordinatingBytes.get();
    }

    public Releasable markPrimaryOperationStarted(long bytes) {
        primaryBytes.addAndGet(bytes);
        return () -> primaryBytes.getAndAdd(-bytes);
    }

    public long getPrimaryBytes() {
        return primaryBytes.get();
    }

    public Releasable markReplicaOperationStarted(long bytes) {
        replicaBytes.getAndAdd(bytes);
        return () -> replicaBytes.getAndAdd(-bytes);
    }

    public long getReplicaBytes() {
        return replicaBytes.get();
    }
}
