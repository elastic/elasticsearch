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

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.concurrent.atomic.AtomicLong;

public class BulkIndexingBreaker {

    // TODO: Configurable? Allow dedicating coordinating nodes to have a higher value?
    private static final long FORTY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.6);
    private static final long THIRTY_PER_HEAP_SIZE = (long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.3);

    private final AtomicLong pendingBytes = new AtomicLong(0);

    public void markCoordinatingOperationStarted(long bytes) {
        long pendingWithOperation = pendingBytes.addAndGet(bytes);

        if (pendingWithOperation > FORTY_PER_HEAP_SIZE) {
            decrementPendingBytes(bytes);
            long pendingPreOperation = pendingWithOperation - bytes;
            throw new EsRejectedExecutionException("rejected execution of coordinating indexing operation [" +
                "pending_bytes=" + pendingPreOperation + ", " +
                "operation_bytes=" + bytes + "," +
                "max_pending_bytes=" + FORTY_PER_HEAP_SIZE + "]", false);
        }
    }

    public void markCoordinatingOperationFinished(long bytes) {
        decrementPendingBytes(bytes);
    }

    public void markPrimaryOperationStarted(long bytes) {
        long pendingWithOperation = pendingBytes.addAndGet(bytes);

        if (pendingWithOperation > THIRTY_PER_HEAP_SIZE) {
            decrementPendingBytes(bytes);
            long pendingPreOperation = pendingWithOperation - bytes;
            throw new EsRejectedExecutionException("rejected execution of primary shard operation [" +
                "pending_bytes=" + pendingPreOperation + ", " +
                "operation_bytes=" + bytes + "," +
                "max_pending_bytes=" + THIRTY_PER_HEAP_SIZE + "]", false);
        }
    }

    public void markPrimaryOperationFinished(long bytes) {
        decrementPendingBytes(bytes);
    }

    public void markReplicaOperationStarted(long bytes) {
        long pendingWithOperation = pendingBytes.addAndGet(bytes);

        if (pendingWithOperation > FORTY_PER_HEAP_SIZE) {
            decrementPendingBytes(bytes);
            long pendingPreOperation = pendingWithOperation - bytes;
            throw new EsRejectedExecutionException("rejected execution of replica shard operation [" +
                "pending_bytes=" + pendingPreOperation + ", " +
                "operation_bytes=" + bytes + "," +
                "max_pending_bytes=" + FORTY_PER_HEAP_SIZE + "]", false);
        }
    }

    public void markReplicaOperationFinished(long bytes) {
        decrementPendingBytes(bytes);
    }

    private void decrementPendingBytes(long operationSizeInBytes) {
        pendingBytes.getAndAdd(-operationSizeInBytes);
    }
}
