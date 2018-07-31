/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class BaseFuture<V> extends CompletableFuture<V> {

    private static final String BLOCKING_OP_REASON = "Blocking operation";

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            TimeoutException, ExecutionException {
        assert timeout <= 0 || blockingAllowed();
        return super.get(timeout, unit);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        assert blockingAllowed();
        return super.get();
    }

    private static boolean blockingAllowed() {
        return Transports.assertNotTransportThread(BLOCKING_OP_REASON) &&
            ThreadPool.assertNotScheduleThread(BLOCKING_OP_REASON) &&
            ClusterApplierService.assertNotClusterStateUpdateThread(BLOCKING_OP_REASON) &&
            MasterService.assertNotMasterUpdateThread(BLOCKING_OP_REASON);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        assert ex instanceof Exception : "Expected exception but was: " + ex.getClass();
        ExceptionsHelper.dieOnError(ex);
        return super.completeExceptionally(ex);
    }

}
