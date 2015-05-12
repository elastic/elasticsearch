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
package org.elasticsearch.test;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SyncedFlushService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Utils for SyncedFlush */
public class SyncedFlushUtil {

    private SyncedFlushUtil() {

    }

    /**
     * Blocking version of {@link SyncedFlushService#attemptSyncedFlush(ShardId, ActionListener)}
     */
    public static SyncedFlushService.SyncedFlushResult attemptSyncedFlush(SyncedFlushService service, ShardId shardId) {
        final CountDownLatch countDown = new CountDownLatch(1);
        final AtomicReference<SyncedFlushService.SyncedFlushResult> result = new AtomicReference<>();
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        service.attemptSyncedFlush(shardId, new ActionListener<SyncedFlushService.SyncedFlushResult>() {
            @Override
            public void onResponse(SyncedFlushService.SyncedFlushResult syncedFlushResult) {
                result.compareAndSet(null, syncedFlushResult);
                countDown.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                exception.compareAndSet(null, e);
                countDown.countDown();
            }
        });
        try {
            countDown.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (exception.get() != null) {
            throw ExceptionsHelper.convertToElastic(exception.get());
        }
        return result.get();
    }

}
