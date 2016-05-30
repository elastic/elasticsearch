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
package org.elasticsearch.recovery;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveriesCollection;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.indices.recovery.RecoveryTargetService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class RecoveriesCollectionTests extends ESSingleNodeTestCase {
    final static RecoveryTargetService.RecoveryListener listener = new RecoveryTargetService.RecoveryListener() {
        @Override
        public void onRecoveryDone(RecoveryState state) {

        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {

        }
    };

    public void testLastAccessTimeUpdate() throws Exception {
        createIndex();
        final RecoveriesCollection collection = new RecoveriesCollection(logger, getInstanceFromNode(ThreadPool.class));
        final long recoveryId = startRecovery(collection);
        try (RecoveriesCollection.RecoveryRef status = collection.getRecovery(recoveryId)) {
            final long lastSeenTime = status.status().lastAccessTime();
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    try (RecoveriesCollection.RecoveryRef currentStatus = collection.getRecovery(recoveryId)) {
                        assertThat("access time failed to update", lastSeenTime, lessThan(currentStatus.status().lastAccessTime()));
                    }
                }
            });
        } finally {
            collection.cancelRecovery(recoveryId, "life");
        }
    }

    public void testRecoveryTimeout() throws InterruptedException {
        createIndex();
        final RecoveriesCollection collection = new RecoveriesCollection(logger, getInstanceFromNode(ThreadPool.class));
        final AtomicBoolean failed = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        final long recoveryId = startRecovery(collection, new RecoveryTargetService.RecoveryListener() {
            @Override
            public void onRecoveryDone(RecoveryState state) {
                latch.countDown();
            }

            @Override
            public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {
                failed.set(true);
                latch.countDown();
            }
        }, TimeValue.timeValueMillis(100));
        try {
            latch.await(30, TimeUnit.SECONDS);
            assertTrue("recovery failed to timeout", failed.get());
        } finally {
            collection.cancelRecovery(recoveryId, "meh");
        }

    }

    public void testRecoveryCancellation() throws Exception {
        createIndex();
        final RecoveriesCollection collection = new RecoveriesCollection(logger, getInstanceFromNode(ThreadPool.class));
        final long recoveryId = startRecovery(collection);
        final long recoveryId2 = startRecovery(collection);
        try (RecoveriesCollection.RecoveryRef recoveryRef = collection.getRecovery(recoveryId)) {
            ShardId shardId = recoveryRef.status().shardId();
            assertTrue("failed to cancel recoveries", collection.cancelRecoveriesForShard(shardId, "test"));
            assertThat("all recoveries should be cancelled", collection.size(), equalTo(0));
        } finally {
            collection.cancelRecovery(recoveryId, "meh");
            collection.cancelRecovery(recoveryId2, "meh");
        }
    }

    protected void createIndex() {
        createIndex("test",
                Settings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build());
        ensureGreen();
    }


    long startRecovery(RecoveriesCollection collection) {
        return startRecovery(collection, listener, TimeValue.timeValueMinutes(60));
    }

    long startRecovery(RecoveriesCollection collection, RecoveryTargetService.RecoveryListener listener, TimeValue timeValue) {
        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = indexServices.indexServiceSafe(resolveIndex("test")).getShardOrNull(0);
        final DiscoveryNode sourceNode = new DiscoveryNode("id", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        return collection.startRecovery(indexShard, sourceNode, listener, timeValue);
    }
}
