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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveriesCollection;
import org.elasticsearch.indices.recovery.RecoveryFailedException;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.lessThan;

public class RecoveriesCollectionTests extends ElasticsearchSingleNodeTest {

    final static RecoveryTarget.RecoveryListener listener = new RecoveryTarget.RecoveryListener() {
        @Override
        public void onRecoveryDone(RecoveryState state) {

        }

        @Override
        public void onRecoveryFailure(RecoveryState state, RecoveryFailedException e, boolean sendShardFailure) {

        }
    };

    @Test
    public void testLastAccessTimeUpdate() throws Exception {
        createIndex("test",
                ImmutableSettings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build());
        ensureGreen();
        final RecoveriesCollection collection = new RecoveriesCollection(logger, getInstanceFromNode(ThreadPool.class));
        final long recoveryId = startRecovery(collection);
        try (RecoveriesCollection.StatusRef status = collection.getStatus(recoveryId)) {
            final long lastSeenTime = status.status().lastAccessTime();
            assertBusy(new Runnable() {
                @Override
                public void run() {
                    try (RecoveriesCollection.StatusRef currentStatus = collection.getStatus(recoveryId)) {
                        assertThat("access time failed to update", lastSeenTime, lessThan(currentStatus.status().lastAccessTime()));
                    }
                }
            });
        } finally {
            collection.cancelRecovery(recoveryId, "life");
        }
    }

    @Test
    public void testRecoveryTimeout() throws InterruptedException {
        createIndex("test",
                ImmutableSettings.builder()
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1, IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build());
        ensureGreen();
        final RecoveriesCollection collection = new RecoveriesCollection(logger, getInstanceFromNode(ThreadPool.class));
        final AtomicBoolean failed = new AtomicBoolean();
        final CountDownLatch latch = new CountDownLatch(1);
        final long recoveryId = startRecovery(collection, new RecoveryTarget.RecoveryListener() {
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

    long startRecovery(RecoveriesCollection collection) {
        return startRecovery(collection, listener, TimeValue.timeValueMinutes(60));
    }

    long startRecovery(RecoveriesCollection collection, RecoveryTarget.RecoveryListener listener, TimeValue timeValue) {
        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = indexServices.indexServiceSafe("test").shard(0);
        return collection.startRecovery(
                indexShard, new DiscoveryNode("id", DummyTransportAddress.INSTANCE, Version.CURRENT),
                new RecoveryState(indexShard.shardId()), listener, timeValue);
    }

}
