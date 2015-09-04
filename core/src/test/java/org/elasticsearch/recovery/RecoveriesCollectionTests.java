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
import org.elasticsearch.indices.recovery.RecoveryStatus;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class RecoveriesCollectionTests extends ESSingleNodeTestCase {

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
        createIndex();
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
        createIndex();
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

    @Test
    public void testRecoveryCancellationNoPredicate() throws Exception {
        createIndex();
        final RecoveriesCollection collection = new RecoveriesCollection(logger, getInstanceFromNode(ThreadPool.class));
        final long recoveryId = startRecovery(collection);
        final long recoveryId2 = startRecovery(collection);
        try (RecoveriesCollection.StatusRef statusRef = collection.getStatus(recoveryId)) {
            ShardId shardId = statusRef.status().shardId();
            assertTrue("failed to cancel recoveries", collection.cancelRecoveriesForShard(shardId, "test"));
            assertThat("all recoveries should be cancelled", collection.size(), equalTo(0));
        } finally {
            collection.cancelRecovery(recoveryId, "meh");
            collection.cancelRecovery(recoveryId2, "meh");
        }
    }

    @Test
    public void testRecoveryCancellationPredicate() throws Exception {
        createIndex();
        final RecoveriesCollection collection = new RecoveriesCollection(logger, getInstanceFromNode(ThreadPool.class));
        final long recoveryId = startRecovery(collection);
        final long recoveryId2 = startRecovery(collection);
        final ArrayList<AutoCloseable> toClose = new ArrayList<>();
        try {
            RecoveriesCollection.StatusRef statusRef = collection.getStatus(recoveryId);
            toClose.add(statusRef);
            ShardId shardId = statusRef.status().shardId();
            assertFalse("should not have cancelled recoveries", collection.cancelRecoveriesForShard(shardId, "test", status -> false));
            final Predicate<RecoveryStatus> shouldCancel = status -> status.recoveryId() == recoveryId;
            assertTrue("failed to cancel recoveries", collection.cancelRecoveriesForShard(shardId, "test", shouldCancel));
            assertThat("we should still have on recovery", collection.size(), equalTo(1));
            statusRef = collection.getStatus(recoveryId);
            toClose.add(statusRef);
            assertNull("recovery should have been deleted", statusRef);
            statusRef = collection.getStatus(recoveryId2);
            toClose.add(statusRef);
            assertNotNull("recovery should NOT have been deleted", statusRef);

        } finally {
            // TODO: do we want a lucene IOUtils version of this?
            for (AutoCloseable closeable : toClose) {
                if (closeable != null) {
                    closeable.close();
                }
            }
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

    long startRecovery(RecoveriesCollection collection, RecoveryTarget.RecoveryListener listener, TimeValue timeValue) {
        IndicesService indexServices = getInstanceFromNode(IndicesService.class);
        IndexShard indexShard = indexServices.indexServiceSafe("test").shard(0);
        final DiscoveryNode sourceNode = new DiscoveryNode("id", DummyTransportAddress.INSTANCE, Version.CURRENT);
        return collection.startRecovery(indexShard, sourceNode, listener, timeValue);
    }

}
