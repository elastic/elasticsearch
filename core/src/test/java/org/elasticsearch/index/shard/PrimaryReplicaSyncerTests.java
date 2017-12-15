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
package org.elasticsearch.index.shard;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.resync.ResyncReplicationResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class PrimaryReplicaSyncerTests extends IndexShardTestCase {

    public void testSyncerSendsOffCorrectDocuments() throws Exception {
        IndexShard shard = newStartedShard(true);
        TaskManager taskManager = new TaskManager(Settings.EMPTY);
        AtomicBoolean syncActionCalled = new AtomicBoolean();
        PrimaryReplicaSyncer.SyncAction syncAction =
            (request, parentTask, allocationId, primaryTerm, listener) -> {
                logger.info("Sending off {} operations", request.getOperations().length);
                syncActionCalled.set(true);
                assertThat(parentTask, instanceOf(PrimaryReplicaSyncer.ResyncTask.class));
                listener.onResponse(new ResyncReplicationResponse());
            };
        PrimaryReplicaSyncer syncer = new PrimaryReplicaSyncer(Settings.EMPTY, taskManager, syncAction);
        syncer.setChunkSize(new ByteSizeValue(randomIntBetween(1, 100)));

        int numDocs = randomInt(10);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "test", Integer.toString(i));
        }

        long globalCheckPoint = numDocs > 0 ? randomIntBetween(0, numDocs - 1) : 0;
        boolean syncNeeded = numDocs > 0 && globalCheckPoint < numDocs - 1;

        String allocationId = shard.routingEntry().allocationId().getId();
        shard.updateShardState(shard.routingEntry(), shard.getPrimaryTerm(), null, 1000L, Collections.singleton(allocationId),
            new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard.routingEntry()).build(), Collections.emptySet());
        shard.updateLocalCheckpointForShard(allocationId, globalCheckPoint);
        assertEquals(globalCheckPoint, shard.getGlobalCheckpoint());

        logger.info("Total ops: {}, global checkpoint: {}", numDocs, globalCheckPoint);

        PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<>();
        syncer.resync(shard, fut);
        fut.get();

        if (syncNeeded) {
            assertTrue("Sync action was not called", syncActionCalled.get());
        }
        assertEquals(globalCheckPoint == numDocs - 1 ? 0 : numDocs, fut.get().getTotalOperations());
        if (syncNeeded) {
            long skippedOps = globalCheckPoint + 1; // everything up to global checkpoint included
            assertEquals(skippedOps, fut.get().getSkippedOperations());
            assertEquals(numDocs - skippedOps, fut.get().getResyncedOperations());
        } else {
            assertEquals(0, fut.get().getSkippedOperations());
            assertEquals(0, fut.get().getResyncedOperations());
        }

        closeShards(shard);
    }

    public void testSyncerOnClosingShard() throws Exception {
        IndexShard shard = newStartedShard(true);
        AtomicBoolean syncActionCalled = new AtomicBoolean();
        CountDownLatch syncCalledLatch = new CountDownLatch(1);
        PrimaryReplicaSyncer.SyncAction syncAction =
            (request, parentTask, allocationId, primaryTerm, listener) -> {
                logger.info("Sending off {} operations", request.getOperations().length);
                syncActionCalled.set(true);
                syncCalledLatch.countDown();
                threadPool.generic().execute(() -> listener.onResponse(new ResyncReplicationResponse()));
            };
        PrimaryReplicaSyncer syncer = new PrimaryReplicaSyncer(Settings.EMPTY, new TaskManager(Settings.EMPTY), syncAction);
        syncer.setChunkSize(new ByteSizeValue(1)); // every document is sent off separately

        int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            indexDoc(shard, "test", Integer.toString(i));
        }

        String allocationId = shard.routingEntry().allocationId().getId();
        shard.updateShardState(shard.routingEntry(), shard.getPrimaryTerm(), null, 1000L, Collections.singleton(allocationId),
            new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard.routingEntry()).build(), Collections.emptySet());

        PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<>();
        threadPool.generic().execute(() -> {
            try {
                syncer.resync(shard, fut);
            } catch (AlreadyClosedException ace) {
                fut.onFailure(ace);
            }
        });
        if (randomBoolean()) {
            syncCalledLatch.await();
        }
        closeShards(shard);
        try {
            fut.actionGet();
            assertTrue("Sync action was not called", syncActionCalled.get());
        } catch (AlreadyClosedException | IndexShardClosedException ignored) {
            // ignore
        }
    }

    public void testStatusSerialization() throws IOException {
        PrimaryReplicaSyncer.ResyncTask.Status status = new PrimaryReplicaSyncer.ResyncTask.Status(randomAlphaOfLength(10),
            randomIntBetween(0, 1000), randomIntBetween(0, 1000), randomIntBetween(0, 1000));
        final BytesStreamOutput out = new BytesStreamOutput();
        status.writeTo(out);
        final ByteBufferStreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap(out.bytes().toBytesRef().bytes));
        PrimaryReplicaSyncer.ResyncTask.Status serializedStatus = new PrimaryReplicaSyncer.ResyncTask.Status(in);
        assertEquals(status, serializedStatus);
    }

    public void testStatusEquals() throws IOException {
        PrimaryReplicaSyncer.ResyncTask task = new PrimaryReplicaSyncer.ResyncTask(0, "type", "action", "desc", null);
        task.setPhase(randomAlphaOfLength(10));
        task.setResyncedOperations(randomIntBetween(0, 1000));
        task.setTotalOperations(randomIntBetween(0, 1000));
        task.setSkippedOperations(randomIntBetween(0, 1000));
        PrimaryReplicaSyncer.ResyncTask.Status status = task.getStatus();
        PrimaryReplicaSyncer.ResyncTask.Status sameStatus = task.getStatus();
        assertNotSame(status, sameStatus);
        assertEquals(status, sameStatus);
        assertEquals(status.hashCode(), sameStatus.hashCode());

        switch (randomInt(3)) {
            case 0: task.setPhase("otherPhase"); break;
            case 1: task.setResyncedOperations(task.getResyncedOperations() + 1); break;
            case 2: task.setSkippedOperations(task.getSkippedOperations() + 1); break;
            case 3: task.setTotalOperations(task.getTotalOperations() + 1); break;
        }

        PrimaryReplicaSyncer.ResyncTask.Status differentStatus = task.getStatus();
        assertNotEquals(status, differentStatus);
    }

    public void testStatusReportsCorrectNumbers() throws IOException {
        PrimaryReplicaSyncer.ResyncTask task = new PrimaryReplicaSyncer.ResyncTask(0, "type", "action", "desc", null);
        task.setPhase(randomAlphaOfLength(10));
        task.setResyncedOperations(randomIntBetween(0, 1000));
        task.setTotalOperations(randomIntBetween(0, 1000));
        task.setSkippedOperations(randomIntBetween(0, 1000));
        PrimaryReplicaSyncer.ResyncTask.Status status = task.getStatus();
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        status.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        String jsonString = jsonBuilder.string();
        assertThat(jsonString, containsString("\"phase\":\"" + task.getPhase() + "\""));
        assertThat(jsonString, containsString("\"totalOperations\":" + task.getTotalOperations()));
        assertThat(jsonString, containsString("\"resyncedOperations\":" + task.getResyncedOperations()));
        assertThat(jsonString, containsString("\"skippedOperations\":" + task.getSkippedOperations()));
    }
}
