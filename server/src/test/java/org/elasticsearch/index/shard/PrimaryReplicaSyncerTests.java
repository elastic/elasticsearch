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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.resync.ResyncReplicationRequest;
import org.elasticsearch.action.resync.ResyncReplicationResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class PrimaryReplicaSyncerTests extends IndexShardTestCase {

    public void testSyncerSendsOffCorrectDocuments() throws Exception {
        IndexShard shard = newStartedShard(true);
        TaskManager taskManager = new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet());
        AtomicBoolean syncActionCalled = new AtomicBoolean();
        List<ResyncReplicationRequest> resyncRequests = new ArrayList<>();
        PrimaryReplicaSyncer.SyncAction syncAction =
            (request, parentTask, allocationId, primaryTerm, listener) -> {
                logger.info("Sending off {} operations", request.getOperations().length);
                syncActionCalled.set(true);
                resyncRequests.add(request);
                assertThat(parentTask, instanceOf(PrimaryReplicaSyncer.ResyncTask.class));
                listener.onResponse(new ResyncReplicationResponse());
            };
        PrimaryReplicaSyncer syncer = new PrimaryReplicaSyncer(taskManager, syncAction);
        syncer.setChunkSize(new ByteSizeValue(randomIntBetween(1, 10)));

        int numDocs = randomInt(10);
        for (int i = 0; i < numDocs; i++) {
            // Index doc but not advance local checkpoint.
            shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
                new SourceToParse(shard.shardId().getIndexName(), Integer.toString(i), new BytesArray("{}"), XContentType.JSON),
                SequenceNumbers.UNASSIGNED_SEQ_NO, 0,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, true);
        }

        long globalCheckPoint = numDocs > 0 ? randomIntBetween(0, numDocs - 1) : 0;
        boolean syncNeeded = numDocs > 0;

        String allocationId = shard.routingEntry().allocationId().getId();
        shard.updateShardState(shard.routingEntry(), shard.getPendingPrimaryTerm(), null, 1000L, Collections.singleton(allocationId),
            new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard.routingEntry()).build());
        shard.updateLocalCheckpointForShard(allocationId, globalCheckPoint);
        assertEquals(globalCheckPoint, shard.getLastKnownGlobalCheckpoint());

        logger.info("Total ops: {}, global checkpoint: {}", numDocs, globalCheckPoint);

        PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<>();
        syncer.resync(shard, fut);
        PrimaryReplicaSyncer.ResyncTask resyncTask = fut.get();

        if (syncNeeded) {
            assertTrue("Sync action was not called", syncActionCalled.get());
            ResyncReplicationRequest resyncRequest = resyncRequests.remove(0);
            assertThat(resyncRequest.getTrimAboveSeqNo(), equalTo(numDocs - 1L));

            assertThat("trimAboveSeqNo has to be specified in request #0 only", resyncRequests.stream()
                    .mapToLong(ResyncReplicationRequest::getTrimAboveSeqNo)
                    .filter(seqNo -> seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO)
                    .findFirst()
                    .isPresent(),
                is(false));

            assertThat(resyncRequest.getMaxSeenAutoIdTimestampOnPrimary(), equalTo(shard.getMaxSeenAutoIdTimestamp()));
        }
        if (syncNeeded && globalCheckPoint < numDocs - 1) {
            if (shard.indexSettings.isSoftDeleteEnabled()) {
                assertThat(resyncTask.getSkippedOperations(), equalTo(0));
                assertThat(resyncTask.getResyncedOperations(), equalTo(resyncTask.getTotalOperations()));
                assertThat(resyncTask.getTotalOperations(), equalTo(Math.toIntExact(numDocs - 1 - globalCheckPoint)));
            } else {
                int skippedOps = Math.toIntExact(globalCheckPoint + 1); // everything up to global checkpoint included
                assertThat(resyncTask.getSkippedOperations(), equalTo(skippedOps));
                assertThat(resyncTask.getResyncedOperations(), equalTo(numDocs - skippedOps));
                assertThat(resyncTask.getTotalOperations(), equalTo(globalCheckPoint == numDocs - 1 ? 0 : numDocs));
            }
        } else {
            assertThat(resyncTask.getSkippedOperations(), equalTo(0));
            assertThat(resyncTask.getResyncedOperations(), equalTo(0));
            assertThat(resyncTask.getTotalOperations(), equalTo(0));
        }
        closeShards(shard);
    }

    public void testSyncerOnClosingShard() throws Exception {
        IndexShard shard = newStartedShard(true);
        AtomicBoolean syncActionCalled = new AtomicBoolean();
        PrimaryReplicaSyncer.SyncAction syncAction =
            (request, parentTask, allocationId, primaryTerm, listener) -> {
                logger.info("Sending off {} operations", request.getOperations().length);
                syncActionCalled.set(true);
                threadPool.generic().execute(() -> listener.onResponse(new ResyncReplicationResponse()));
            };
        PrimaryReplicaSyncer syncer = new PrimaryReplicaSyncer(
            new TaskManager(Settings.EMPTY, threadPool, Collections.emptySet()), syncAction);
        syncer.setChunkSize(new ByteSizeValue(1)); // every document is sent off separately

        int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            // Index doc but not advance local checkpoint.
            shard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
                new SourceToParse(shard.shardId().getIndexName(), Integer.toString(i), new BytesArray("{}"), XContentType.JSON),
                SequenceNumbers.UNASSIGNED_SEQ_NO, 0, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false);
        }

        String allocationId = shard.routingEntry().allocationId().getId();
        shard.updateShardState(shard.routingEntry(), shard.getPendingPrimaryTerm(), null, 1000L, Collections.singleton(allocationId),
            new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard.routingEntry()).build());

        CountDownLatch syncCalledLatch = new CountDownLatch(1);
        PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<PrimaryReplicaSyncer.ResyncTask>() {
            @Override
            public void onFailure(Exception e) {
                try {
                    super.onFailure(e);
                } finally {
                    syncCalledLatch.countDown();
                }
            }
            @Override
            public void onResponse(PrimaryReplicaSyncer.ResyncTask result) {
                try {
                    super.onResponse(result);
                } finally {
                    syncCalledLatch.countDown();
                }
            }
        };
        threadPool.generic().execute(() -> {
            syncer.resync(shard, fut);
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

    public void testStatusSerializationAsNamedWriteable() throws IOException {
        PrimaryReplicaSyncer.ResyncTask.Status status = new PrimaryReplicaSyncer.ResyncTask.Status(randomAlphaOfLength(10),
            randomIntBetween(0, 1000), randomIntBetween(0, 1000), randomIntBetween(0, 1000));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(status);
            try (StreamInput in = new NamedWriteableAwareStreamInput(
                new ByteBufferStreamInput(ByteBuffer.wrap(out.bytes().toBytesRef().bytes)),
                new NamedWriteableRegistry(NetworkModule.getNamedWriteables()))) {
                assertThat(in.readNamedWriteable(Task.Status.class), equalTo(status));
            }
        }
    }

    public void testStatusEquals() throws IOException {
        PrimaryReplicaSyncer.ResyncTask task =
            new PrimaryReplicaSyncer.ResyncTask(0, "type", "action", "desc", null, Collections.emptyMap());
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
            case 0:
                task.setPhase("otherPhase");
                break;
            case 1:
                task.setResyncedOperations(task.getResyncedOperations() + 1);
                break;
            case 2:
                task.setSkippedOperations(task.getSkippedOperations() + 1);
                break;
            case 3:
                task.setTotalOperations(task.getTotalOperations() + 1);
                break;
        }

        PrimaryReplicaSyncer.ResyncTask.Status differentStatus = task.getStatus();
        assertNotEquals(status, differentStatus);
    }

    public void testStatusReportsCorrectNumbers() throws IOException {
        PrimaryReplicaSyncer.ResyncTask task =
            new PrimaryReplicaSyncer.ResyncTask(0, "type", "action", "desc", null, Collections.emptyMap());
        task.setPhase(randomAlphaOfLength(10));
        task.setResyncedOperations(randomIntBetween(0, 1000));
        task.setTotalOperations(randomIntBetween(0, 1000));
        task.setSkippedOperations(randomIntBetween(0, 1000));
        PrimaryReplicaSyncer.ResyncTask.Status status = task.getStatus();
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        status.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        String jsonString = Strings.toString(jsonBuilder);
        assertThat(jsonString, containsString("\"phase\":\"" + task.getPhase() + "\""));
        assertThat(jsonString, containsString("\"totalOperations\":" + task.getTotalOperations()));
        assertThat(jsonString, containsString("\"resyncedOperations\":" + task.getResyncedOperations()));
        assertThat(jsonString, containsString("\"skippedOperations\":" + task.getSkippedOperations()));
    }
}
