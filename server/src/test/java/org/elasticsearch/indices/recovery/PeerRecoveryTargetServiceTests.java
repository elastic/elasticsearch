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

package org.elasticsearch.indices.recovery;

import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.elasticsearch.indices.recovery.PeerRecoveryTargetService.getStartRecoveryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class PeerRecoveryTargetServiceTests extends IndexShardTestCase {

    public void testWriteFileChunksConcurrently() throws Exception {
        IndexShard sourceShard = newStartedShard(true);
        int numDocs = between(20, 100);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(sourceShard, "_doc", Integer.toString(i));
        }
        sourceShard.flush(new FlushRequest());
        Store.MetadataSnapshot sourceSnapshot = sourceShard.store().getMetadata(null);
        List<StoreFileMetaData> mdFiles = new ArrayList<>();
        for (StoreFileMetaData md : sourceSnapshot) {
            mdFiles.add(md);
        }
        final IndexShard targetShard = newShard(false);
        final DiscoveryNode pNode = getFakeDiscoNode(sourceShard.routingEntry().currentNodeId());
        final DiscoveryNode rNode = getFakeDiscoNode(targetShard.routingEntry().currentNodeId());
        targetShard.markAsRecovering("test-peer-recovery", new RecoveryState(targetShard.routingEntry(), rNode, pNode));
        final RecoveryTarget recoveryTarget = new RecoveryTarget(targetShard, null, null);
        final PlainActionFuture<Void> receiveFileInfoFuture = new PlainActionFuture<>();
        recoveryTarget.receiveFileInfo(
            mdFiles.stream().map(StoreFileMetaData::name).collect(Collectors.toList()),
            mdFiles.stream().map(StoreFileMetaData::length).collect(Collectors.toList()),
            Collections.emptyList(), Collections.emptyList(), 0, receiveFileInfoFuture
        );
        receiveFileInfoFuture.actionGet();
        List<RecoveryFileChunkRequest> requests = new ArrayList<>();
        for (StoreFileMetaData md : mdFiles) {
            try (IndexInput in = sourceShard.store().directory().openInput(md.name(), IOContext.READONCE)) {
                int pos = 0;
                while (pos < md.length()) {
                    int length = between(1, Math.toIntExact(md.length() - pos));
                    byte[] buffer = new byte[length];
                    in.readBytes(buffer, 0, length);
                    requests.add(new RecoveryFileChunkRequest(0, sourceShard.shardId(), md, pos, new BytesArray(buffer),
                        pos + length == md.length(), 1, 1));
                    pos += length;
                }
            }
        }
        Randomness.shuffle(requests);
        BlockingQueue<RecoveryFileChunkRequest> queue = new ArrayBlockingQueue<>(requests.size());
        queue.addAll(requests);
        Thread[] senders = new Thread[between(1, 4)];
        CyclicBarrier barrier = new CyclicBarrier(senders.length);
        for (int i = 0; i < senders.length; i++) {
            senders[i] = new Thread(() -> {
                try {
                    barrier.await();
                    RecoveryFileChunkRequest r;
                    while ((r = queue.poll()) != null) {
                        recoveryTarget.writeFileChunk(r.metadata(), r.position(), r.content(), r.lastChunk(), r.totalTranslogOps(),
                            ActionListener.wrap(ignored -> {},
                                e -> {
                                    throw new AssertionError(e);
                                }));
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            senders[i].start();
        }
        for (Thread sender : senders) {
            sender.join();
        }
        PlainActionFuture<Void> cleanFilesFuture = new PlainActionFuture<>();
        recoveryTarget.cleanFiles(0, Long.parseLong(sourceSnapshot.getCommitUserData().get(SequenceNumbers.MAX_SEQ_NO)),
            sourceSnapshot, cleanFilesFuture);
        cleanFilesFuture.actionGet();
        recoveryTarget.decRef();
        Store.MetadataSnapshot targetSnapshot = targetShard.snapshotStoreMetadata();
        Store.RecoveryDiff diff = sourceSnapshot.recoveryDiff(targetSnapshot);
        assertThat(diff.different, empty());
        closeShards(sourceShard, targetShard);
    }

    public void testPrepareIndexForPeerRecovery() throws Exception {
        CheckedFunction<IndexShard, Long, Exception> populateData = shard -> {
            List<Long> seqNos = LongStream.range(0, 100).boxed().collect(Collectors.toList());
            Randomness.shuffle(seqNos);
            for (long seqNo : seqNos) {
                shard.applyIndexOperationOnReplica(seqNo, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, new SourceToParse(
                    shard.shardId().getIndexName(), "_doc", UUIDs.randomBase64UUID(), new BytesArray("{}"), XContentType.JSON));
                if (randomInt(100) < 5) {
                    shard.flush(new FlushRequest().waitIfOngoing(true));
                }
            }
            shard.sync();
            long globalCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, shard.getLocalCheckpoint());
            shard.updateGlobalCheckpointOnReplica(globalCheckpoint, "test");
            shard.sync();
            return globalCheckpoint;
        };
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

        CheckedFunction<IndexShard, Long, Exception> getStartingSeqno = shard -> {
            RecoveryTarget recoveryTarget = new RecoveryTarget(shard, null, null);
            try (Closeable ignored = recoveryTarget::decRef) {
                return getStartRecoveryRequest(logger, localNode, recoveryTarget).startingSeqNo();
            }
        };

        // empty copy
        IndexShard shard = newShard(false);
        shard.markAsRecovering("for testing", new RecoveryState(shard.routingEntry(), localNode, localNode));
        shard.prepareForIndexRecovery();
        expectThrows(IndexNotFoundException.class, shard::snapshotStoreMetadata);
        assertThat(getStartingSeqno.apply(shard), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
        closeShards(shard);

        // good copy
        shard = newStartedShard(false);
        long globalCheckpoint = populateData.apply(shard);
        IndexShard replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(getStartingSeqno.apply(replica), equalTo(globalCheckpoint + 1));
        closeShards(replica);

        // corrupted copy
        shard = newStartedShard(false);
        if (randomBoolean()) {
            populateData.apply(shard);
        }
        shard.store().markStoreCorrupted(new IOException("test"));
        replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(getStartingSeqno.apply(replica), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
        closeShards(replica);

        // copy with truncated translog
        shard = newStartedShard(false);
        globalCheckpoint = populateData.apply(shard);
        replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        String translogUUID = Translog.createEmptyTranslog(replica.shardPath().resolveTranslog(), globalCheckpoint,
            replica.shardId(), replica.getPendingPrimaryTerm());
        replica.store().associateIndexWithNewTranslog(translogUUID);
        long startingSeqNo = shard.store().findSafeIndexCommit(shard.shardPath().resolveTranslog())
            .map(commitInfo -> commitInfo.localCheckpoint + 1).orElse(SequenceNumbers.UNASSIGNED_SEQ_NO);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(getStartingSeqno.apply(replica), equalTo(startingSeqNo));
    }
}
