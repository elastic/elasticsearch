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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class PeerRecoveryTargetServiceTests extends IndexShardTestCase {

    public void testGetStartingSeqNo() throws Exception {
        final IndexShard replica = newShard(false);
        try {
            // Empty store
            {
                recoveryEmptyReplica(replica, true);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(0L));
                recoveryTarget.decRef();
            }
            // Last commit is good - use it.
            final long initDocs = scaledRandomIntBetween(1, 10);
            {
                for (int i = 0; i < initDocs; i++) {
                    indexDoc(replica, "_doc", Integer.toString(i));
                    if (randomBoolean()) {
                        flushShard(replica);
                    }
                }
                flushShard(replica);
                replica.updateGlobalCheckpointOnReplica(initDocs - 1, "test");
                replica.sync();
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs));
                recoveryTarget.decRef();
            }
            // Global checkpoint does not advance, last commit is not good - use the previous commit
            final int moreDocs = randomIntBetween(1, 10);
            {
                for (int i = 0; i < moreDocs; i++) {
                    indexDoc(replica, "_doc", Long.toString(i));
                    if (randomBoolean()) {
                        flushShard(replica);
                    }
                }
                flushShard(replica);
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs));
                recoveryTarget.decRef();
            }
            // Advances the global checkpoint, a safe commit also advances
            {
                replica.updateGlobalCheckpointOnReplica(initDocs + moreDocs - 1, "test");
                replica.sync();
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(initDocs + moreDocs));
                recoveryTarget.decRef();
            }
            // Different translogUUID, fallback to file-based
            {
                replica.close("test", false);
                final List<IndexCommit> commits = DirectoryReader.listCommits(replica.store().directory());
                IndexWriterConfig iwc = new IndexWriterConfig(null)
                    .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                    .setCommitOnClose(false)
                    .setMergePolicy(NoMergePolicy.INSTANCE)
                    .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
                try (IndexWriter writer = new IndexWriter(replica.store().directory(), iwc)) {
                    final Map<String, String> userData = new HashMap<>(commits.get(commits.size() - 1).getUserData());
                    userData.put(Translog.TRANSLOG_UUID_KEY, UUIDs.randomBase64UUID());
                    writer.setLiveCommitData(userData.entrySet());
                    writer.commit();
                }
                final RecoveryTarget recoveryTarget = new RecoveryTarget(replica, null, null);
                assertThat(PeerRecoveryTargetService.getStartingSeqNo(logger, recoveryTarget), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
                recoveryTarget.decRef();
            }
        } finally {
            closeShards(replica);
        }
    }

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
        recoveryTarget.receiveFileInfo(
            mdFiles.stream().map(StoreFileMetaData::name).collect(Collectors.toList()),
            mdFiles.stream().map(StoreFileMetaData::length).collect(Collectors.toList()),
            Collections.emptyList(), Collections.emptyList(), 0
        );
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
        recoveryTarget.cleanFiles(0, Long.parseLong(sourceSnapshot.getCommitUserData().get(SequenceNumbers.MAX_SEQ_NO)), sourceSnapshot);
        recoveryTarget.decRef();
        Store.MetadataSnapshot targetSnapshot = targetShard.snapshotStoreMetadata();
        Store.RecoveryDiff diff = sourceSnapshot.recoveryDiff(targetSnapshot);
        assertThat(diff.different, empty());
        closeShards(sourceShard, targetShard);
    }
}
