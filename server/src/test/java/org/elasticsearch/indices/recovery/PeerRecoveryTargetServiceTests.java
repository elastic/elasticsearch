/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.repositories.IndexId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class PeerRecoveryTargetServiceTests extends IndexShardTestCase {
    private static final ByteSizeValue SNAPSHOT_FILE_PART_SIZE = new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES);

    public void testWriteFileChunksConcurrently() throws Exception {
        IndexShard sourceShard = newStartedShard(true);
        int numDocs = between(20, 100);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(sourceShard, "_doc", Integer.toString(i));
        }
        sourceShard.flush(new FlushRequest());
        Store.MetadataSnapshot sourceSnapshot = sourceShard.store().getMetadata(null);
        List<StoreFileMetadata> mdFiles = new ArrayList<>();
        for (StoreFileMetadata md : sourceSnapshot) {
            mdFiles.add(md);
        }
        final IndexShard targetShard = newShard(false);
        final DiscoveryNode pNode = getFakeDiscoNode(sourceShard.routingEntry().currentNodeId());
        final DiscoveryNode rNode = getFakeDiscoNode(targetShard.routingEntry().currentNodeId());
        targetShard.markAsRecovering("test-peer-recovery", new RecoveryState(targetShard.routingEntry(), rNode, pNode));
        final RecoveryTarget recoveryTarget = new RecoveryTarget(targetShard, null, null, null);
        final PlainActionFuture<Void> receiveFileInfoFuture = new PlainActionFuture<>();
        recoveryTarget.receiveFileInfo(
            mdFiles.stream().map(StoreFileMetadata::name).collect(Collectors.toList()),
            mdFiles.stream().map(StoreFileMetadata::length).collect(Collectors.toList()),
            Collections.emptyList(), Collections.emptyList(), 0, receiveFileInfoFuture
        );
        receiveFileInfoFuture.actionGet();
        List<RecoveryFileChunkRequest> requests = new ArrayList<>();
        long seqNo = 0;
        for (StoreFileMetadata md : mdFiles) {
            try (IndexInput in = sourceShard.store().directory().openInput(md.name(), IOContext.READONCE)) {
                int pos = 0;
                while (pos < md.length()) {
                    int length = between(1, Math.toIntExact(md.length() - pos));
                    byte[] buffer = new byte[length];
                    in.readBytes(buffer, 0, length);
                    requests.add(new RecoveryFileChunkRequest(0, seqNo++, sourceShard.shardId(), md, pos,
                        ReleasableBytesReference.wrap(new BytesArray(buffer)),
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

    private SeqNoStats populateRandomData(IndexShard shard) throws IOException {
        List<Long> seqNos = LongStream.range(0, 100).boxed().collect(Collectors.toList());
        Randomness.shuffle(seqNos);
        for (long seqNo : seqNos) {
            shard.applyIndexOperationOnReplica(seqNo, 1, shard.getOperationPrimaryTerm(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false, new SourceToParse(shard.shardId().getIndexName(), UUIDs.randomBase64UUID(),
                    new BytesArray("{}"), XContentType.JSON));
            if (randomInt(100) < 5) {
                shard.flush(new FlushRequest().waitIfOngoing(true));
            }
        }
        shard.sync();
        long globalCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, shard.getLocalCheckpoint());
        shard.updateGlobalCheckpointOnReplica(globalCheckpoint, "test");
        shard.sync();
        return shard.seqNoStats();
    }

    public void testPrepareIndexForPeerRecovery() throws Exception {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

        // empty copy
        IndexShard shard = newShard(false);
        shard.markAsRecovering("for testing", new RecoveryState(shard.routingEntry(), localNode, localNode));
        shard.prepareForIndexRecovery();
        assertThat(shard.recoverLocallyUpToGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
        assertThat(shard.recoveryState().getTranslog().totalLocal(), equalTo(RecoveryState.Translog.UNKNOWN));
        assertThat(shard.recoveryState().getTranslog().recoveredOperations(), equalTo(0));
        assertThat(shard.getLastKnownGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
        closeShards(shard);

        // good copy
        shard = newStartedShard(false);
        long globalCheckpoint = populateRandomData(shard).getGlobalCheckpoint();
        Optional<SequenceNumbers.CommitInfo> safeCommit = shard.store().findSafeIndexCommit(globalCheckpoint);
        assertTrue(safeCommit.isPresent());
        int expectedTotalLocal = 0;
        if (safeCommit.get().localCheckpoint < globalCheckpoint) {
            try (Translog.Snapshot snapshot = getTranslog(shard).newSnapshot(safeCommit.get().localCheckpoint + 1, globalCheckpoint)) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    if (op.seqNo() <= globalCheckpoint) {
                        expectedTotalLocal++;
                    }
                }
            }
        }
        IndexShard replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(replica.recoverLocallyUpToGlobalCheckpoint(), equalTo(globalCheckpoint + 1));
        assertThat(replica.recoveryState().getTranslog().totalLocal(), equalTo(expectedTotalLocal));
        assertThat(replica.recoveryState().getTranslog().recoveredOperations(), equalTo(expectedTotalLocal));
        assertThat(replica.getLastKnownGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
        closeShards(replica);

        // corrupted copy
        shard = newStartedShard(false);
        if (randomBoolean()) {
            populateRandomData(shard);
        }
        shard.store().markStoreCorrupted(new IOException("test"));
        replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(replica.recoverLocallyUpToGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
        assertThat(replica.recoveryState().getTranslog().totalLocal(), equalTo(RecoveryState.Translog.UNKNOWN));
        assertThat(replica.recoveryState().getTranslog().recoveredOperations(), equalTo(0));
        assertThat(replica.getLastKnownGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
        closeShards(replica);

        // copy with truncated translog
        shard = newStartedShard(false);
        SeqNoStats seqNoStats = populateRandomData(shard);
        replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        globalCheckpoint =  randomFrom(UNASSIGNED_SEQ_NO, seqNoStats.getMaxSeqNo());
        String translogUUID = Translog.createEmptyTranslog(replica.shardPath().resolveTranslog(), globalCheckpoint,
            replica.shardId(), replica.getPendingPrimaryTerm());
        replica.store().associateIndexWithNewTranslog(translogUUID);
        safeCommit = replica.store().findSafeIndexCommit(globalCheckpoint);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        if (safeCommit.isPresent()) {
            assertThat(replica.recoverLocallyUpToGlobalCheckpoint(), equalTo(safeCommit.get().localCheckpoint + 1));
            assertThat(replica.recoveryState().getTranslog().totalLocal(), equalTo(0));
        } else {
            assertThat(replica.recoverLocallyUpToGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
            assertThat(replica.recoveryState().getTranslog().totalLocal(), equalTo(RecoveryState.Translog.UNKNOWN));
        }
        assertThat(replica.recoveryState().getStage(), equalTo(RecoveryState.Stage.TRANSLOG));
        assertThat(replica.recoveryState().getTranslog().recoveredOperations(), equalTo(0));
        assertThat(replica.getLastKnownGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
        closeShards(replica);
    }

    public void testClosedIndexSkipsLocalRecovery() throws Exception {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        IndexShard shard = newStartedShard(false);
        long globalCheckpoint = populateRandomData(shard).getGlobalCheckpoint();
        Optional<SequenceNumbers.CommitInfo> safeCommit = shard.store().findSafeIndexCommit(globalCheckpoint);
        assertTrue(safeCommit.isPresent());
        final IndexMetadata indexMetadata;
        if (randomBoolean()) {
            indexMetadata = IndexMetadata.builder(shard.indexSettings().getIndexMetadata())
                .settings(shard.indexSettings().getSettings())
                .state(IndexMetadata.State.CLOSE).build();
        } else {
            indexMetadata = IndexMetadata.builder(shard.indexSettings().getIndexMetadata())
                .settings(Settings.builder().put(shard.indexSettings().getSettings())
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)).build();
        }
        IndexShard replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE), indexMetadata, NoOpEngine::new);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(replica.recoverLocallyUpToGlobalCheckpoint(), equalTo(safeCommit.get().localCheckpoint + 1));
        assertThat(replica.recoveryState().getTranslog().totalLocal(), equalTo(0));
        assertThat(replica.recoveryState().getTranslog().recoveredOperations(), equalTo(0));
        assertThat(replica.getLastKnownGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
        closeShards(replica);
    }

    public void testResetStartingSeqNoIfLastCommitCorrupted() throws Exception {
        IndexShard shard = newStartedShard(false);
        populateRandomData(shard);
        DiscoveryNode pNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        DiscoveryNode rNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        shard = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.PeerRecoverySource.INSTANCE));
        shard.markAsRecovering("peer recovery", new RecoveryState(shard.routingEntry(), pNode, rNode));
        shard.prepareForIndexRecovery();
        long startingSeqNo = shard.recoverLocallyUpToGlobalCheckpoint();
        shard.store().markStoreCorrupted(new IOException("simulated"));
        RecoveryTarget recoveryTarget = new RecoveryTarget(shard, null, null, null);
        StartRecoveryRequest request = PeerRecoveryTargetService.getStartRecoveryRequest(logger, rNode, recoveryTarget, startingSeqNo);
        assertThat(request.startingSeqNo(), equalTo(UNASSIGNED_SEQ_NO));
        assertThat(request.metadataSnapshot().size(), equalTo(0));
        recoveryTarget.decRef();
        closeShards(shard);
    }

    public void testResetStartRequestIfTranslogIsCorrupted() throws Exception {
        DiscoveryNode pNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        DiscoveryNode rNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        IndexShard shard = newStartedShard(false);
        final SeqNoStats seqNoStats = populateRandomData(shard);
        shard.close("test", false);
        if (randomBoolean()) {
            shard.store().associateIndexWithNewTranslog(UUIDs.randomBase64UUID());
        } else if (randomBoolean()) {
            Translog.createEmptyTranslog(
                shard.shardPath().resolveTranslog(), seqNoStats.getGlobalCheckpoint(), shard.shardId(), shard.getOperationPrimaryTerm());
        } else {
            IOUtils.rm(shard.shardPath().resolveTranslog());
        }
        shard = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.PeerRecoverySource.INSTANCE));
        shard.markAsRecovering("peer recovery", new RecoveryState(shard.routingEntry(), pNode, rNode));
        shard.prepareForIndexRecovery();
        RecoveryTarget recoveryTarget = new RecoveryTarget(shard, null, null, null);
        StartRecoveryRequest request = PeerRecoveryTargetService.getStartRecoveryRequest(
            logger, rNode, recoveryTarget, randomNonNegativeLong());
        assertThat(request.startingSeqNo(), equalTo(UNASSIGNED_SEQ_NO));
        assertThat(request.metadataSnapshot(), sameInstance(Store.MetadataSnapshot.EMPTY));
        recoveryTarget.decRef();
        closeShards(shard);
    }

    public void testSnapshotFileWrite() throws Exception {
        DiscoveryNode pNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        DiscoveryNode rNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

        IndexShard shard = newShard(false);
        shard = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.PeerRecoverySource.INSTANCE));
        shard.markAsRecovering("peer recovery", new RecoveryState(shard.routingEntry(), pNode, rNode));
        shard.prepareForIndexRecovery();

        RecoveryState.Index recoveryStateIndex = shard.recoveryState().getIndex();

        Directory directory = shard.store().directory();

        String fileName = randomAlphaOfLength(10);
        Tuple<StoreFileMetadata, byte[]> storeFileMetadataAndData = createStoreFileMetadataWithRandomContent(fileName);
        StoreFileMetadata storeFileMetadata = storeFileMetadataAndData.v1();
        byte[] fileData = storeFileMetadataAndData.v2();

        String repositoryName = "repo";
        IndexId indexId = new IndexId("index", "uuid");
        ShardId shardId = shard.shardId();
        BlobStoreIndexShardSnapshot.FileInfo fileInfo =
            new BlobStoreIndexShardSnapshot.FileInfo("name", storeFileMetadata, SNAPSHOT_FILE_PART_SIZE);

        SnapshotFilesProvider snapshotFilesProvider = new SnapshotFilesProvider(null) {
            @Override
            public InputStream getInputStreamForSnapshotFile(String requestedRepositoryName,
                                                             IndexId requestedIndexId,
                                                             ShardId requestedShardId,
                                                             BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo,
                                                             Consumer<Long> rateLimiterListener) {
                assertThat(requestedRepositoryName, equalTo(repositoryName));
                assertThat(requestedIndexId, equalTo(indexId));
                assertThat(requestedShardId, equalTo(shardId));

                assertThat(snapshotFileInfo.name(), equalTo(fileInfo.name()));
                assertThat(snapshotFileInfo.metadata().isSame(storeFileMetadata), equalTo(true));

                return new ByteArrayInputStream(fileData);
            }
        };

        recoveryStateIndex.addFileDetail(storeFileMetadata.name(), storeFileMetadata.length(), false);
        recoveryStateIndex.setFileDetailsComplete();

        RecoveryTarget recoveryTarget = new RecoveryTarget(shard, null, snapshotFilesProvider, null);

        PlainActionFuture<Void> writeSnapshotFileFuture = PlainActionFuture.newFuture();
        recoveryTarget.downloadSnapshotFile(repositoryName, indexId, fileInfo, writeSnapshotFileFuture);
        writeSnapshotFileFuture.get();

        Optional<String> tmpFileName = Arrays.stream(directory.listAll())
            .filter(directoryFile -> directoryFile.endsWith(fileName))
            .findFirst();

        assertThat(tmpFileName.isPresent(), is(equalTo(true)));
        try (IndexInput indexInput = directory.openInput(tmpFileName.get(), IOContext.READONCE)) {
            byte[] writtenData = new byte[(int) storeFileMetadata.length()];
            indexInput.readBytes(writtenData, 0, (int) storeFileMetadata.length());
            assertThat(writtenData, is(equalTo(fileData)));
        }

        RecoveryState.FileDetail fileDetails = recoveryStateIndex.getFileDetails(storeFileMetadata.name());
        assertThat(fileDetails.recovered(), equalTo(storeFileMetadata.length()));

        recoveryTarget.decRef();
        closeShards(shard);
    }

    public void testSnapshotFileIsDeletedAfterFailure() throws Exception {
        DiscoveryNode pNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        DiscoveryNode rNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

        IndexShard shard = newShard(false);
        shard = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.PeerRecoverySource.INSTANCE));
        shard.markAsRecovering("peer recovery", new RecoveryState(shard.routingEntry(), pNode, rNode));
        shard.prepareForIndexRecovery();

        RecoveryState.Index recoveryStateIndex = shard.recoveryState().getIndex();

        Directory directory = shard.store().directory();
        String[] filesBeforeRestoringSnapshotFile = directory.listAll();

        String fileName = randomAlphaOfLength(10);
        Tuple<StoreFileMetadata, byte[]> storeFileMetadataAndData = createStoreFileMetadataWithRandomContent(fileName);
        StoreFileMetadata storeFileMetadata = storeFileMetadataAndData.v1();
        byte[] fileData = storeFileMetadataAndData.v2();

        SnapshotFilesProvider snapshotFilesProvider = new SnapshotFilesProvider(null) {
            @Override
            public InputStream getInputStreamForSnapshotFile(String requestedRepositoryName,
                                                             IndexId requestedIndexId,
                                                             ShardId requestedShardId,
                                                             BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo,
                                                             Consumer<Long> rateLimiterListener) {
                if (randomBoolean()) {
                    throw new RuntimeException("Unexpected error");
                }

                if (randomBoolean()) {
                    byte[] fileDataCopy = new byte[fileData.length];
                    System.arraycopy(fileData, 0, fileDataCopy, 0, fileData.length);
                    // Corrupt the file
                    for (int i = 0; i < randomIntBetween(1, fileDataCopy.length); i++) {
                        fileDataCopy[i] = randomByte();
                    }
                    return new ByteArrayInputStream(fileDataCopy);
                } else {
                    // Return random data
                    return new ByteArrayInputStream(randomByteArrayOfLength(randomIntBetween(1, fileData.length)));
                }
            }
        };

        recoveryStateIndex.addFileDetail(storeFileMetadata.name(), storeFileMetadata.length(), false);
        recoveryStateIndex.setFileDetailsComplete();

        RecoveryTarget recoveryTarget = new RecoveryTarget(shard, null, snapshotFilesProvider, null);

        String repositoryName = "repo";
        IndexId indexId = new IndexId("index", "uuid");
        BlobStoreIndexShardSnapshot.FileInfo fileInfo =
            new BlobStoreIndexShardSnapshot.FileInfo("name", storeFileMetadata, SNAPSHOT_FILE_PART_SIZE);

        PlainActionFuture<Void> writeSnapshotFileFuture = PlainActionFuture.newFuture();
        recoveryTarget.downloadSnapshotFile(repositoryName, indexId, fileInfo, writeSnapshotFileFuture);
        expectThrows(Exception.class, writeSnapshotFileFuture::actionGet);

        assertThat(filesBeforeRestoringSnapshotFile, equalTo(directory.listAll()));

        RecoveryState.FileDetail fileDetails = recoveryStateIndex.getFileDetails(storeFileMetadata.name());
        assertThat(fileDetails.recovered(), equalTo(0L));

        // Subsequent writes on the same file can proceed without issues
        PlainActionFuture<Void> writeChunkFuture = PlainActionFuture.newFuture();
        ReleasableBytesReference bytesRef = ReleasableBytesReference.wrap(new BytesArray(fileData));
        recoveryTarget.writeFileChunk(storeFileMetadata, 0, bytesRef, true, 0, writeChunkFuture);
        writeChunkFuture.get();

        recoveryTarget.decRef();
        closeShards(shard);
    }

    public void testSnapshotFileAreDeletedAfterCancel() throws Exception {
        DiscoveryNode pNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        DiscoveryNode rNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

        IndexShard shard = newShard(false);
        shard = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.PeerRecoverySource.INSTANCE));
        shard.markAsRecovering("peer recovery", new RecoveryState(shard.routingEntry(), pNode, rNode));
        shard.prepareForIndexRecovery();

        RecoveryState.Index recoveryStateIndex = shard.recoveryState().getIndex();

        Directory directory = shard.store().directory();
        String[] filesBeforeRestoringSnapshotFile = directory.listAll();

        String fileName = randomAlphaOfLength(10);
        Tuple<StoreFileMetadata, byte[]> storeFileMetadataAndData = createStoreFileMetadataWithRandomContent(fileName);
        StoreFileMetadata storeFileMetadata = storeFileMetadataAndData.v1();
        byte[] fileData = storeFileMetadataAndData.v2();

        SnapshotFilesProvider snapshotFilesProvider = new SnapshotFilesProvider(null) {
            @Override
            public InputStream getInputStreamForSnapshotFile(String requestedRepositoryName,
                                                             IndexId requestedIndexId,
                                                             ShardId requestedShardId,
                                                             BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo,
                                                             Consumer<Long> rateLimiterListener) {
                return new ByteArrayInputStream(fileData);
            }
        };

        recoveryStateIndex.addFileDetail(storeFileMetadata.name(), storeFileMetadata.length(), false);
        recoveryStateIndex.setFileDetailsComplete();

        RecoveryTarget recoveryTarget = new RecoveryTarget(shard, null, snapshotFilesProvider, null);

        String repository = "repo";
        IndexId indexId = new IndexId("index", "uuid");
        BlobStoreIndexShardSnapshot.FileInfo fileInfo =
            new BlobStoreIndexShardSnapshot.FileInfo("name", storeFileMetadata, new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES));

        recoveryTarget.incRef();

        PlainActionFuture<Void> writeSnapshotFileFuture = PlainActionFuture.newFuture();
        recoveryTarget.downloadSnapshotFile(repository, indexId, fileInfo, writeSnapshotFileFuture);
        writeSnapshotFileFuture.get();

        RecoveryState.FileDetail fileDetails = recoveryStateIndex.getFileDetails(storeFileMetadata.name());
        assertThat(fileDetails.recovered(), equalTo(storeFileMetadata.length()));

        final String[] filesBeforeCancellingRecovery = directory.listAll();

        recoveryTarget.cancel("This is a test");

        final String[] filesAfterCancellingRecoveryWithOneOutstandingReference = directory.listAll();

        // Since there's still one outstanding reference the snapshot file is kept around
        assertThat(filesBeforeCancellingRecovery, equalTo(filesAfterCancellingRecoveryWithOneOutstandingReference));

        recoveryTarget.decRef();

        // Once the reference is released, the tmp file should be deleted
        assertThat(filesBeforeRestoringSnapshotFile, equalTo(directory.listAll()));

        closeShards(shard);
    }

    private Tuple<StoreFileMetadata, byte[]> createStoreFileMetadataWithRandomContent(String fileName) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("test", "file", out, 1024)) {
            byte[] buffer = randomByteArrayOfLength(1024);
            indexOutput.writeBytes(buffer, buffer.length);
            CodecUtil.writeFooter(indexOutput);
        }

        byte[] luceneEncodedFileBytes = out.toByteArray();
        long checksum = CodecUtil.retrieveChecksum(new ByteArrayIndexInput("test", luceneEncodedFileBytes));

        String encodedChecksum = Store.digestToString(checksum);
        String writtenBy = org.apache.lucene.util.Version.LATEST.toString();
        return Tuple.tuple(
            new StoreFileMetadata(fileName, luceneEncodedFileBytes.length, encodedChecksum, writtenBy),
            luceneEncodedFileBytes
        );
    }
}
