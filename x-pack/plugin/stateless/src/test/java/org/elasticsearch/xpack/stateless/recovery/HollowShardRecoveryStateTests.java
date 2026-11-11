/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.HollowCommitMetadata;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Verifies that the hollow commit metadata carried by primary relocation describes the commit as a read from the store would. */
public class HollowShardRecoveryStateTests extends ESTestCase {

    public void testMetadataMatchesSegmentInfos() throws Exception {
        for (boolean useCompoundFile : List.of(false, true)) {
            final var infos = hollowSegmentInfos(useCompoundFile, false);
            final var metadata = roundTrip(infos);
            assertEquals(infos.getGeneration(), metadata.generation());
            assertEquals(infos.getUserData(), metadata.userData());
            assertArrayEquals(infos.getId(), metadata.id());
            assertEquals(infos.getCommitLuceneVersion(), metadata.luceneVersion());
            assertEquals(infos.totalMaxDoc(), metadata.maxDoc());
            assertEquals(infos.size(), metadata.segmentCount());
            assertEquals(Set.copyOf(infos.files(true)), metadata.files());
            assertEquals(infos.getSegmentsFileName(), metadata.segmentsFileName());
            // The hollow engine reports commit stats from the metadata, they must match those computed from the segment infos
            assertEquals(new CommitStats(infos), metadata.commitStats());
            assertTrue(metadata.numDocs() < metadata.maxDoc());
        }
    }

    public void testEmptyCommitMetadata() throws Exception {
        final var infos = hollowSegmentInfos(randomBoolean(), true);
        final var metadata = HollowCommitMetadata.of(infos);
        assertEquals(0, metadata.segmentCount());
        assertEquals(Set.of(infos.getSegmentsFileName()), metadata.files());
        assertEquals(metadata, roundTrip(infos));
    }

    public void testSourceOnlyTransfersMetadataOfLatestUploadedCommit() throws Exception {
        final var infos = hollowSegmentInfos(randomBoolean(), randomBoolean());
        final var metadata = HollowCommitMetadata.of(infos);
        final var files = Set.copyOf(infos.files(true));
        final var shardId = new ShardId("test", "uuid", 0);

        final var latestBcc = hollowCommit(infos.getGeneration(), files);
        assertEquals(
            new HollowShardRecoveryState(latestBcc, metadata),
            StatelessPrimaryRelocationSourceService.hollowRecoveryState(shardId, metadata, latestBcc)
        );
        // the engine is not hollow
        assertNull(StatelessPrimaryRelocationSourceService.hollowRecoveryState(shardId, null, latestBcc));
        // a concurrent flush uploaded another hollow commit after the engine was hollowed
        final var newerBcc = hollowCommit(infos.getGeneration() + 1, files);
        assertNull(StatelessPrimaryRelocationSourceService.hollowRecoveryState(shardId, metadata, newerBcc));
    }

    public void testRejectsMismatchedOrNonHollowCommits() throws Exception {
        final var infos = hollowSegmentInfos(randomBoolean(), randomBoolean());
        final var metadata = HollowCommitMetadata.of(infos);
        final var files = Set.copyOf(infos.files(true));
        assertTrue(HollowShardRecoveryState.matches(hollowCommit(infos.getGeneration(), files), metadata));
        expectThrows(
            IllegalArgumentException.class,
            () -> new HollowShardRecoveryState(hollowCommit(infos.getGeneration() + 1, files), metadata)
        );
        final var otherFiles = new HashSet<>(files);
        otherFiles.add("_extra.si");
        expectThrows(
            IllegalArgumentException.class,
            () -> new HollowShardRecoveryState(hollowCommit(infos.getGeneration(), otherFiles), metadata)
        );

        final var userData = new HashMap<>(infos.getUserData());
        userData.put(StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE, "1");
        expectThrows(
            IllegalArgumentException.class,
            () -> new HollowCommitMetadata(
                metadata.generation(),
                userData,
                metadata.id(),
                metadata.luceneVersion(),
                metadata.numDocs(),
                metadata.maxDoc(),
                metadata.segmentCount(),
                metadata.files()
            )
        );
    }

    public void testHandoffTransportVersions() throws IOException {
        final var infos = hollowSegmentInfos(randomBoolean(), randomBoolean());
        final var bcc = hollowCommit(infos.getGeneration(), Set.copyOf(infos.files(true)));
        final var state = new HollowShardRecoveryState(bcc, HollowCommitMetadata.of(infos));
        for (var hollowRecoveryState : Arrays.asList(state, null)) {
            final var request = new TransportStatelessPrimaryRelocationHandoffAction.Request(
                1,
                bcc.shardId(),
                new ReplicationTracker.PrimaryContext(
                    1,
                    Map.of(),
                    new IndexShardRoutingTable.Builder(bcc.shardId()).addShard(
                        TestShardRouting.newShardRouting(bcc.shardId(), "source", true, ShardRoutingState.STARTED)
                    ).build()
                ),
                RetentionLeases.EMPTY,
                Map.of(),
                new BlobFileWithLength(bcc.toBlobFile(), bcc.calculateBccBlobLength()),
                Set.of(),
                false,
                Set.of(bcc.toBlobFile()),
                true,
                hollowRecoveryState
            );
            for (var version : List.of(
                TransportVersion.current(),
                TransportVersionUtils.randomVersionNotSupporting(
                    TransportStatelessPrimaryRelocationHandoffAction.HOLLOW_SHARD_RECOVERY_STATE
                )
            )) {
                final var copy = copyWriteable(
                    request,
                    new NamedWriteableRegistry(List.of()),
                    TransportStatelessPrimaryRelocationHandoffAction.Request::new,
                    version
                );
                final var hints = copy.recoveryInfoFromSource();
                assertEquals(request.recoveryInfoFromSource().sourceBlobsInfo(), hints.sourceBlobsInfo());
                if (hollowRecoveryState != null
                    && version.supports(TransportStatelessPrimaryRelocationHandoffAction.HOLLOW_SHARD_RECOVERY_STATE)) {
                    assertEquals(state, hints.hollowRecoveryState());
                } else {
                    assertNull(hints.hollowRecoveryState());
                }
            }
        }
    }

    /**
     * Writes a hollow commit with several segments, hard and soft deletes, and doc values updates, and reads it back as the store would.
     */
    private static SegmentInfos hollowSegmentInfos(boolean useCompoundFile, boolean empty) throws IOException {
        try (var directory = new ByteBuffersDirectory()) {
            try (
                var writer = new IndexWriter(
                    directory,
                    new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
                        .setUseCompoundFile(useCompoundFile)
                        .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                )
            ) {
                if (empty == false) {
                    for (int i = 0; i < 12; i++) {
                        writer.addDocument(doc(i));
                        if (i % 4 == 3) {
                            writer.commit();
                        }
                    }
                    writer.deleteDocuments(new Term("id", "0"));
                    writer.softUpdateDocument(new Term("id", "1"), doc(1), new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1));
                    writer.updateNumericDocValue(new Term("id", "5"), "value", 42);
                }
                writer.setLiveCommitData(
                    Map.of(
                        StatelessCompoundCommit.TRANSLOG_RECOVERY_START_FILE,
                        Long.toString(StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE),
                        Engine.HISTORY_UUID_KEY,
                        "history",
                        SequenceNumbers.LOCAL_CHECKPOINT_KEY,
                        "11",
                        SequenceNumbers.MAX_SEQ_NO,
                        "11"
                    ).entrySet()
                );
                writer.commit();
            }
            return SegmentInfos.readLatestCommit(directory);
        }
    }

    private static Document doc(int id) {
        var doc = new Document();
        doc.add(new StringField("id", Integer.toString(id), Field.Store.NO));
        doc.add(new NumericDocValuesField("value", id));
        return doc;
    }

    /**
     * Sends the metadata to the target as the handoff does, which takes the files of the commit from the hollow compound commit.
     */
    private static HollowCommitMetadata roundTrip(SegmentInfos infos) throws IOException {
        final var state = new HollowShardRecoveryState(
            hollowCommit(infos.getGeneration(), Set.copyOf(infos.files(true))),
            HollowCommitMetadata.of(infos)
        );
        final var copy = copyWriteable(state, new NamedWriteableRegistry(List.of()), HollowShardRecoveryState::new);
        assertEquals(state, copy);
        return copy.metadata();
    }

    private static BatchedCompoundCommit hollowCommit(long generation, Set<String> files) {
        final var termAndGeneration = new PrimaryTermAndGeneration(1, generation);
        final var blobFile = new BlobFile(BatchedCompoundCommit.blobNameFromGeneration(generation), termAndGeneration);
        final var commit = StatelessCompoundCommit.newHollowStatelessCompoundCommit(
            new ShardId("test", "uuid", 0),
            termAndGeneration,
            files.stream().collect(Collectors.toMap(Function.identity(), file -> new BlobLocation(blobFile, 0, 1))),
            100,
            Set.of(),
            0,
            InternalFilesReplicatedRanges.EMPTY,
            Map.of(),
            null
        );
        return new BatchedCompoundCommit(termAndGeneration, List.of(commit));
    }
}
