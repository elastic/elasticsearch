/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BlobStoreRepositoryReshardingTests extends IndexShardTestCase {
    private final List<IndexShard> shards = new ArrayList<>();

    @After
    public void teardown() throws IOException {
        for (IndexShard s : shards) {
            try {
                closeShardNoCheck(s);
            } finally {
                IOUtils.close(s.store());
            }
        }
        shards.clear();
    }

    public void testSingleReshardingRoundMetadataAdjustment() throws IOException {
        ProjectId projectId = randomProjectIdOrDefault();
        var indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        var shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        shards.add(shard);
        recoverShardFromStore(shard);

        Repository repository = TestUtils.createRepository(projectId, createTempDir(), xContentRegistry());

        // Perform a normal shard snapshot unaffected by resharding.
        var snapshot = new Snapshot(projectId, repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "snapshot1"));
        ShardGeneration shardGen = snapshotShard(shard, snapshot, repository);
        assertNotNull(shardGen);

        var snapshotShardGenerations = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder().put(indexId, 0, shardGen).build(),
            ShardGenerations.EMPTY
        );
        RepositoryData repositoryData1 = finalizeSnapshot(
            projectId,
            repository,
            RepositoryData.EMPTY_REPO_GEN,
            snapshot,
            snapshotShardGenerations,
            shard.indexSettings().getIndexMetadata()
        );
        assertEquals(1, repositoryData1.getSnapshots(indexId).size());
        String snapshot1MetadataBlobId = repositoryData1.indexMetaDataGenerations().indexMetaBlobId(snapshot.getSnapshotId(), indexId);
        assertNotNull(snapshot1MetadataBlobId);

        // Now create a snapshot with index metadata adjustment due to resharding.
        var snapshot2 = new Snapshot(projectId, repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "snapshot2"));
        ShardGeneration shardGen2 = snapshotShard(shard, snapshot2, repository);
        assertNotNull(shardGen);

        var snapshotShardGenerations2 = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder().put(indexId, 0, shardGen2).build(),
            ShardGenerations.EMPTY
        );

        // Emulate a split 1 -> 2.
        IndexMetadata reshardedMetadata = IndexMetadata.builder(shard.indexSettings().getIndexMetadata())
            .reshardAddShards(2)
            .settingsVersion(shard.indexSettings().getIndexMetadata().getSettingsVersion() + 1)
            .build();

        RepositoryData repositoryData2 = finalizeSnapshot(
            projectId,
            repository,
            repositoryData1.getGenId(),
            snapshot2,
            snapshotShardGenerations2,
            reshardedMetadata
        );
        assertEquals(2, repositoryData2.getSnapshots(indexId).size());
        String snapshot2MetadataIdentifier = repositoryData2.indexMetaDataGenerations()
            .snapshotIndexMetadataIdentifier(snapshot2.getSnapshotId(), indexId);
        assertTrue(snapshot2MetadataIdentifier.startsWith(BlobStoreRepository.RESHARDED_METADATA_IDENTIFIER_PREFIX));
        String snapshot2MetadataBlobId = repositoryData2.indexMetaDataGenerations().indexMetaBlobId(snapshot2.getSnapshotId(), indexId);
        // We didn't reuse the metadata blob since we performed adjustments on the index metadata and it's different now.
        assertNotEquals(snapshot2MetadataBlobId, snapshot1MetadataBlobId);
    }

    public void testNextSnapshotPostAdjustment() throws IOException {
        ProjectId projectId = randomProjectIdOrDefault();
        var indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        var shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard0 = newShard(shardId, true);
        shards.add(shard0);
        recoverShardFromStore(shard0);

        Repository repository = TestUtils.createRepository(projectId, createTempDir(), xContentRegistry());

        // Emulate a split 1 -> 2.
        IndexMetadata reshardedMetadata = IndexMetadata.builder(shard0.indexSettings().getIndexMetadata())
            .reshardAddShards(2)
            .settingsVersion(shard0.indexSettings().getIndexMetadata().getSettingsVersion() + 1)
            .build();

        // Perform a snapshot that adjusts index metadata.
        var snapshot = new Snapshot(projectId, repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "snapshot1"));
        ShardGeneration shardGen = snapshotShard(shard0, snapshot, repository);
        assertNotNull(shardGen);

        var snapshotShardGenerations = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder().put(indexId, 0, shardGen).build(),
            ShardGenerations.EMPTY
        );
        RepositoryData repositoryData1 = finalizeSnapshot(
            projectId,
            repository,
            RepositoryData.EMPTY_REPO_GEN,
            snapshot,
            snapshotShardGenerations,
            reshardedMetadata
        );
        assertEquals(1, repositoryData1.getSnapshots(indexId).size());
        String snapshot1MetadataIdentifier = repositoryData1.indexMetaDataGenerations()
            .snapshotIndexMetadataIdentifier(snapshot.getSnapshotId(), indexId);
        assertTrue(snapshot1MetadataIdentifier.startsWith(BlobStoreRepository.RESHARDED_METADATA_IDENTIFIER_PREFIX));
        String snapshot1MetadataBlobId = repositoryData1.indexMetaDataGenerations().indexMetaBlobId(snapshot.getSnapshotId(), indexId);

        // Now create a post-resharding snapshot.
        IndexShard shard1 = newShard(
            new ShardId(indexId.getName(), indexId.getId(), 1),
            true,
            "node1",
            reshardedMetadata.withIncrementedPrimaryTerm(1),
            directoryReader -> directoryReader
        );
        shards.add(shard1);
        recoverShardFromStore(shard1);

        var snapshot2 = new Snapshot(projectId, repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "snapshot2"));
        ShardGeneration snapshot2Shard0Generation = snapshotShard(shard0, snapshot2, repository);
        assertNotNull(snapshot2Shard0Generation);
        ShardGeneration snapshot2Shard1Generation = snapshotShard(shard1, snapshot2, repository);
        assertNotNull(snapshot2Shard1Generation);

        var snapshotShardGenerations2 = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder().put(indexId, 0, snapshot2Shard0Generation).put(indexId, 1, snapshot2Shard1Generation).build(),
            ShardGenerations.EMPTY
        );

        RepositoryData repositoryData2 = finalizeSnapshot(
            projectId,
            repository,
            repositoryData1.getGenId(),
            snapshot2,
            snapshotShardGenerations2,
            reshardedMetadata
        );
        assertEquals(2, repositoryData2.getSnapshots(indexId).size());
        // We didn't reuse the metadata blob since previous metadata was adjusted and as such shouldn't be reused.
        assertNotEquals(
            snapshot1MetadataBlobId,
            repositoryData2.indexMetaDataGenerations().indexMetaBlobId(snapshot2.getSnapshotId(), indexId)
        );
    }

    public void testMultipleReshardingRoundsMetadataAdjustment() throws IOException {
        ProjectId projectId = randomProjectIdOrDefault();
        var indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());

        IndexShard shard0 = newShard(new ShardId(indexId.getName(), indexId.getId(), 0), true);
        shards.add(shard0);
        recoverShardFromStore(shard0);

        Repository repository = TestUtils.createRepository(projectId, createTempDir(), xContentRegistry());

        // Perform a normal shard snapshot unaffected by resharding.
        var snapshot = new Snapshot(projectId, repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "snapshot1"));
        ShardGeneration initialSnapshotShard0Generation = snapshotShard(shard0, snapshot, repository);
        assertNotNull(initialSnapshotShard0Generation);

        var snapshot1ShardGenerations = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder().put(indexId, 0, initialSnapshotShard0Generation).build(),
            ShardGenerations.EMPTY
        );
        RepositoryData repositoryData1 = finalizeSnapshot(
            projectId,
            repository,
            RepositoryData.EMPTY_REPO_GEN,
            snapshot,
            snapshot1ShardGenerations,
            shard0.indexSettings().getIndexMetadata()
        );
        assertEquals(1, repositoryData1.getSnapshots(indexId).size());
        String snapshot1MetadataBlobId = repositoryData1.indexMetaDataGenerations().indexMetaBlobId(snapshot.getSnapshotId(), indexId);
        assertNotNull(snapshot1MetadataBlobId);

        // Now create a snapshot with index metadata adjustment due to resharding.
        var snapshot2 = new Snapshot(projectId, repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "snapshot2"));
        ShardGeneration secondSnapshotShard0Generation = snapshotShard(shard0, snapshot2, repository);
        assertNotNull(secondSnapshotShard0Generation);

        var snapshot2ShardGenerations = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder().put(indexId, 0, secondSnapshotShard0Generation).build(),
            ShardGenerations.EMPTY
        );

        // Emulate a split 1 -> 2.
        IndexMetadata reshardedMetadata = IndexMetadata.builder(shard0.indexSettings().getIndexMetadata())
            .reshardAddShards(2)
            .settingsVersion(shard0.indexSettings().getIndexMetadata().getSettingsVersion() + 1)
            .build();

        RepositoryData repositoryData2 = finalizeSnapshot(
            projectId,
            repository,
            repositoryData1.getGenId(),
            snapshot2,
            snapshot2ShardGenerations,
            reshardedMetadata
        );
        assertEquals(2, repositoryData2.getSnapshots(indexId).size());
        String snapshot2MetadataIdentifier = repositoryData2.indexMetaDataGenerations()
            .snapshotIndexMetadataIdentifier(snapshot2.getSnapshotId(), indexId);
        assertTrue(snapshot2MetadataIdentifier.startsWith(BlobStoreRepository.RESHARDED_METADATA_IDENTIFIER_PREFIX));
        String snapshot2MetadataBlobId = repositoryData2.indexMetaDataGenerations().indexMetaBlobId(snapshot2.getSnapshotId(), indexId);
        // We didn't reuse the metadata blob since we performed adjustments on the index metadata and it's different now.
        assertNotEquals(snapshot1MetadataBlobId, snapshot2MetadataBlobId);

        // Now create a snapshot with index metadata adjustment due to yet another resharding step.
        IndexShard shard1 = newShard(
            new ShardId(indexId.getName(), indexId.getId(), 1),
            true,
            "node1",
            reshardedMetadata.withIncrementedPrimaryTerm(1),
            directoryReader -> directoryReader
        );
        shards.add(shard1);
        recoverShardFromStore(shard1);

        var finalSnapshot = new Snapshot(projectId, repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "snapshot3"));

        ShardGeneration finalSnapshotShard0Generation = snapshotShard(shard0, finalSnapshot, repository);
        assertNotNull(finalSnapshotShard0Generation);
        ShardGeneration finalSnapshotShard1Generation = snapshotShard(shard1, finalSnapshot, repository);
        assertNotNull(finalSnapshotShard1Generation);

        var finalSnapshotShardGenerations = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder()
                .put(indexId, 0, finalSnapshotShard0Generation)
                .put(indexId, 1, finalSnapshotShard1Generation)
                .build(),
            ShardGenerations.EMPTY
        );
        // Emulate another split 2 -> 4 and a snapshot that started when shard count was 2.
        IndexMetadata twiceReshardedMetadata = IndexMetadata.builder(reshardedMetadata)
            .reshardAddShards(4)
            .settingsVersion(shard0.indexSettings().getIndexMetadata().getSettingsVersion() + 1)
            .build();

        RepositoryData finalRepositoryData = finalizeSnapshot(
            projectId,
            repository,
            repositoryData2.getGenId(),
            finalSnapshot,
            finalSnapshotShardGenerations,
            twiceReshardedMetadata
        );
        assertEquals(3, finalRepositoryData.getSnapshots(indexId).size());
        String finalMetadataIdentifier = finalRepositoryData.indexMetaDataGenerations()
            .snapshotIndexMetadataIdentifier(finalSnapshot.getSnapshotId(), indexId);
        assertTrue(finalMetadataIdentifier.startsWith(BlobStoreRepository.RESHARDED_METADATA_IDENTIFIER_PREFIX));
        String finalMetadataBlobId = finalRepositoryData.indexMetaDataGenerations().indexMetaBlobId(finalSnapshot.getSnapshotId(), indexId);
        // We didn't reuse the metadata blob since the index settings version advanced due to resharding
        // and we performed adjustments on the index metadata.
        assertNotEquals(snapshot1MetadataBlobId, finalMetadataBlobId);
        assertNotEquals(snapshot2MetadataBlobId, finalMetadataBlobId);
    }

    public void testPostReshardingFinalizationBeforeAdjustedMetadataFinalization() throws IOException {
        ProjectId projectId = randomProjectIdOrDefault();
        var indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());

        IndexShard shard0 = newShard(new ShardId(indexId.getName(), indexId.getId(), 0), true);
        shards.add(shard0);
        recoverShardFromStore(shard0);

        // Emulate a split 1 -> 2.
        IndexMetadata reshardedMetadata = IndexMetadata.builder(shard0.indexSettings().getIndexMetadata())
            .reshardAddShards(2)
            .settingsVersion(shard0.indexSettings().getIndexMetadata().getSettingsVersion() + 1)
            .build();

        IndexShard shard1 = newShard(
            new ShardId(indexId.getName(), indexId.getId(), 1),
            true,
            "node1",
            reshardedMetadata.withIncrementedPrimaryTerm(1),
            directoryReader -> directoryReader
        );
        shards.add(shard1);
        recoverShardFromStore(shard1);

        Repository repository = TestUtils.createRepository(projectId, createTempDir(), xContentRegistry());
        // Perform a normal post-resharding snapshot.
        var postReshardSnapshot = new Snapshot(
            projectId,
            repository.getMetadata().name(),
            new SnapshotId(randomAlphaOfLength(10), "snapshot1")
        );

        ShardGeneration postReshardShard0Generation = snapshotShard(shard0, postReshardSnapshot, repository);
        assertNotNull(postReshardShard0Generation);
        ShardGeneration postReshardShard1Generation = snapshotShard(shard1, postReshardSnapshot, repository);
        assertNotNull(postReshardShard1Generation);

        var postReshardShardGenerations = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder().put(indexId, 0, postReshardShard0Generation).put(indexId, 1, postReshardShard1Generation).build(),
            ShardGenerations.EMPTY
        );

        RepositoryData initialRepositoryData = finalizeSnapshot(
            projectId,
            repository,
            RepositoryData.EMPTY_REPO_GEN,
            postReshardSnapshot,
            postReshardShardGenerations,
            reshardedMetadata
        );
        assertEquals(1, initialRepositoryData.getSnapshots(indexId).size());
        String postReshardSnapshotMetadataBlobId = initialRepositoryData.indexMetaDataGenerations()
            .indexMetaBlobId(postReshardSnapshot.getSnapshotId(), indexId);
        assertNotNull(postReshardSnapshotMetadataBlobId);

        // Now we'll perform and finalize a snapshot that started before resharding to emulate out of order finalization.
        var preReshardSnapshot = new Snapshot(
            projectId,
            repository.getMetadata().name(),
            new SnapshotId(randomAlphaOfLength(10), "snapshot2")
        );

        ShardGeneration preReshardShard0Generation = snapshotShard(shard0, preReshardSnapshot, repository);
        assertNotNull(preReshardShard0Generation);

        var preReshardShardGenerations = new FinalizeSnapshotContext.UpdatedShardGenerations(
            ShardGenerations.builder().put(indexId, 0, preReshardShard0Generation).build(),
            ShardGenerations.EMPTY
        );

        // Note that we are still using `reshardedMetadata` here since that is the latest state of the index metadata (resharding is
        // complete at this point).
        RepositoryData repositoryData = finalizeSnapshot(
            projectId,
            repository,
            initialRepositoryData.getGenId(),
            preReshardSnapshot,
            preReshardShardGenerations,
            reshardedMetadata
        );
        assertEquals(2, repositoryData.getSnapshots(indexId).size());
        String preReshardSnapshotMetadataIdentifier = repositoryData.indexMetaDataGenerations()
            .snapshotIndexMetadataIdentifier(preReshardSnapshot.getSnapshotId(), indexId);
        assertTrue(preReshardSnapshotMetadataIdentifier.startsWith(BlobStoreRepository.RESHARDED_METADATA_IDENTIFIER_PREFIX));
        String preReshardSnapshotMetadataBlobId = repositoryData.indexMetaDataGenerations()
            .indexMetaBlobId(preReshardSnapshot.getSnapshotId(), indexId);
        // We didn't reuse the metadata blob since we performed adjustments on the index metadata and it's different now.
        assertNotEquals(preReshardSnapshotMetadataBlobId, postReshardSnapshotMetadataBlobId);
    }

    private RepositoryData finalizeSnapshot(
        ProjectId projectId,
        Repository repository,
        long repositoryGeneration,
        Snapshot snapshot,
        FinalizeSnapshotContext.UpdatedShardGenerations updatedShardGenerations,
        IndexMetadata indexMetadata
    ) {
        return safeAwait(
            listener -> repository.finalizeSnapshot(
                new FinalizeSnapshotContext(
                    false,
                    updatedShardGenerations,
                    repositoryGeneration,
                    Metadata.builder().put(ProjectMetadata.builder(projectId).put(indexMetadata, false)).build(),
                    new SnapshotInfo(
                        snapshot,
                        updatedShardGenerations.liveIndices().indices().stream().map(IndexId::getName).toList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        null,
                        1L,
                        6,
                        Collections.emptyList(),
                        false,
                        Collections.emptyMap(),
                        0L,
                        Collections.emptyMap()
                    ),
                    IndexVersion.current(),
                    listener,
                    () -> {}
                )
            )
        );
    }
}
