/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.store.StoreFileMetadataDirectory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

public class RestoreReadOnlySnapshotIT extends AbstractSnapshotIntegTestCase {

    /**
     * Replaces the strict default pre-restore version check (which rejects indices created before
     * {@link IndexVersions#MINIMUM_COMPATIBLE}) with a permissive one that allows indices from
     * {@link IndexVersions#MINIMUM_READONLY_COMPATIBLE} onwards. This enables restoring a snapshot
     * whose index metadata has been patched to a 7.x compatibility version in order to trigger
     * {@link Store#checkAndPatchLocalCheckpoint()} via
     * {@code RestoreService.prepareForReadOnlyRestore()}.
     */
    public static class PermissiveRestorePlugin extends Plugin implements RepositoryPlugin {
        @Override
        public BiConsumer<Snapshot, IndexVersion> addPreRestoreVersionCheck() {
            return (snapshot, version) -> {
                if (version.before(IndexVersions.MINIMUM_READONLY_COMPATIBLE)) {
                    throw new SnapshotRestoreException(
                        snapshot,
                        "the snapshot was created with Elasticsearch version ["
                            + version.toReleaseVersion()
                            + "] which is below the minimum read-only compatible version ["
                            + IndexVersions.MINIMUM_READONLY_COMPATIBLE.toReleaseVersion()
                            + "]"
                    );
                }
            };
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PermissiveRestorePlugin.class);
    }

    /**
     * Verifies that {@link Store#checkAndPatchLocalCheckpoint()} repairs a restored shard commit
     * where {@code local_checkpoint < max_seq_no}. The test deterministically injects the lag by
     * patching the inline {@code segments_N} bytes in the repository blob, then patches the index
     * metadata to appear as a 7.x index so that {@code prepareForReadOnlyRestore} sets
     * {@code VERIFIED_READ_ONLY_SETTING}, which gates the patch call in {@code StoreRecovery.bootstrap()}.
     * After restore the shard's live seq-no stats must show {@code local_checkpoint == max_seq_no}.
     */
    public void testRestoreExercisesCheckAndPatchLocalCheckpoint() throws Exception {
        final String indexName = "test-lcp-patch";
        final String repoName = "test-lcp-patch-repo";
        final String snapshotName = "test-lcp-patch-snap";
        final int numDocs = 5;

        createRepository(repoName, FsRepository.TYPE);
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(Integer.toString(i)).setSource("field", i).get();
        }
        flushAndRefresh(indexName);
        createSnapshot(repoName, snapshotName, Collections.singletonList(indexName));

        final BlobStoreRepository repository = getRepositoryOnMaster(repoName);
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final SnapshotId snapshotId = repositoryData.getSnapshotIds()
            .stream()
            .filter(s -> snapshotName.equals(s.getName()))
            .findFirst()
            .orElseThrow();
        final IndexId indexId = repositoryData.resolveIndexId(indexName);

        long snapshotMaxSeqNo = patchShardSnapshotForLaggedCheckpoint(repository, indexId, snapshotId);

        // Verify the snapshot blob now carries local_checkpoint = max_seq_no - 1 before restore
        {
            var shardContainer = repository.shardContainer(indexId, 0);
            var patchedSnap = repository.loadShardSnapshot(shardContainer, snapshotId);
            var inlineFiles = patchedSnap.indexFiles()
                .stream()
                .map(BlobStoreIndexShardSnapshot.FileInfo::metadata)
                .filter(StoreFileMetadata::hashEqualsContents)
                .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
            SegmentInfos si = Lucene.readSegmentInfos(new StoreFileMetadataDirectory(inlineFiles));
            assertThat(Long.parseLong(si.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY)), equalTo(snapshotMaxSeqNo - 1));
        }

        patchIndexMetadataToOldVersion(repository, repositoryData, indexId, snapshotId);
        cluster().wipeIndices(indexName);

        final String restoredIndex = "test-lcp-patch-restored";
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, snapshotName)
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndex)
            .setWaitForCompletion(true)
            .get();
        ensureGreen(restoredIndex);

        // checkAndPatchLocalCheckpoint must have equalized local_checkpoint with max_seq_no in the Lucene commit;
        // the ReadOnlyEngine initialises from that commit, so the live shard stats reflect the patched values.
        ShardStats primaryStats = Arrays.stream(indicesAdmin().prepareStats(restoredIndex).clear().get().getShards())
            .filter(s -> s.getShardRouting().primary())
            .findFirst()
            .orElseThrow();

        assertNotNull(primaryStats.getSeqNoStats());
        assertThat(primaryStats.getSeqNoStats().getLocalCheckpoint(), equalTo((long) numDocs - 1));
        assertThat(primaryStats.getSeqNoStats().getMaxSeqNo(), equalTo((long) numDocs - 1));
        assertHitCount(prepareSearch(restoredIndex).setSize(0), numDocs);
    }

    /**
     * Patches the inline {@code segments_N} bytes in a shard snapshot manifest to set
     * {@code local_checkpoint = max_seq_no - 1}, so that {@link Store#checkAndPatchLocalCheckpoint()}
     * will detect and repair the discrepancy when the shard is restored.
     *
     * @return the original {@code max_seq_no} value from the snapshot commit
     */
    private long patchShardSnapshotForLaggedCheckpoint(BlobStoreRepository repository, IndexId indexId, SnapshotId snapshotId)
        throws IOException {
        var shardContainer = repository.shardContainer(indexId, 0);
        var shardSnapshot = repository.loadShardSnapshot(shardContainer, snapshotId);

        var segsFileInfo = shardSnapshot.indexFiles()
            .stream()
            .filter(fi -> fi.physicalName().startsWith("segments_"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no inline segments file in snapshot"));

        // Read SegmentInfos directly from inline hash bytes; matches the pattern used by readShardSnapshotCommitInfo
        var inlineFiles = shardSnapshot.indexFiles()
            .stream()
            .map(BlobStoreIndexShardSnapshot.FileInfo::metadata)
            .filter(StoreFileMetadata::hashEqualsContents)
            .collect(Collectors.toMap(StoreFileMetadata::name, Function.identity()));
        SegmentInfos si = Lucene.readSegmentInfos(new StoreFileMetadataDirectory(inlineFiles));

        // Decrement local_checkpoint by 1 and commit to a writable dir to get the patched bytes
        ByteBuffersDirectory tmpDir = new ByteBuffersDirectory();
        long maxSeqNo = Long.parseLong(si.userData.get(SequenceNumbers.MAX_SEQ_NO));
        Map<String, String> newUserData = new HashMap<>(si.userData);
        newUserData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo - 1));
        si.userData = newUserData;
        si.changed();
        si.commit(tmpDir);

        String newSegName = SegmentInfos.getLastCommitSegmentsFileName(tmpDir);
        long newLength = tmpDir.fileLength(newSegName);
        byte[] newBytes = new byte[(int) newLength];
        try (IndexInput in = tmpDir.openInput(newSegName, IOContext.DEFAULT)) {
            in.readBytes(newBytes, 0, newBytes.length);
        }
        BytesRef newHash = new BytesRef(newBytes);
        long checksum = CodecUtil.retrieveChecksum(new ByteArrayIndexInput(newSegName, newBytes));
        StoreFileMetadata newMeta = new StoreFileMetadata(
            newSegName,
            newLength,
            Store.digestToString(checksum),
            segsFileInfo.metadata().writtenBy(),
            newHash,
            StoreFileMetadata.UNAVAILABLE_WRITER_UUID
        );

        List<BlobStoreIndexShardSnapshot.FileInfo> patchedFiles = shardSnapshot.indexFiles()
            .stream()
            .map(fi -> fi.physicalName().startsWith("segments_") ? new BlobStoreIndexShardSnapshot.FileInfo(fi.name(), newMeta, null) : fi)
            .toList();
        BlobStoreIndexShardSnapshot patched = new BlobStoreIndexShardSnapshot(
            shardSnapshot.snapshot(),
            patchedFiles,
            shardSnapshot.startTime(),
            shardSnapshot.time(),
            shardSnapshot.incrementalFileCount(),
            shardSnapshot.incrementalSize()
        );
        BlobStoreRepository.INDEX_SHARD_SNAPSHOT_FORMAT.write(patched, shardContainer, snapshotId.getUUID(), false);
        return maxSeqNo;
    }

    /**
     * Overwrites the snapshot's index metadata blob with a copy that has
     * {@link IndexMetadata#SETTING_VERSION_CREATED} and {@link IndexMetadata#SETTING_VERSION_COMPATIBILITY}
     * set to a randomly chosen 7.x version. This causes
     * {@code RestoreService.prepareForReadOnlyRestore()} to add {@code VERIFIED_READ_ONLY_SETTING=true},
     * which in turn gates the {@link Store#checkAndPatchLocalCheckpoint()} call during bootstrap.
     */
    private void patchIndexMetadataToOldVersion(
        BlobStoreRepository repository,
        RepositoryData repositoryData,
        IndexId indexId,
        SnapshotId snapshotId
    ) throws IOException {
        IndexMetadata original = repository.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId);
        IndexVersion oldVersion = IndexVersionUtils.randomVersionBetween(
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.MINIMUM_COMPATIBLE)
        );
        IndexMetadata patched = IndexMetadata.builder(original)
            .settings(
                Settings.builder()
                    .put(original.getSettings())
                    .put(IndexMetadata.SETTING_VERSION_CREATED, oldVersion)
                    .put(IndexMetadata.SETTING_VERSION_COMPATIBILITY, oldVersion)
            )
            .build();

        String blobId = repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, indexId);
        var indexContainer = repository.blobStore().blobContainer(repository.basePath().add("indices").add(indexId.getId()));
        BlobStoreRepository.INDEX_METADATA_FORMAT.write(patched, indexContainer, blobId, false);
    }
}
