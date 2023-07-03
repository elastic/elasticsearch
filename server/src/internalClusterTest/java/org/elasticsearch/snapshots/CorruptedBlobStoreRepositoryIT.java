/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentFactory;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFileExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class CorruptedBlobStoreRepositoryIT extends AbstractSnapshotIntegTestCase {

    public void testRecreateCorruptedRepositoryUnblocksIt() throws Exception {
        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        Settings.Builder settings = Settings.builder().put("location", repo);
        createRepository(repoName, "fs", settings);

        createIndex("test-idx-1");
        logger.info("--> indexing some data");
        indexRandom(true, client().prepareIndex("test-idx-1").setSource("foo", "bar"));

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .setIndices("test-idx-1")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repoName);
        Files.move(repo.resolve("index-" + repositoryData.getGenId()), repo.resolve("index-" + (repositoryData.getGenId() + 1)));

        assertRepositoryBlocked(repoName, snapshot);

        logger.info("--> recreate repository with same settings in order to reset corrupted state");
        assertAcked(clusterAdmin().preparePutRepository(repoName).setType("fs").setSettings(settings));

        startDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareGetSnapshots(repoName).addSnapshots(snapshot).get());
    }

    public void testConcurrentlyChangeRepositoryContents() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(
            repoName,
            "fs",
            Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repoName);
        Files.move(repo.resolve("index-" + repositoryData.getGenId()), repo.resolve("index-" + (repositoryData.getGenId() + 1)));

        assertRepositoryBlocked(repoName, snapshot);

        if (randomBoolean()) {
            logger.info("--> move index-N blob back to initial generation");
            Files.move(repo.resolve("index-" + (repositoryData.getGenId() + 1)), repo.resolve("index-" + repositoryData.getGenId()));

            logger.info("--> verify repository remains blocked");
            assertRepositoryBlocked(repoName, snapshot);
        }

        logger.info("--> remove repository");
        assertAcked(client.admin().cluster().prepareDeleteRepository(repoName));

        logger.info("--> recreate repository");
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(repoName)
                .setType("fs")
                .setSettings(
                    Settings.builder()
                        .put("location", repo)
                        .put("compress", false)
                        .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                )
        );

        startDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots(repoName).addSnapshots(snapshot).get()
        );
    }

    public void testFindDanglingLatestGeneration() throws Exception {
        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(
            repoName,
            "fs",
            Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        final Repository repository = getRepositoryOnMaster(repoName);

        logger.info("--> move index-N blob to next generation");
        final RepositoryData repositoryData = getRepositoryData(repoName);
        final long beforeMoveGen = repositoryData.getGenId();
        Files.move(repo.resolve("index-" + beforeMoveGen), repo.resolve("index-" + (beforeMoveGen + 1)));

        logger.info("--> set next generation as pending in the cluster state");
        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .metadata(
                    Metadata.builder(currentState.getMetadata())
                        .putCustom(
                            RepositoriesMetadata.TYPE,
                            currentState.metadata()
                                .<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE)
                                .withUpdatedGeneration(repository.getMetadata().name(), beforeMoveGen, beforeMoveGen + 1)
                        )
                        .build()
                )
                .build()
        );

        logger.info("--> full cluster restart");
        internalCluster().fullRestart();
        ensureGreen();

        logger.info("--> verify index-N blob is found at the new location");
        assertThat(getRepositoryData(repoName).getGenId(), is(beforeMoveGen + 1));

        startDeleteSnapshot(repoName, snapshot).get();

        logger.info("--> verify index-N blob is found at the expected location");
        assertThat(getRepositoryData(repoName).getGenId(), is(beforeMoveGen + 2));

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareGetSnapshots(repoName).addSnapshots(snapshot).get());
    }

    public void testHandlingMissingRootLevelSnapshotMetadata() throws Exception {
        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(
            repoName,
            "fs",
            Settings.builder()
                .put("location", repo)
                .put("compress", false)
                // Don't cache repository data because the test manually modifies the repository data
                .put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        final String snapshotPrefix = "test-snap-";
        final int snapshots = randomIntBetween(1, 2);
        logger.info("--> creating [{}] snapshots", snapshots);
        for (int i = 0; i < snapshots; ++i) {
            // Workaround to simulate BwC situation: taking a snapshot without indices here so that we don't create any new version shard
            // generations (the existence of which would short-circuit checks for the repo containing old version snapshots)
            CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(repoName, snapshotPrefix + i)
                .setIndices()
                .setWaitForCompletion(true)
                .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), is(0));
            assertThat(
                createSnapshotResponse.getSnapshotInfo().successfulShards(),
                equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
            );
        }
        final RepositoryData repositoryData = getRepositoryData(repoName);

        final SnapshotId snapshotToCorrupt = randomFrom(repositoryData.getSnapshotIds());
        logger.info("--> delete root level snapshot metadata blob for snapshot [{}]", snapshotToCorrupt);
        Files.delete(repo.resolve(Strings.format(BlobStoreRepository.SNAPSHOT_NAME_FORMAT, snapshotToCorrupt.getUUID())));

        logger.info("--> strip version information from index-N blob");
        final RepositoryData withoutVersions = new RepositoryData(
            RepositoryData.MISSING_UUID, // old-format repository data has no UUID
            repositoryData.getGenId(),
            repositoryData.getSnapshotIds().stream().collect(Collectors.toMap(SnapshotId::getUUID, Function.identity())),
            repositoryData.getSnapshotIds()
                .stream()
                .collect(
                    Collectors.toMap(
                        SnapshotId::getUUID,
                        s -> new RepositoryData.SnapshotDetails(repositoryData.getSnapshotState(s), null, -1, -1, null)
                    )
                ),
            Collections.emptyMap(),
            ShardGenerations.EMPTY,
            IndexMetaDataGenerations.EMPTY,
            RepositoryData.MISSING_UUID
        ); // old-format repository has no cluster UUID

        Files.write(
            repo.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + withoutVersions.getGenId()),
            BytesReference.toBytes(
                BytesReference.bytes(withoutVersions.snapshotsToXContent(XContentFactory.jsonBuilder(), Version.CURRENT, true))
            ),
            StandardOpenOption.TRUNCATE_EXISTING
        );

        logger.info("--> verify that repo is assumed in old metadata format");
        final ThreadPool threadPool = internalCluster().getCurrentMasterNodeInstance(ThreadPool.class);
        assertThat(
            PlainActionFuture.get(
                f -> threadPool.generic()
                    .execute(
                        ActionRunnable.supply(
                            f,
                            () -> SnapshotsService.minCompatibleVersion(Version.CURRENT, getRepositoryData(repoName), null)
                        )
                    )
            ),
            is(SnapshotsService.OLD_SNAPSHOT_FORMAT)
        );

        logger.info("--> verify that snapshot with missing root level metadata can be deleted");
        assertAcked(startDeleteSnapshot(repoName, snapshotToCorrupt.getName()).get());

        logger.info("--> verify that repository is assumed in new metadata format after removing corrupted snapshot");
        assertThat(
            PlainActionFuture.get(
                f -> threadPool.generic()
                    .execute(
                        ActionRunnable.supply(
                            f,
                            () -> SnapshotsService.minCompatibleVersion(Version.CURRENT, getRepositoryData(repoName), null)
                        )
                    )
            ),
            is(Version.CURRENT)
        );
        final RepositoryData finalRepositoryData = getRepositoryData(repoName);
        for (SnapshotId snapshotId : finalRepositoryData.getSnapshotIds()) {
            assertThat(finalRepositoryData.getVersion(snapshotId), is(IndexVersion.current()));
        }
    }

    public void testMountCorruptedRepositoryData() throws Exception {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository contents");
        Client client = client();

        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(
            repoName,
            "fs",
            Settings.builder()
                .put("location", repo)
                // Don't cache repository data because the test manually modifies the repository data
                .put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false)
                .put("compress", false)
        );

        final String snapshot = "test-snap";

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshot)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> corrupt index-N blob");
        final Repository repository = getRepositoryOnMaster(repoName);
        final RepositoryData repositoryData = getRepositoryData(repoName);
        Files.write(repo.resolve("index-" + repositoryData.getGenId()), randomByteArrayOfLength(randomIntBetween(1, 100)));

        logger.info("--> verify loading repository data throws RepositoryException");
        expectThrows(RepositoryException.class, () -> getRepositoryData(repository));

        final String otherRepoName = "other-repo";
        assertAcked(
            clusterAdmin().preparePutRepository(otherRepoName)
                .setType("fs")
                .setVerify(false) // don't try and load the repo data, since it is corrupt
                .setSettings(Settings.builder().put("location", repo).put("compress", false))
        );
        final Repository otherRepo = getRepositoryOnMaster(otherRepoName);

        logger.info("--> verify loading repository data from newly mounted repository throws RepositoryException");
        expectThrows(RepositoryException.class, () -> getRepositoryData(otherRepo));
    }

    public void testHandleSnapshotErrorWithBwCFormat() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);
        final String oldVersionSnapshot = initWithSnapshotVersion(repoName, repoPath, SnapshotsService.OLD_SNAPSHOT_FORMAT);

        final String indexName = "test-index";
        createIndex(indexName);

        createFullSnapshot(repoName, "snapshot-1");

        // In the old metadata version the shard level metadata could be moved to the next generation for all sorts of reasons, this should
        // not break subsequent repository operations
        logger.info("--> move shard level metadata to new generation");
        final IndexId indexId = getRepositoryData(repoName).resolveIndexId(indexName);
        final Path shardPath = repoPath.resolve("indices").resolve(indexId.getId()).resolve("0");
        final Path initialShardMetaPath = shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + "0");
        assertFileExists(initialShardMetaPath);
        Files.move(initialShardMetaPath, shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + "1"));

        startDeleteSnapshot(repoName, oldVersionSnapshot).get();

        createFullSnapshot(repoName, "snapshot-2");
    }

    public void testRepairBrokenShardGenerations() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "fs", repoPath);
        final String oldVersionSnapshot = initWithSnapshotVersion(repoName, repoPath, SnapshotsService.OLD_SNAPSHOT_FORMAT);

        final String indexName = "test-index";
        createIndex(indexName);

        createFullSnapshot(repoName, "snapshot-1");

        startDeleteSnapshot(repoName, oldVersionSnapshot).get();

        logger.info("--> move shard level metadata to new generation and make RepositoryData point at an older generation");
        final IndexId indexId = getRepositoryData(repoName).resolveIndexId(indexName);
        final Path shardPath = repoPath.resolve("indices").resolve(indexId.getId()).resolve("0");
        final Path initialShardMetaPath = shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + "0");
        assertFileExists(initialShardMetaPath);
        Files.move(initialShardMetaPath, shardPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + randomIntBetween(1, 1000)));

        final RepositoryData repositoryData = getRepositoryData(repoName);
        final Map<String, SnapshotId> snapshotIds = repositoryData.getSnapshotIds()
            .stream()
            .collect(Collectors.toMap(SnapshotId::getUUID, Function.identity()));
        final RepositoryData brokenRepoData = new RepositoryData(
            repositoryData.getUuid(),
            repositoryData.getGenId(),
            snapshotIds,
            snapshotIds.values().stream().collect(Collectors.toMap(SnapshotId::getUUID, repositoryData::getSnapshotDetails)),
            repositoryData.getIndices().values().stream().collect(Collectors.toMap(Function.identity(), repositoryData::getSnapshots)),
            ShardGenerations.builder().putAll(repositoryData.shardGenerations()).put(indexId, 0, new ShardGeneration(0L)).build(),
            repositoryData.indexMetaDataGenerations(),
            repositoryData.getClusterUUID()
        );
        Files.write(
            repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + repositoryData.getGenId()),
            BytesReference.toBytes(
                BytesReference.bytes(brokenRepoData.snapshotsToXContent(XContentFactory.jsonBuilder(), Version.CURRENT))
            ),
            StandardOpenOption.TRUNCATE_EXISTING
        );

        logger.info("--> recreating repository to clear caches");
        clusterAdmin().prepareDeleteRepository(repoName).get();
        createRepository(repoName, "fs", repoPath);

        createFullSnapshot(repoName, "snapshot-2");
    }

    /**
     * Tests that a shard snapshot with a corrupted shard index file can still be used for restore and incremental snapshots.
     */
    public void testSnapshotWithCorruptedShardIndexFile() throws Exception {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository contents");

        final Client client = client();
        final Path repo = randomRepoPath();
        final String indexName = "test-idx";
        final int nDocs = randomIntBetween(1, 10);

        logger.info("-->  creating index [{}] with [{}] documents in it", indexName, nDocs);
        assertAcked(prepareCreate(indexName).setSettings(indexSettingsNoReplicas(1)));

        final IndexRequestBuilder[] documents = new IndexRequestBuilder[nDocs];
        for (int j = 0; j < nDocs; j++) {
            documents[j] = client.prepareIndex(indexName).setSource("foo", "bar");
        }
        indexRandom(true, documents);
        flushAndRefresh();

        createRepository("test-repo", "fs", repo);

        final String snapshot1 = "test-snap-1";
        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", snapshot1);
        assertThat(snapshotInfo.indices(), hasSize(1));

        final RepositoryData repositoryData = getRepositoryData("test-repo");
        final Map<String, IndexId> indexIds = repositoryData.getIndices();
        assertThat(indexIds.size(), equalTo(1));

        final IndexId corruptedIndex = indexIds.get(indexName);
        final Path shardIndexFile = repo.resolve("indices")
            .resolve(corruptedIndex.getId())
            .resolve("0")
            .resolve("index-" + repositoryData.shardGenerations().getShardGen(corruptedIndex, 0));

        logger.info("-->  truncating shard index file [{}]", shardIndexFile);
        try (SeekableByteChannel outChan = Files.newByteChannel(shardIndexFile, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }

        logger.info("-->  verifying snapshot state for [{}]", snapshot1);
        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).snapshotId().getName(), equalTo(snapshot1));

        logger.info("-->  deleting index [{}]", indexName);
        assertAcked(indicesAdmin().prepareDelete(indexName));

        logger.info("-->  restoring snapshot [{}]", snapshot1);
        clusterAdmin().prepareRestoreSnapshot("test-repo", snapshot1)
            .setRestoreGlobalState(randomBoolean())
            .setWaitForCompletion(true)
            .get();
        ensureGreen();

        assertDocCount(indexName, nDocs);

        logger.info("-->  indexing [{}] more documents into [{}]", nDocs, indexName);
        for (int j = 0; j < nDocs; j++) {
            documents[j] = client.prepareIndex(indexName).setSource("foo2", "bar2");
        }
        indexRandom(true, documents);

        final String snapshot2 = "test-snap-2";
        logger.info("-->  creating snapshot [{}]", snapshot2);
        final SnapshotInfo snapshotInfo2 = clusterAdmin().prepareCreateSnapshot("test-repo", snapshot2)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertThat(snapshotInfo2.state(), equalTo(SnapshotState.PARTIAL));
        assertThat(snapshotInfo2.failedShards(), equalTo(1));
        assertThat(snapshotInfo2.successfulShards(), equalTo(snapshotInfo.totalShards() - 1));
        assertThat(snapshotInfo2.indices(), hasSize(1));
    }

    public void testDeleteSnapshotWithMissingIndexAndShardMetadata() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        createRepository("test-repo", "fs", repo);

        final String[] indices = { "test-idx-1", "test-idx-2" };
        createIndex(indices);
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices(indices)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        final Map<String, IndexId> indexIds = getRepositoryData("test-repo").getIndices();
        final Path indicesPath = repo.resolve("indices");

        logger.info("--> delete index metadata and shard metadata");
        for (String index : indices) {
            Path shardZero = indicesPath.resolve(indexIds.get(index).getId()).resolve("0");
            if (randomBoolean()) {
                Files.delete(
                    shardZero.resolve("index-" + getRepositoryData("test-repo").shardGenerations().getShardGen(indexIds.get(index), 0))
                );
            }
            Files.delete(shardZero.resolve("snap-" + snapshotInfo.snapshotId().getUUID() + ".dat"));
        }

        startDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");

        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1").get()
        );

        for (String index : indices) {
            assertTrue(Files.notExists(indicesPath.resolve(indexIds.get(index).getId())));
        }
    }

    public void testDeleteSnapshotWithMissingMetadata() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        createRepository("test-repo", "fs", repo);

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> delete global state metadata");
        Path metadata = repo.resolve("meta-" + createSnapshotResponse.getSnapshotInfo().snapshotId().getUUID() + ".dat");
        Files.delete(metadata);

        startDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1").get()
        );
    }

    public void testDeleteSnapshotWithCorruptedSnapshotFile() throws Exception {
        Client client = client();

        Path repo = randomRepoPath();
        createRepository(
            "test-repo",
            "fs",
            Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        logger.info("--> creating snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> truncate snapshot file to make it unreadable");
        Path snapshotPath = repo.resolve("snap-" + createSnapshotResponse.getSnapshotInfo().snapshotId().getUUID() + ".dat");
        try (SeekableByteChannel outChan = Files.newByteChannel(snapshotPath, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }
        startDeleteSnapshot("test-repo", "test-snap-1").get();

        logger.info("--> make sure snapshot doesn't exist");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots("test-repo").addSnapshots("test-snap-1").get().getSnapshots()
        );

        logger.info("--> make sure that we can create the snapshot again");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );
    }

    /** Tests that a snapshot with a corrupted global state file can still be deleted */
    public void testDeleteSnapshotWithCorruptedGlobalState() throws Exception {
        final Path repo = randomRepoPath();

        createRepository(
            "test-repo",
            "fs",
            Settings.builder().put("location", repo).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );
        flushAndRefresh("test-idx-1", "test-idx-2");

        SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");

        final Path globalStatePath = repo.resolve("meta-" + snapshotInfo.snapshotId().getUUID() + ".dat");
        if (randomBoolean()) {
            // Delete the global state metadata file
            IOUtils.deleteFilesIgnoringExceptions(globalStatePath);
        } else {
            // Truncate the global state metadata file
            try (SeekableByteChannel outChan = Files.newByteChannel(globalStatePath, StandardOpenOption.WRITE)) {
                outChan.truncate(randomInt(10));
            }
        }

        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).snapshotId().getName(), equalTo("test-snap"));

        SnapshotsStatusResponse snapshotStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo").setSnapshots("test-snap").get();
        assertThat(snapshotStatusResponse.getSnapshots(), hasSize(1));
        assertThat(snapshotStatusResponse.getSnapshots().get(0).getSnapshot().getSnapshotId().getName(), equalTo("test-snap"));

        assertAcked(startDeleteSnapshot("test-repo", "test-snap").get());
        expectThrows(SnapshotMissingException.class, () -> clusterAdmin().prepareGetSnapshots("test-repo").addSnapshots("test-snap").get());
        assertRequestBuilderThrows(
            clusterAdmin().prepareSnapshotStatus("test-repo").addSnapshots("test-snap"),
            SnapshotMissingException.class
        );

        createFullSnapshot("test-repo", "test-snap");
    }

    public void testSnapshotWithMissingShardLevelIndexFile() throws Exception {
        disableRepoConsistencyCheck("This test uses a purposely broken repository so it would fail consistency checks");

        Path repo = randomRepoPath();
        createRepository("test-repo", "fs", repo);

        createIndex("test-idx-1", "test-idx-2");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );

        logger.info("--> creating snapshot");
        clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-1").setWaitForCompletion(true).setIndices("test-idx-*").get();

        logger.info("--> deleting shard level index file");
        final Path indicesPath = repo.resolve("indices");
        for (IndexId indexId : getRepositoryData("test-repo").getIndices().values()) {
            final Path shardGen;
            try (Stream<Path> shardFiles = Files.list(indicesPath.resolve(indexId.getId()).resolve("0"))) {
                shardGen = shardFiles.filter(file -> file.getFileName().toString().startsWith(BlobStoreRepository.INDEX_FILE_PREFIX))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Failed to find shard index blob"));
            }
            Files.delete(shardGen);
        }

        logger.info("--> creating another snapshot");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices("test-idx-1")
            .get();
        assertEquals(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            createSnapshotResponse.getSnapshotInfo().totalShards() - 1
        );

        logger.info(
            "--> restoring the first snapshot, the repository should not have lost any shard data despite deleting index-N, "
                + "because it uses snap-*.data files and not the index-N to determine what files to restore"
        );
        indicesAdmin().prepareDelete("test-idx-1", "test-idx-2").get();
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .get();
        assertEquals(0, restoreSnapshotResponse.getRestoreInfo().failedShards());
    }

    public void testDeletesWithUnexpectedIndexBlob() throws Exception {
        Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(repoName, FsRepository.TYPE, repo);
        final String snapshot = "first-snapshot";
        createFullSnapshot(repoName, snapshot);
        // create extra file with the index prefix
        final var extraFile = Files.createFile(repo.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + randomAlphaOfLength(5)));
        assertAcked(startDeleteSnapshot(repoName, snapshot).get());
        // delete file again so repo consistency checks pass
        Files.delete(extraFile);
    }

    private void assertRepositoryBlocked(String repo, String existingSnapshot) {
        if (getRepositoryMetadata(repo).generation() == RepositoryData.CORRUPTED_REPO_GEN) return;

        logger.info("--> try to delete snapshot");
        final RepositoryException ex = expectThrows(
            RepositoryException.class,
            () -> clusterAdmin().prepareDeleteSnapshot(repo, existingSnapshot).execute().actionGet()
        );
        assertThat(ex.getMessage(), containsString("concurrent modification of the index-N file"));

        logger.info("--> try to create snapshot");
        final RepositoryException ex2 = expectThrows(
            RepositoryException.class,
            () -> clusterAdmin().prepareCreateSnapshot(repo, existingSnapshot).execute().actionGet()
        );
        assertThat(ex2.getMessage(), containsString("The repository has been disabled to prevent data corruption"));

        logger.info("--> confirm corrupt flag in cluster state");
        assertEquals(RepositoryData.CORRUPTED_REPO_GEN, getRepositoryMetadata(repo).generation());
    }
}
