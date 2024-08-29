/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.RepositoryDataTests.generateRandomRepoData;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for the {@link BlobStoreRepository} and its subclasses.
 */
public class BlobStoreRepositoryTests extends ESSingleNodeTestCase {

    static final String REPO_TYPE = "fs";
    private static final String TEST_REPO_NAME = "test-repo";

    public void testRetrieveSnapshots() {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());

        logger.info("-->  creating repository");
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
            .setType(REPO_TYPE)
            .setSettings(Settings.builder().put(node().settings()).put("location", location))
            .get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        logger.info("--> creating an index and indexing documents");
        final String indexName = "test-idx";
        createIndex(indexName);
        ensureGreen();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            prepareIndex(indexName).setId(id).setSource("text", "sometext").get();
        }
        indicesAdmin().prepareFlush(indexName).get();

        logger.info("--> create first snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        final SnapshotId snapshotId1 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> create second snapshot");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, TEST_REPO_NAME, "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        final SnapshotId snapshotId2 = createSnapshotResponse.getSnapshotInfo().snapshotId();

        logger.info("--> make sure the node's repository can resolve the snapshots");
        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(TEST_REPO_NAME);
        final List<SnapshotId> originalSnapshots = Arrays.asList(snapshotId1, snapshotId2);

        List<SnapshotId> snapshotIds = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository)
            .getSnapshotIds()
            .stream()
            .sorted((s1, s2) -> s1.getName().compareTo(s2.getName()))
            .toList();
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    public void testReadAndWriteSnapshotsThroughIndexFile() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        final long pendingGeneration = repository.metadata.pendingGeneration();
        // write to and read from a index file with no entries
        assertThat(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getSnapshotIds().size(), equalTo(0));
        final RepositoryData emptyData = RepositoryData.EMPTY;
        writeIndexGen(repository, emptyData, emptyData.getGenId());
        RepositoryData repoData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        assertEquals(repoData, emptyData);
        assertEquals(repoData.getIndices().size(), 0);
        assertEquals(repoData.getSnapshotIds().size(), 0);
        assertEquals(pendingGeneration + 1L, repoData.getGenId());

        // write to and read from an index file with snapshots but no indices
        repoData = addRandomSnapshotsToRepoData(repoData, false);
        writeIndexGen(repository, repoData, repoData.getGenId());
        assertEquals(repoData, ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository));

        // write to and read from a index file with random repository data
        repoData = addRandomSnapshotsToRepoData(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), true);
        writeIndexGen(repository, repoData, repoData.getGenId());
        assertEquals(repoData, ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository));
    }

    public void testIndexGenerationalFiles() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        assertEquals(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), RepositoryData.EMPTY);

        final long pendingGeneration = repository.metadata.pendingGeneration();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        writeIndexGen(repository, repositoryData, RepositoryData.EMPTY_REPO_GEN);
        assertThat(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), equalTo(repositoryData));
        final long expectedGeneration = pendingGeneration + 1L;
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration));

        // adding more and writing to a new index generational file
        repositoryData = addRandomSnapshotsToRepoData(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), true);
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertEquals(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration + 1L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration + 1L));

        // removing a snapshot and writing to a new index generational file
        repositoryData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository)
            .removeSnapshots(Collections.singleton(repositoryData.getSnapshotIds().iterator().next()), ShardGenerations.EMPTY);
        writeIndexGen(repository, repositoryData, repositoryData.getGenId());
        assertEquals(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), repositoryData);
        assertThat(repository.latestIndexBlobId(), equalTo(expectedGeneration + 2L));
        assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(expectedGeneration + 2L));
    }

    public void testCorruptIndexLatestFile() throws Exception {
        final BlobStoreRepository repository = setupRepo();

        final long generation = randomLong();
        final byte[] generationBytes = Numbers.longToBytes(generation);

        final byte[] buffer = new byte[16];
        System.arraycopy(generationBytes, 0, buffer, 0, 8);

        for (int i = 0; i < 16; i++) {
            repository.blobContainer()
                .writeBlob(OperationPurpose.SNAPSHOT_METADATA, BlobStoreRepository.INDEX_LATEST_BLOB, new BytesArray(buffer, 0, i), false);
            if (i == 8) {
                assertThat(repository.readSnapshotIndexLatestBlob(), equalTo(generation));
            } else {
                assertThat(
                    expectThrows(RepositoryException.class, repository::readSnapshotIndexLatestBlob).getMessage(),
                    allOf(
                        containsString("exception reading blob [index.latest]: expected 8 bytes"),
                        i < 8 ? containsString("blob was " + i + " bytes") : containsString("blob was longer")
                    )
                );
            }
        }
    }

    public void testRepositoryDataConcurrentModificationNotAllowed() {
        final BlobStoreRepository repository = setupRepo();

        // write to index generational file
        RepositoryData repositoryData = generateRandomRepoData();
        final long startingGeneration = repositoryData.getGenId();
        writeIndexGen(repository, repositoryData, startingGeneration);

        // write repo data again to index generational file, errors because we already wrote to the
        // N+1 generation from which this repository data instance was created
        final RepositoryData fresherRepositoryData = repositoryData.withGenId(startingGeneration + 1);

        assertThat(
            safeAwaitFailure(
                RepositoryData.class,
                listener -> repository.writeIndexGen(
                    fresherRepositoryData,
                    repositoryData.getGenId(),
                    IndexVersion.current(),
                    Function.identity(),
                    listener
                )
            ),
            instanceOf(RepositoryException.class)
        );
    }

    public void testBadChunksize() {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());

        expectThrows(
            RepositoryException.class,
            () -> client.admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .setType(REPO_TYPE)
                .setSettings(
                    Settings.builder()
                        .put(node().settings())
                        .put("location", location)
                        .put("chunk_size", randomLongBetween(-10, 0), ByteSizeUnit.BYTES)
                )
                .get()
        );
    }

    public void testRepositoryDataDetails() throws Exception {
        final BlobStoreRepository repository = setupRepo();
        final String repositoryName = repository.getMetadata().name();

        createIndex("green-index");
        ensureGreen("green-index");

        assertAcked(
            indicesAdmin().prepareCreate("red-index")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), "*")
                        .build()
                )
                .setWaitForActiveShards(0)
        );

        final long beforeStartTime = getInstanceFromNode(ThreadPool.class).absoluteTimeInMillis();
        final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(
            TEST_REQUEST_TIMEOUT,
            repositoryName,
            "test-snap-1"
        ).setWaitForCompletion(true).setPartial(true).get();
        final long afterEndTime = System.currentTimeMillis();

        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo(SnapshotState.PARTIAL));
        final SnapshotId snapshotId = createSnapshotResponse.getSnapshotInfo().snapshotId();

        final Consumer<RepositoryData.SnapshotDetails> snapshotDetailsAsserter = snapshotDetails -> {
            assertThat(snapshotDetails.getSnapshotState(), equalTo(SnapshotState.PARTIAL));
            assertThat(snapshotDetails.getVersion(), equalTo(IndexVersion.current()));
            assertThat(snapshotDetails.getStartTimeMillis(), allOf(greaterThanOrEqualTo(beforeStartTime), lessThanOrEqualTo(afterEndTime)));
            assertThat(
                snapshotDetails.getEndTimeMillis(),
                allOf(
                    greaterThanOrEqualTo(beforeStartTime),
                    lessThanOrEqualTo(afterEndTime),
                    greaterThanOrEqualTo(snapshotDetails.getStartTimeMillis())
                )
            );
        };

        final RepositoryData repositoryData = AbstractSnapshotIntegTestCase.getRepositoryData(repository);
        final RepositoryData.SnapshotDetails snapshotDetails = repositoryData.getSnapshotDetails(snapshotId);
        snapshotDetailsAsserter.accept(snapshotDetails);

        // now check the handling of the case where details are missing, by removing the details from the repo data as if from an
        // older repo format and verifing that they are refreshed from the SnapshotInfo when writing the repo data out

        writeIndexGen(
            repository,
            repositoryData.withExtraDetails(
                Collections.singletonMap(
                    snapshotId,
                    new RepositoryData.SnapshotDetails(SnapshotState.PARTIAL, IndexVersion.current(), -1, -1, null)
                )
            ),
            repositoryData.getGenId()
        );

        snapshotDetailsAsserter.accept(AbstractSnapshotIntegTestCase.getRepositoryData(repository).getSnapshotDetails(snapshotId));
    }

    private static void writeIndexGen(BlobStoreRepository repository, RepositoryData repositoryData, long generation) {
        safeAwait(
            (ActionListener<RepositoryData> listener) -> repository.writeIndexGen(
                repositoryData,
                generation,
                IndexVersion.current(),
                Function.identity(),
                listener
            )
        );
    }

    private BlobStoreRepository setupRepo() {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());

        Settings.Builder repoSettings = Settings.builder().put(node().settings()).put("location", location);
        boolean compress = randomBoolean();
        if (compress == false) {
            repoSettings.put(BlobStoreRepository.COMPRESS_SETTING.getKey(), false);
        }
        AcknowledgedResponse putRepositoryResponse = client.admin()
            .cluster()
            .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
            .setType(REPO_TYPE)
            .setSettings(repoSettings)
            .setVerify(false) // prevent eager reading of repo data
            .get(TimeValue.timeValueSeconds(10));
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));

        final RepositoriesService repositoriesService = getInstanceFromNode(RepositoriesService.class);
        final BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(TEST_REPO_NAME);
        assertThat("getBlobContainer has to be lazy initialized", repository.getBlobContainer(), nullValue());
        assertEquals("Compress must be set to", compress, repository.isCompress());
        return repository;
    }

    @After
    public void removeRepo() {
        try {
            client().admin()
                .cluster()
                .prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, TEST_REPO_NAME)
                .get(TimeValue.timeValueSeconds(10));
        } catch (RepositoryMissingException e) {
            // ok, not all tests create the test repo
        }
    }

    private RepositoryData addRandomSnapshotsToRepoData(RepositoryData repoData, boolean inclIndices) {
        int numSnapshots = randomIntBetween(1, 20);
        for (int i = 0; i < numSnapshots; i++) {
            SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(8), UUIDs.randomBase64UUID());
            int numIndices = inclIndices ? randomIntBetween(0, 20) : 0;
            final ShardGenerations.Builder builder = ShardGenerations.builder();
            for (int j = 0; j < numIndices; j++) {
                builder.put(new IndexId(randomAlphaOfLength(8), UUIDs.randomBase64UUID()), 0, new ShardGeneration(1L));
            }
            final ShardGenerations shardGenerations = builder.build();
            final Map<IndexId, String> indexLookup = shardGenerations.indices()
                .stream()
                .collect(Collectors.toMap(Function.identity(), ind -> randomAlphaOfLength(256)));
            final RepositoryData.SnapshotDetails details = new RepositoryData.SnapshotDetails(
                randomFrom(SnapshotState.SUCCESS, SnapshotState.PARTIAL, SnapshotState.FAILED),
                IndexVersion.current(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomAlphaOfLength(10)
            );
            repoData = repoData.addSnapshot(
                snapshotId,
                details,
                shardGenerations,
                indexLookup,
                indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random())))
            );
        }
        return repoData;
    }

    public void testEnsureUploadListenerIsResolvedWhenAFileSnapshotTaskFails() throws Exception {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        final FsRepository repository = new FsRepository(
            repositoryMetadata,
            createEnvironment(),
            xContentRegistry(),
            clusterService,
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        ) {
            @Override
            protected void snapshotFile(SnapshotShardContext context, BlobStoreIndexShardSnapshot.FileInfo fileInfo) throws IOException {
                // Randomly fail some file snapshot tasks
                if (randomBoolean()) {
                    throw new IOException("cannot upload file");
                }
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        // Generate some FileInfo, as the files that get uploaded as part of the shard snapshot
        SnapshotShardContext context = ShardSnapshotTaskRunnerTests.dummyContext();
        int noOfFiles = randomIntBetween(10, 100);
        BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files = new LinkedBlockingQueue<>(noOfFiles);
        PlainActionFuture<Void> listenerCalled = new PlainActionFuture<>();
        ActionListener<Collection<Void>> allFilesUploadListener = ActionListener.running(() -> listenerCalled.onResponse(null));
        for (int i = 0; i < noOfFiles; i++) {
            files.add(ShardSnapshotTaskRunnerTests.dummyFileInfo());
        }
        repository.snapshotFiles(context, files, allFilesUploadListener);
        listenerCalled.get(10, TimeUnit.SECONDS);
    }

    public void testGetRepositoryDataThreadContext() {
        final var future = new PlainActionFuture<Void>();
        try (var listeners = new RefCountingListener(future)) {
            final var repo = setupRepo();
            final int threads = between(1, 5);
            final var barrier = new CyclicBarrier(threads);
            final var headerName = "test-header";
            final var threadPool = client().threadPool();
            final var threadContext = threadPool.getThreadContext();
            for (int i = 0; i < threads; i++) {
                final var headerValue = randomAlphaOfLength(10);
                try (var ignored = threadContext.stashContext()) {
                    threadContext.putHeader(headerName, headerValue);
                    threadPool.generic().execute(ActionRunnable.wrap(listeners.acquire(), l -> {
                        safeAwait(barrier);
                        repo.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, l.map(repositoryData -> {
                            assertEquals(headerValue, threadContext.getHeader(headerName));
                            return null;
                        }));
                    }));
                }
            }
        }
        future.actionGet(10, TimeUnit.SECONDS);
    }

    public void testGetRepositoryDataForking() {
        final var forkedListeners = Collections.synchronizedList(new ArrayList<Runnable>());
        final var future = new PlainActionFuture<Void>();
        try (var listeners = new RefCountingListener(future)) {
            final var repo = setupRepo();
            final int threads = between(1, 5);
            final var barrier = new CyclicBarrier(threads);
            final var threadPool = client().threadPool();
            final var testThread = Thread.currentThread();
            final var resultsCountDown = new CountDownLatch(threads);
            for (int i = 0; i < threads; i++) {
                threadPool.generic().execute(ActionRunnable.wrap(listeners.acquire(), l -> {
                    final var callingThread = Thread.currentThread();
                    safeAwait(barrier);
                    repo.getRepositoryData(runnable -> {
                        forkedListeners.add(runnable);
                        resultsCountDown.countDown();
                    }, l.map(repositoryData -> {
                        final var currentThread = Thread.currentThread();
                        if (currentThread == testThread) {
                            assertEquals(0, resultsCountDown.getCount());
                        } else {
                            assertSame(callingThread, currentThread);
                            resultsCountDown.countDown();
                        }
                        return null;
                    }));
                }));
            }
            safeAwait(resultsCountDown);
            forkedListeners.forEach(Runnable::run);
            repo.getRepositoryData(runnable -> fail("should use cached value and not fork"), listeners.acquire(ignored -> {}));
        }
        future.actionGet(10, TimeUnit.SECONDS);
    }

    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
                .build()
        );
    }

    public void testShardBlobsToDelete() {
        final var repo = setupRepo();
        try (var shardBlobsToDelete = repo.new ShardBlobsToDelete()) {
            final var expectedShardGenerations = ShardGenerations.builder();
            final var expectedBlobsToDelete = new HashSet<String>();

            final var countDownLatch = new CountDownLatch(1);
            int blobCount = 0;
            try (var refs = new RefCountingRunnable(countDownLatch::countDown)) {
                for (int index = between(0, 1000); index > 0; index--) {
                    final var indexId = new IndexId(randomIdentifier(), randomUUID());
                    for (int shard = between(1, 30); shard > 0; shard--) {
                        final var shardId = shard;
                        final var shardGeneration = new ShardGeneration(randomUUID());
                        expectedShardGenerations.put(indexId, shard, shardGeneration);
                        final var blobsToDelete = randomList(
                            100,
                            () -> randomFrom("meta-", "index-", "snap-") + randomUUID() + randomFrom("", ".dat")
                        );
                        blobCount += blobsToDelete.size();
                        final var indexPath = repo.basePath()
                            .add("indices")
                            .add(indexId.getId())
                            .add(Integer.toString(shard))
                            .buildAsString();
                        for (final var blobToDelete : blobsToDelete) {
                            expectedBlobsToDelete.add(indexPath + blobToDelete);
                        }

                        repo.threadPool()
                            .generic()
                            .execute(
                                ActionRunnable.run(
                                    refs.acquireListener(),
                                    () -> shardBlobsToDelete.addShardDeleteResult(indexId, shardId, shardGeneration, blobsToDelete)
                                )
                            );
                    }
                }
            }
            safeAwait(countDownLatch);
            assertEquals(expectedShardGenerations.build(), shardBlobsToDelete.getUpdatedShardGenerations());
            shardBlobsToDelete.getBlobPaths().forEachRemaining(s -> assertTrue(expectedBlobsToDelete.remove(s)));
            assertThat(expectedBlobsToDelete, empty());
            assertThat(shardBlobsToDelete.sizeInBytes(), lessThanOrEqualTo(Math.max(ByteSizeUnit.KB.toIntBytes(1), 20 * blobCount)));
        }
    }

    public void testUuidCreationLogging() {
        final var repo = setupRepo();
        final var repoMetadata = repo.getMetadata();
        final var repoName = repoMetadata.name();
        final var snapshot = randomIdentifier();

        MockLog.assertThatLogger(
            () -> safeGet(
                client().execute(
                    TransportCreateSnapshotAction.TYPE,
                    new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, snapshot).waitForCompletion(true)
                )
            ),
            BlobStoreRepository.class,
            new MockLog.SeenEventExpectation(
                "new repo uuid message",
                BlobStoreRepository.class.getCanonicalName(),
                Level.INFO,
                Strings.format("Generated new repository UUID [*] for repository [%s] in generation [*]", repoName)
            )
        );

        MockLog.assertThatLogger(
            // no more "Generated" messages ...
            () -> {
                safeGet(
                    client().execute(
                        TransportDeleteRepositoryAction.TYPE,
                        new DeleteRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                    )
                );

                // we get a "Registering" message when re-registering the repository with ?verify=true (the default)
                MockLog.assertThatLogger(
                    () -> safeGet(
                        client().execute(
                            TransportPutRepositoryAction.TYPE,
                            new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).type("fs")
                                .verify(true)
                                .settings(repoMetadata.settings())
                        )
                    ),
                    RepositoriesService.class,
                    new MockLog.SeenEventExpectation(
                        "existing repo uuid message",
                        RepositoriesService.class.getCanonicalName(),
                        Level.INFO,
                        Strings.format("Registering repository [%s] with repository UUID *", repoName)
                    )
                );

                safeGet(
                    client().execute(
                        TransportCreateSnapshotAction.TYPE,
                        new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, randomIdentifier()).waitForCompletion(true)
                    )
                );
                assertTrue(
                    safeGet(client().execute(TransportGetSnapshotsAction.TYPE, new GetSnapshotsRequest(TEST_REQUEST_TIMEOUT, repoName)))
                        .getSnapshots()
                        .stream()
                        .anyMatch(snapshotInfo -> snapshotInfo.snapshotId().getName().equals(snapshot))
                );

                safeGet(
                    client().execute(
                        TransportDeleteRepositoryAction.TYPE,
                        new DeleteRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName)
                    )
                );

                // No "Registering" message with ?verify=false because we don't read the repo data yet
                MockLog.assertThatLogger(
                    () -> safeGet(
                        client().execute(
                            TransportPutRepositoryAction.TYPE,
                            new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).type("fs")
                                .verify(false)
                                .settings(repoMetadata.settings())
                        )
                    ),
                    RepositoriesService.class,
                    new MockLog.UnseenEventExpectation(
                        "existing repo uuid message",
                        RepositoriesService.class.getCanonicalName(),
                        Level.INFO,
                        "Registering repository*"
                    )
                );

                // But we do get the "Registering" message the first time we read the repo
                MockLog.assertThatLogger(
                    () -> safeGet(
                        client().execute(
                            TransportCreateSnapshotAction.TYPE,
                            new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, randomIdentifier()).waitForCompletion(true)
                        )
                    ),
                    RepositoriesService.class,
                    new MockLog.SeenEventExpectation(
                        "existing repo uuid message",
                        RepositoriesService.class.getCanonicalName(),
                        Level.INFO,
                        Strings.format("Registering repository [%s] with repository UUID *", repoName)
                    )
                );
            },
            BlobStoreRepository.class,
            new MockLog.UnseenEventExpectation(
                "no repo uuid generated messages",
                BlobStoreRepository.class.getCanonicalName(),
                Level.INFO,
                "Generated new repository UUID*"
            )
        );
    }
}
