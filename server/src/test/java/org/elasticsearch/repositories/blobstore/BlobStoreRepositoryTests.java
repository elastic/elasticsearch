/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
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
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamOutput;
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
import org.elasticsearch.repositories.FinalizeSnapshotContext.UpdatedShardGenerations;
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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.RepositoryDataTests.generateRandomRepoData;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_FILE_PREFIX;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.METADATA_BLOB_NAME_SUFFIX;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.METADATA_PREFIX;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_PREFIX;
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
            .sorted(Comparator.comparing(SnapshotId::getName))
            .toList();
        assertThat(snapshotIds, equalTo(originalSnapshots));
    }

    public void testReadAndWriteSnapshotsThroughIndexFile() {
        final BlobStoreRepository repository = setupRepo();
        final long pendingGeneration = repository.metadata.pendingGeneration();
        // write to and read from a index file with no entries
        assertThat(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository).getSnapshotIds().size(), equalTo(0));
        final RepositoryData emptyData = RepositoryData.EMPTY;
        writeIndexGen(repository, emptyData, emptyData.getGenId());
        RepositoryData repoData = ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository);
        assertEquals(repoData, emptyData);
        assertEquals(0, repoData.getIndices().size());
        assertEquals(0, repoData.getSnapshotIds().size());
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
        assertEquals(RepositoryData.EMPTY, ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository));

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

        // adding more snapshots to check that writeIndexGen checks the actual repo metadata and fails on a readonly repository
        final var newRepositoryData = addRandomSnapshotsToRepoData(ESBlobStoreRepositoryIntegTestCase.getRepositoryData(repository), true);
        final var barrier = new CyclicBarrier(2);
        final var setReadonlyPromise = blockAndSetRepositoryReadOnly(repository.getMetadata().name(), barrier);
        safeAwait(barrier); // wait for set-readonly task to start executing
        safeAwait(l -> {
            repository.writeIndexGen(
                newRepositoryData,
                newRepositoryData.getGenId(),
                IndexVersion.current(),
                Function.identity(),
                ActionTestUtils.assertNoSuccessListener(e -> {
                    assertThat(
                        asInstanceOf(RepositoryException.class, e).getMessage(),
                        containsString("[test-repo] Failed to execute cluster state update [set pending repository generation")
                    );
                    assertThat(
                        asInstanceOf(RepositoryException.class, e.getCause()).getMessage(),
                        containsString("[test-repo] repository is readonly, cannot update root blob")
                    );
                    l.onResponse(null);
                })
            );
            safeAwait(barrier); // allow set-readonly task to proceed now that the set-pending-generation task is enqueued
        });
        safeAwait(setReadonlyPromise);
    }

    /**
     * Submits a cluster state update which blocks on the {@code barrier} twice and then marks the given repository as readonly.
     * @return a promise that is completed when the cluster state update finishes.
     */
    private SubscribableListener<Void> blockAndSetRepositoryReadOnly(String repositoryName, CyclicBarrier barrier) {
        return SubscribableListener.newForked(
            l -> getInstanceFromNode(ClusterService.class).submitUnbatchedStateUpdateTask(
                "update readonly flag",
                new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        safeAwait(barrier);
                        safeAwait(barrier);
                        return new ClusterState.Builder(currentState).metadata(
                            Metadata.builder(currentState.metadata())
                                .putCustom(
                                    RepositoriesMetadata.TYPE,
                                    new RepositoriesMetadata(
                                        RepositoriesMetadata.get(currentState)
                                            .repositories()
                                            .stream()
                                            .map(
                                                r -> r.name().equals(repositoryName)
                                                    ? new RepositoryMetadata(
                                                        r.name(),
                                                        r.uuid(),
                                                        r.type(),
                                                        Settings.builder()
                                                            .put(r.settings())
                                                            .put(BlobStoreRepository.READONLY_SETTING_KEY, "true")
                                                            .build(),
                                                        r.generation(),
                                                        r.pendingGeneration()
                                                    )
                                                    : r
                                            )
                                            .toList()
                                    )
                                )
                        ).build();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        l.onFailure(e);
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                        l.onResponse(null);
                    }
                }
            )
        );
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

    public void testRepositoryDataDetails() {
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
        return setupRepo(Settings.builder());
    }

    private BlobStoreRepository setupRepo(Settings.Builder repoSettings) {
        final Client client = client();
        final Path location = ESIntegTestCase.randomRepoPath(node().settings());
        repoSettings.put(node().settings()).put("location", location);
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
                new UpdatedShardGenerations(shardGenerations, ShardGenerations.EMPTY),
                indexLookup,
                indexLookup.values().stream().collect(Collectors.toMap(Function.identity(), ignored -> UUIDs.randomBase64UUID(random())))
            );
        }
        return repoData;
    }

    public void testEnsureUploadListenerIsResolvedWhenAFileSnapshotTaskFails() throws Exception {
        final ProjectId projectId = randomProjectIdOrDefault();
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(projectId, repositoryMetadata);
        final FsRepository repository = new FsRepository(
            projectId,
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
                Strings.format("Generated new repository UUID [*] for repository %s in generation [*]", repo.toStringShort())
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
                        Strings.format("Registering repository %s with repository UUID *", repo.toStringShort())
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
                        Strings.format("Registering repository %s with repository UUID *", repo.toStringShort())
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

    // =============== Shard Blobs to Delete Tests =================

    /*
        There is sufficient heap space to perform all operations on shardBlobsToDelete
     */
    @TestLogging(reason = "test includes assertions about logging", value = "org.elasticsearch.repositories.blobstore:WARN")
    public void testShardBlobsToDeleteWithSufficientHeapSpace() {
        // When the heap memory is above 2GB (Integer.MAX_VALUE) then we expect it to be limited to 2GB
        long heapMemory = randomLongBetween(Integer.MAX_VALUE / 2, Integer.MAX_VALUE * 2L);
        Settings.Builder settings = Settings.builder().put("repositories.blobstore.max_shard_delete_results_size", heapMemory + "b");
        final var repo = setupRepo(settings);
        try (var shardBlobsToDelete = repo.new ShardBlobsToDelete(settings.build())) {
            // Ensure the logging is as expected
            try (var mockLog = MockLog.capture(BlobStoreRepository.class)) {
                // We expect every write to succeed
                addWarnLogCountExpectation(mockLog, 0);

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
                            final var blobsToDelete = generateRandomBlobsToDelete();

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
                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    /*
        There is limited heap space for N-1 writes, but will run out for the Nth write
     */
    @TestLogging(reason = "test includes assertions about logging", value = "org.elasticsearch.repositories.blobstore:WARN")
    public void testShardBlobsToDeleteWithLimitedHeapSpace() {
        final IndexId indexId = new IndexId(randomIdentifier(), randomUUID());
        final int shardId = between(1, 30);
        final List<String> blobsToDelete = generateRandomBlobsToDelete();

        final ShardGeneration shardGeneration = new ShardGeneration(randomUUID());
        final var expectedShardGenerations = ShardGenerations.builder().put(indexId, shardId, shardGeneration).build();

        int bytesToWriteIndexId = StreamOutput.bytesInString(indexId.getId());
        int bytesToWriteShardId = StreamOutput.bytesInVInt(shardId);
        int bytesToWriteBlobsToDelete = StreamOutput.bytesInStringCollection(blobsToDelete);
        int totalBytesRequired = bytesToWriteIndexId + bytesToWriteShardId + bytesToWriteBlobsToDelete;

        Settings.Builder settings = Settings.builder().put("repositories.blobstore.max_shard_delete_results_size", totalBytesRequired + "b");
        final var repo = setupRepo(settings);
        try (var shardBlobsToDelete = repo.new ShardBlobsToDelete(settings.build())) {

            // The first time we expect the write to succeed
            try (var mockLog = MockLog.capture(BlobStoreRepository.class)) {
                addWarnLogCountExpectation(mockLog, 0);

                final var expectedBlobsToDelete = new HashSet<String>();
                final var indexPath = repo.basePath()
                    .add("indices")
                    .add(indexId.getId())
                    .add(Integer.toString(shardId))
                    .buildAsString();
                for (final var blobToDelete : blobsToDelete) {
                    expectedBlobsToDelete.add(indexPath + blobToDelete);
                }

                final var countDownLatch = new CountDownLatch(1);
                try (var refs = new RefCountingRunnable(countDownLatch::countDown)) {
                        repo.threadPool()
                            .generic()
                            .execute(
                                ActionRunnable.run(
                                    refs.acquireListener(),
                                    () -> shardBlobsToDelete.addShardDeleteResult(indexId, shardId, shardGeneration, blobsToDelete)
                                )
                            );
                }
                safeAwait(countDownLatch);
                assertEquals(expectedShardGenerations, shardBlobsToDelete.getUpdatedShardGenerations());
                shardBlobsToDelete.getBlobPaths().forEachRemaining(s -> assertTrue(expectedBlobsToDelete.remove(s)));
                assertThat(expectedBlobsToDelete, empty());
                mockLog.assertAllExpectationsMatched();
            }

            // The second time we expect the write to fail
            try (var mockLog = MockLog.capture(BlobStoreRepository.class)) {
                addWarnLogCountExpectation(mockLog, 1);

                final var expectedBlobsToDelete = new HashSet<String>();
                final var indexPath = repo.basePath()
                    .add("indices")
                    .add(indexId.getId())
                    .add(Integer.toString(shardId))
                    .buildAsString();
                for (final var blobToDelete : blobsToDelete) {
                    expectedBlobsToDelete.add(indexPath + blobToDelete);
                }

                final var countDownLatch = new CountDownLatch(1);
                try (var refs = new RefCountingRunnable(countDownLatch::countDown)) {
                    repo.threadPool()
                        .generic()
                        .execute(
                            ActionRunnable.run(
                                refs.acquireListener(),
                                () -> shardBlobsToDelete.addShardDeleteResult(indexId, shardId, shardGeneration, blobsToDelete)
                            )
                        );
                }
                safeAwait(countDownLatch);
                assertEquals(expectedShardGenerations, shardBlobsToDelete.getUpdatedShardGenerations());

                int expectedBlobsToDeleteSizeBeforeRemoving = expectedBlobsToDelete.size();
                var y = shardBlobsToDelete.getBlobPaths();
                shardBlobsToDelete.getBlobPaths().forEachRemaining(s -> assertTrue(expectedBlobsToDelete.remove(s)));
                assertEquals(expectedBlobsToDelete.size(), expectedBlobsToDeleteSizeBeforeRemoving);

                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    /*
        There is insufficient / no heap space, so no writes to shardDeletesResults will succeed
     */
    @TestLogging(reason = "test includes assertions about logging", value = "org.elasticsearch.repositories.blobstore:WARN")
    public void testShardBlobsToDeleteWithOutHeapSpace() {
        boolean noHeap = randomBoolean();
        Settings.Builder settings;
        if (noHeap) {
            // We have no heap for some reason
            settings = Settings.builder().put("repositories.blobstore.max_shard_delete_results_size", "0b");
        } else {
            // Set the heap stupidly low so that it fails
            settings = Settings.builder().put("repositories.blobstore.max_shard_delete_results_size", "1b");
        }

        final var repo = setupRepo(settings);
        try (var shardBlobsToDelete = repo.new ShardBlobsToDelete(settings.build())) {
            // Ensure the logging is as expected
            try (var mockLog = MockLog.capture(BlobStoreRepository.class)) {
                final var expectedShardGenerations = ShardGenerations.builder();
                final var expectedBlobsToDelete = new HashSet<String>();
                final var countDownLatch = new CountDownLatch(1);

                int indexCount = between(0, 1000);
                List<Integer> shardCounts = new ArrayList<>();
                int count = 0;
                for (int i = 0; i < indexCount; i++) {
                    int shardCount = between(1, 30);
                    shardCounts.add(shardCount);
                    count += shardCount;
                }

                if (noHeap) {
                    // If there is no heap we don't even attempt to write
                    addWarnLogCountExpectation(mockLog, 0);
                } else {
                    // We expect every write to fail
                    addWarnLogCountExpectation(mockLog, count);
                }

                try (var refs = new RefCountingRunnable(countDownLatch::countDown)) {
                    for (int index = 0; index < indexCount; index++) {
                        final var indexId = new IndexId(randomIdentifier(), randomUUID());
                        for (int shard = 0; shard < shardCounts.get(index); shard++) {
                            final var shardId = shard;
                            final var shardGeneration = new ShardGeneration(randomUUID());
                            expectedShardGenerations.put(indexId, shard, shardGeneration);
                            final var blobsToDelete = generateRandomBlobsToDelete();

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

                /*
                    If there is no heap space, then getBlobPaths() returns an empty iterator
                    If there is heap space, but it was too small for all write operations to succeed,
                        only the blob paths of the successful writes will be return
                 */
                int expectedBlobsToDeleteSizeBeforeRemoving = expectedBlobsToDelete.size();
                shardBlobsToDelete.getBlobPaths().forEachRemaining(s -> assertTrue(expectedBlobsToDelete.remove(s)));
                assertEquals(expectedBlobsToDelete.size(), expectedBlobsToDeleteSizeBeforeRemoving);

                mockLog.assertAllExpectationsMatched();
            }
        }
    }

    private List<String> generateRandomBlobsToDelete() {
        return randomList(
            100,
            () -> randomFrom(METADATA_PREFIX, INDEX_FILE_PREFIX, SNAPSHOT_PREFIX) + randomUUID() + randomFrom(
                "",
                METADATA_BLOB_NAME_SUFFIX
            )
        );
    }

    private void addWarnLogCountExpectation(MockLog mockLog, int expectedWarnLogsThrown) {
        mockLog.addExpectation(new MockLog.LoggingExpectation() {
            int count = 0;

            @Override
            public void match(LogEvent event) {
                if (event.getLevel() != Level.WARN) {
                    return;
                }
                if (event.getLoggerName().equals(BlobStoreRepository.class.getCanonicalName()) == false) {
                    return;
                }

                Pattern pattern = Pattern.compile("Failure to clean up the following dangling blobs");
                Matcher matcher = pattern.matcher(event.getMessage().getFormattedMessage());

                if (matcher.find()) {
                    count++;
                }
            }

            @Override
            public void assertMatched() {
                assertEquals(count, expectedWarnLogsThrown);
            }
        });
    }
}
