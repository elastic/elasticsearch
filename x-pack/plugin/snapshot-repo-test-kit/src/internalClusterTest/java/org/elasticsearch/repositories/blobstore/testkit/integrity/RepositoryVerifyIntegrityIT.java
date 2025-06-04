/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshotsIntegritySuppressor;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreCorruptionUtils;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.RepositoryFileType;
import org.elasticsearch.repositories.blobstore.testkit.SnapshotRepositoryTestKit;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOTS_FORMAT;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.SNAPSHOT_FORMAT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class RepositoryVerifyIntegrityIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            SnapshotRepositoryTestKit.class,
            MockTransportService.TestPlugin.class
        );
    }

    private static long getCurrentTime(Function<LongStream, OptionalLong> summarizer) {
        return summarizer.apply(
            StreamSupport.stream(internalCluster().getInstances(ThreadPool.class).spliterator(), false)
                .mapToLong(ThreadPool::absoluteTimeInMillis)
        ).orElseThrow(AssertionError::new);
    }

    public void testSuccess() throws IOException {
        final var minStartTimeMillis = getCurrentTime(LongStream::min);
        final var testContext = createTestContext();
        final var request = testContext.getVerifyIntegrityRequest();
        if (randomBoolean()) {
            request.addParameter("verify_blob_contents", null);
        }
        final var response = getRestClient().performRequest(request);
        final var maxEndTimeMillis = getCurrentTime(LongStream::max);
        assertEquals(200, response.getStatusLine().getStatusCode());
        final var responseObjectPath = ObjectPath.createFromResponse(response);
        final var logEntryCount = responseObjectPath.evaluateArraySize("log");
        final var seenSnapshotNames = new HashSet<String>();
        final var seenIndexNames = new HashSet<String>();
        for (int i = 0; i < logEntryCount; i++) {
            assertThat(
                responseObjectPath.evaluate("log." + i + ".timestamp_in_millis"),
                allOf(greaterThanOrEqualTo(minStartTimeMillis), lessThanOrEqualTo(maxEndTimeMillis))
            );
            assertThat(
                responseObjectPath.evaluate("log." + i + ".timestamp"),
                request.getParameters().containsKey("human") ? instanceOf(String.class) : nullValue()
            );
            final String maybeSnapshotName = responseObjectPath.evaluate("log." + i + ".snapshot.snapshot");
            if (maybeSnapshotName != null) {
                assertTrue(seenSnapshotNames.add(maybeSnapshotName));
            } else {
                final String indexName = responseObjectPath.evaluate("log." + i + ".index.name");
                assertNotNull(indexName);
                assertTrue(seenIndexNames.add(indexName));
                assertEquals(
                    testContext.snapshotNames().size(),
                    (int) responseObjectPath.evaluate("log." + i + ".snapshot_restorability.total_snapshot_count")
                );
                assertEquals(
                    testContext.snapshotNames().size(),
                    (int) responseObjectPath.evaluate("log." + i + ".snapshot_restorability.restorable_snapshot_count")
                );
            }
        }
        assertEquals(Set.copyOf(testContext.snapshotNames()), seenSnapshotNames);
        assertEquals(Set.copyOf(testContext.indexNames()), seenIndexNames);

        assertEquals(0, (int) responseObjectPath.evaluate("results.total_anomalies"));
        assertEquals("pass", responseObjectPath.evaluate("results.result"));
    }

    public void testTaskStatus() throws IOException {
        final var testContext = createTestContext();

        // use non-master node to coordinate the request so that we can capture chunks being sent back
        final var coordNodeName = getCoordinatingNodeName();
        final var coordNodeTransportService = MockTransportService.getInstance(coordNodeName);
        final var masterTaskManager = MockTransportService.getInstance(internalCluster().getMasterName()).getTaskManager();

        final SubscribableListener<RepositoryVerifyIntegrityTask.Status> snapshotsCompleteStatusListener = new SubscribableListener<>();
        final AtomicInteger chunksSeenCounter = new AtomicInteger();

        coordNodeTransportService.<TransportRepositoryVerifyIntegrityResponseChunkAction.Request>addRequestHandlingBehavior(
            TransportRepositoryVerifyIntegrityResponseChunkAction.ACTION_NAME,
            (handler, request, channel, task) -> {
                final SubscribableListener<Void> unblockChunkHandlingListener = switch (request.chunkContents().type()) {
                    case START_RESPONSE -> {
                        final var status = asInstanceOf(
                            RepositoryVerifyIntegrityTask.Status.class,
                            randomBoolean()
                                ? masterTaskManager.getTask(task.getParentTaskId().getId()).getStatus()
                                : client().admin()
                                    .cluster()
                                    .prepareGetTask(task.getParentTaskId())
                                    .get(SAFE_AWAIT_TIMEOUT)
                                    .getTask()
                                    .getTask()
                                    .status()
                        );
                        assertEquals(testContext.repositoryName(), status.repositoryName());
                        assertEquals(testContext.snapshotNames().size(), status.snapshotCount());
                        assertEquals(0L, status.snapshotsVerified());
                        assertEquals(testContext.indexNames().size(), status.indexCount());
                        assertEquals(0L, status.indicesVerified());
                        assertEquals(testContext.indexNames().size() * testContext.snapshotNames().size(), status.indexSnapshotCount());
                        assertEquals(0L, status.indexSnapshotsVerified());
                        assertEquals(0L, status.blobsVerified());
                        assertEquals(0L, status.blobBytesVerified());
                        yield SubscribableListener.nullSuccess();
                    }
                    case INDEX_RESTORABILITY -> {
                        // several of these chunks might arrive concurrently; we want to verify the task status before processing any of
                        // them, so use SubscribableListener to pick out the first status
                        snapshotsCompleteStatusListener.onResponse(
                            asInstanceOf(
                                RepositoryVerifyIntegrityTask.Status.class,
                                masterTaskManager.getTask(task.getParentTaskId().getId()).getStatus()
                            )
                        );
                        yield snapshotsCompleteStatusListener.andThenAccept(status -> {
                            assertEquals(testContext.repositoryName(), status.repositoryName());
                            assertEquals(testContext.snapshotNames().size(), status.snapshotCount());
                            assertEquals(testContext.snapshotNames().size(), status.snapshotsVerified());
                            assertEquals(testContext.indexNames().size(), status.indexCount());
                            assertEquals(0L, status.indicesVerified());
                        });
                    }
                    case SNAPSHOT_INFO -> SubscribableListener.nullSuccess();
                    case ANOMALY -> fail(null, "should not see anomalies");
                };

                unblockChunkHandlingListener.addListener(ActionTestUtils.assertNoFailureListener(ignored -> {
                    chunksSeenCounter.incrementAndGet();
                    handler.messageReceived(request, channel, task);
                }));
            }
        );

        try (var client = createRestClient(coordNodeName)) {
            final var response = client.performRequest(testContext.getVerifyIntegrityRequest());
            assertEquals(1 + testContext.indexNames().size() + testContext.snapshotNames().size(), chunksSeenCounter.get());
            assertEquals(200, response.getStatusLine().getStatusCode());
            final var responseObjectPath = ObjectPath.createFromResponse(response);
            assertEquals(0, (int) responseObjectPath.evaluate("results.total_anomalies"));
            assertEquals("pass", responseObjectPath.evaluate("results.result"));
        } finally {
            coordNodeTransportService.clearAllRules();
        }
    }

    public void testShardSnapshotFailed() throws IOException {
        final var testContext = createTestContext();

        final var newIndex = randomIdentifier();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(newIndex)
                .setWaitForActiveShards(ActiveShardCount.NONE)
                .setSettings(indexSettings(1, 0).put(INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_id", "not-a-node-id"))
        );

        final var createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, testContext.repositoryName(), randomIdentifier())
            .setWaitForCompletion(true)
            .setPartial(true)
            .get();

        assertEquals(SnapshotState.PARTIAL, createSnapshotResponse.getSnapshotInfo().state());

        final var takeGoodSnapshot = randomBoolean();
        if (takeGoodSnapshot) {
            updateIndexSettings(Settings.builder().putNull(INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "_id"), newIndex);
            ensureGreen(newIndex);
            createSnapshot(testContext.repositoryName(), randomIdentifier(), List.of(newIndex));
        }

        final Response response;
        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
            response = getRestClient().performRequest(testContext.getVerifyIntegrityRequest());
        } finally {
            assertAcked(
                client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
            );
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        final var responseObjectPath = ObjectPath.createFromResponse(response);
        assertThat(getAnomalies(responseObjectPath), equalTo(Set.of()));
        assertEquals(0, (int) responseObjectPath.evaluate("results.total_anomalies"));
        assertEquals("pass", responseObjectPath.evaluate("results.result"));

        final var logEntryCount = responseObjectPath.evaluateArraySize("log");
        for (int i = 0; i < logEntryCount; i++) {
            if (newIndex.equals(responseObjectPath.evaluate("log." + i + ".index.name"))) {
                assertEquals(
                    takeGoodSnapshot ? 2 : 1,
                    (int) responseObjectPath.evaluate("log." + i + ".snapshot_restorability.total_snapshot_count")
                );
                assertEquals(
                    takeGoodSnapshot ? 1 : 0,
                    (int) responseObjectPath.evaluate("log." + i + ".snapshot_restorability.restorable_snapshot_count")
                );
            }
        }
    }

    public void testCorruption() throws IOException {
        final var testContext = createTestContext();

        final Response response;
        final Path corruptedFile;
        final RepositoryFileType corruptedFileType;
        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
            corruptedFile = BlobStoreCorruptionUtils.corruptRandomFile(testContext.repositoryRootPath());
            corruptedFileType = RepositoryFileType.getRepositoryFileType(testContext.repositoryRootPath(), corruptedFile);
            logger.info("--> corrupted file: {}", corruptedFile);
            logger.info("--> corrupted file type: {}", corruptedFileType);

            final var request = testContext.getVerifyIntegrityRequest();
            if (corruptedFileType == RepositoryFileType.SHARD_DATA || randomBoolean()) {
                request.addParameter("verify_blob_contents", null);
            }
            response = getRestClient().performRequest(request);
        } finally {
            assertAcked(
                client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
            );
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        final var responseObjectPath = ObjectPath.createFromResponse(response);
        final var logEntryCount = responseObjectPath.evaluateArraySize("log");
        final var anomalies = new HashSet<String>();
        final var seenIndexNames = new HashSet<String>();
        int fullyRestorableIndices = 0;
        for (int i = 0; i < logEntryCount; i++) {
            final String maybeAnomaly = responseObjectPath.evaluate("log." + i + ".anomaly");
            if (maybeAnomaly != null) {
                anomalies.add(maybeAnomaly);
            } else {
                final String indexName = responseObjectPath.evaluate("log." + i + ".index.name");
                if (indexName != null) {
                    assertTrue(seenIndexNames.add(indexName));
                    assertThat(testContext.indexNames(), hasItem(indexName));
                    final int totalSnapshots = responseObjectPath.evaluate("log." + i + ".snapshot_restorability.total_snapshot_count");
                    final int restorableSnapshots = responseObjectPath.evaluate(
                        "log." + i + ".snapshot_restorability.restorable_snapshot_count"
                    );
                    if (totalSnapshots == restorableSnapshots) {
                        fullyRestorableIndices += 1;
                    }
                }
            }
        }

        assertThat(
            fullyRestorableIndices,
            corruptedFileType == RepositoryFileType.SHARD_GENERATION || corruptedFileType.equals(RepositoryFileType.GLOBAL_METADATA)
                ? equalTo(testContext.indexNames().size())
                : lessThan(testContext.indexNames().size())
        );
        // Missing shard generation file is automatically repaired based on the shard snapshot files.
        // See also BlobStoreRepository#buildBlobStoreIndexShardSnapshots
        final boolean deletedShardGen = corruptedFileType == RepositoryFileType.SHARD_GENERATION && Files.exists(corruptedFile) == false;
        assertThat(anomalies, deletedShardGen ? empty() : not(empty()));
        assertThat(responseObjectPath.evaluate("results.total_anomalies"), greaterThanOrEqualTo(anomalies.size()));
        assertEquals(deletedShardGen ? "pass" : "fail", responseObjectPath.evaluate("results.result"));

        // remove permitted/expected anomalies to verify that no unexpected ones were seen
        switch (corruptedFileType) {
            case SNAPSHOT_INFO -> anomalies.remove("failed to load snapshot info");
            case GLOBAL_METADATA -> anomalies.remove("failed to load global metadata");
            case INDEX_METADATA -> anomalies.remove("failed to load index metadata");
            case SHARD_GENERATION -> {
                if (deletedShardGen == false) {
                    anomalies.remove("failed to load shard generation");
                }
            }
            case SHARD_SNAPSHOT_INFO -> anomalies.remove("failed to load shard snapshot");
            case SHARD_DATA -> {
                anomalies.remove("missing blob");
                anomalies.remove("mismatched blob length");
                anomalies.remove("corrupt data blob");
            }
        }
        assertThat(anomalies, empty());
    }

    public void testTransportException() throws IOException {
        final var testContext = createTestContext();

        // use non-master node to coordinate the request so that we can capture chunks being sent back
        final var coordNodeName = getCoordinatingNodeName();
        final var coordNodeTransportService = MockTransportService.getInstance(coordNodeName);
        final var masterTransportService = MockTransportService.getInstance(internalCluster().getMasterName());

        final var messageCount = 2 // request & response
            * (1 // forward to master
                + 1 // start response
                + testContext.indexNames().size() + testContext.snapshotNames().size());
        final var failureStep = between(1, messageCount);

        final var failTransportMessageBehaviour = new StubbableTransport.RequestHandlingBehavior<>() {
            final AtomicInteger currentStep = new AtomicInteger();

            @Override
            public void messageReceived(
                TransportRequestHandler<TransportRequest> handler,
                TransportRequest request,
                TransportChannel channel,
                Task task
            ) throws Exception {
                if (currentStep.incrementAndGet() == failureStep) {
                    throw new ElasticsearchException("simulated");
                } else {
                    handler.messageReceived(request, new TransportChannel() {
                        @Override
                        public String getProfileName() {
                            return "test";
                        }

                        @Override
                        public void sendResponse(TransportResponse response) {
                            if (currentStep.incrementAndGet() == failureStep) {
                                channel.sendResponse(new ElasticsearchException("simulated"));
                            } else {
                                channel.sendResponse(response);
                            }
                        }

                        @Override
                        public void sendResponse(Exception exception) {
                            if (currentStep.incrementAndGet() == failureStep) {
                                throw new AssertionError("shouldn't have failed yet");
                            } else {
                                channel.sendResponse(exception);
                            }
                        }
                    }, task);
                }
            }
        };

        masterTransportService.addRequestHandlingBehavior(
            TransportRepositoryVerifyIntegrityAction.ACTION_NAME,
            failTransportMessageBehaviour
        );

        coordNodeTransportService.addRequestHandlingBehavior(
            TransportRepositoryVerifyIntegrityResponseChunkAction.ACTION_NAME,
            failTransportMessageBehaviour
        );

        final var request = testContext.getVerifyIntegrityRequest();
        if (failureStep <= 2) {
            request.addParameter("ignore", "500");
        }
        final Response response;
        try (var restClient = createRestClient(coordNodeName)) {
            response = restClient.performRequest(request);
        }
        final var responseObjectPath = ObjectPath.createFromResponse(response);
        if (failureStep <= 2) {
            assertEquals(500, response.getStatusLine().getStatusCode());
            assertNotNull(responseObjectPath.evaluate("error"));
            assertEquals(500, (int) responseObjectPath.evaluate("status"));
        } else {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertNotNull(responseObjectPath.evaluate("log"));
            assertNotNull(responseObjectPath.evaluate("exception"));
        }

        assertNull(responseObjectPath.evaluate("results"));
    }

    public void testBadSnapshotInfo() throws IOException {
        final var testContext = createTestContext();

        final var snapshotInfoBlob = BlobStoreCorruptionUtils.getRandomFileToCorrupt(
            testContext.repositoryRootPath(),
            RepositoryFileType.SNAPSHOT_INFO
        );

        final SnapshotInfo snapshotInfo;
        try (var inputStream = Files.newInputStream(snapshotInfoBlob)) {
            snapshotInfo = SNAPSHOT_FORMAT.deserialize(testContext.repositoryName(), xContentRegistry(), inputStream);
        }

        final var newIndices = new ArrayList<>(snapshotInfo.indices());
        newIndices.remove(between(0, newIndices.size() - 1));

        final Response response;
        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
            try (var outputStream = Files.newOutputStream(snapshotInfoBlob)) {
                SNAPSHOT_FORMAT.serialize(
                    new SnapshotInfo(
                        snapshotInfo.snapshot(),
                        newIndices,
                        snapshotInfo.dataStreams(),
                        snapshotInfo.featureStates(),
                        snapshotInfo.reason(),
                        snapshotInfo.version(),
                        snapshotInfo.startTime(),
                        snapshotInfo.endTime(),
                        snapshotInfo.totalShards(),
                        snapshotInfo.successfulShards(),
                        snapshotInfo.shardFailures(),
                        snapshotInfo.includeGlobalState(),
                        snapshotInfo.userMetadata(),
                        snapshotInfo.state(),
                        snapshotInfo.indexSnapshotDetails()
                    ),
                    snapshotInfoBlob.toString(),
                    randomBoolean(),
                    outputStream
                );
            }

            response = getRestClient().performRequest(testContext.getVerifyIntegrityRequest());
        } finally {
            assertAcked(
                client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
            );
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(getAnomalies(ObjectPath.createFromResponse(response)), equalTo(Set.of("snapshot contents mismatch")));
    }

    public void testShardPathEmpty() throws IOException {
        final var testContext = createTestContext();

        final var shardPath = BlobStoreCorruptionUtils.getRandomFileToCorrupt(
            testContext.repositoryRootPath(),
            RepositoryFileType.SHARD_GENERATION
        ).getParent();

        final Response response;
        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
            IOUtils.rm(shardPath);
            response = getRestClient().performRequest(testContext.getVerifyIntegrityRequest());
        } finally {
            assertAcked(
                client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
            );
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(getAnomalies(ObjectPath.createFromResponse(response)), equalTo(Set.of("failed to load shard snapshot")));
    }

    public void testShardPathUnreadable() throws IOException {
        final var testContext = createTestContext();

        final var shardPath = BlobStoreCorruptionUtils.getRandomFileToCorrupt(
            testContext.repositoryRootPath(),
            RepositoryFileType.SHARD_GENERATION
        ).getParent();

        final Response response;
        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
            IOUtils.rm(shardPath);
            Files.write(shardPath, new byte[0], StandardOpenOption.CREATE_NEW);
            response = getRestClient().performRequest(testContext.getVerifyIntegrityRequest());
        } finally {
            assertAcked(
                client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
            );
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(getAnomalies(ObjectPath.createFromResponse(response)), equalTo(Set.of("failed to list shard container contents")));
    }

    public void testShardGenerationMissing() throws IOException {
        final var testContext = createTestContext();

        final var repository = asInstanceOf(
            BlobStoreRepository.class,
            internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class).repository(testContext.repositoryName())
        );
        final var repoSettings = repository.getMetadata().settings();

        final RepositoryData repositoryData = safeAwait(l -> repository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, l));

        assertAcked(
            client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
        );

        final var rootBlob = BlobStoreCorruptionUtils.getRandomFileToCorrupt(
            testContext.repositoryRootPath(),
            RepositoryFileType.ROOT_INDEX_N
        );

        final var indexToBreak = randomFrom(testContext.indexNames());
        final var newShardGenerations = ShardGenerations.builder();
        for (final var index : repositoryData.shardGenerations().indices()) {
            final var indexShardGenerations = repositoryData.shardGenerations().getGens(index);
            for (int i = 0; i < indexShardGenerations.size(); i++) {
                if (i > 0 || index.getName().equals(indexToBreak) == false) {
                    newShardGenerations.put(index, i, indexShardGenerations.get(i));
                }
            }
        }

        final var brokenRepositoryData = new RepositoryData(
            repositoryData.getUuid(),
            repositoryData.getGenId(),
            repositoryData.getSnapshotIds().stream().collect(Collectors.toMap(SnapshotId::getUUID, Function.identity())),
            repositoryData.getSnapshotIds().stream().collect(Collectors.toMap(SnapshotId::getUUID, repositoryData::getSnapshotDetails)),
            repositoryData.getIndices().values().stream().collect(Collectors.toMap(Function.identity(), repositoryData::getSnapshots)),
            newShardGenerations.build(),
            repositoryData.indexMetaDataGenerations(),
            repositoryData.getClusterUUID()
        );

        final Response response;
        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {

            Files.write(
                rootBlob,
                BytesReference.toBytes(
                    BytesReference.bytes(brokenRepositoryData.snapshotsToXContent(XContentFactory.jsonBuilder(), IndexVersion.current()))
                ),
                StandardOpenOption.TRUNCATE_EXISTING
            );

            assertAcked(
                client().admin()
                    .cluster()
                    .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
                    .setType(FsRepository.TYPE)
                    .setSettings(repoSettings)
            );

            response = getRestClient().performRequest(testContext.getVerifyIntegrityRequest());
        } finally {
            assertAcked(
                client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
            );
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(getAnomalies(ObjectPath.createFromResponse(response)), equalTo(Set.of("shard generation not defined")));
    }

    public void testSnapshotNotInShardGeneration() throws IOException {
        final var testContext = createTestContext();
        runInconsistentShardGenerationBlobTest(
            testContext,
            blobStoreIndexShardSnapshots -> blobStoreIndexShardSnapshots.withRetainedSnapshots(
                testContext.snapshotNames().stream().skip(1).map(n -> new SnapshotId(n, "_na_")).collect(Collectors.toSet())
            ),
            "snapshot not in shard generation"
        );
    }

    public void testBlobInShardGenerationButNotSnapshot() throws IOException {
        final var testContext = createTestContext();
        final var snapshotToUpdate = randomFrom(testContext.snapshotNames());
        runInconsistentShardGenerationBlobTest(testContext, blobStoreIndexShardSnapshots -> {
            BlobStoreIndexShardSnapshots result = BlobStoreIndexShardSnapshots.EMPTY;
            for (final var snapshotFiles : blobStoreIndexShardSnapshots.snapshots()) {
                if (snapshotFiles.snapshot().equals(snapshotToUpdate)) {
                    result = result.withAddedSnapshot(
                        new SnapshotFiles(
                            snapshotToUpdate,
                            CollectionUtils.appendToCopy(
                                snapshotFiles.indexFiles(),
                                new BlobStoreIndexShardSnapshot.FileInfo(
                                    "extra",
                                    new StoreFileMetadata("extra", 1L, "checksum", Version.CURRENT.toString()),
                                    ByteSizeValue.ONE
                                )
                            ),
                            snapshotFiles.shardStateIdentifier()
                        )
                    );
                } else {
                    result = result.withAddedSnapshot(snapshotFiles);
                }
            }
            return result;
        }, "blob in shard generation but not snapshot");
    }

    public void testSnapshotShardGenerationMismatch() throws IOException {
        final var testContext = createTestContext();
        runInconsistentShardGenerationBlobTest(testContext, blobStoreIndexShardSnapshots -> {
            final var fileToUpdate = randomFrom(blobStoreIndexShardSnapshots.iterator().next().indexFiles());
            final var updatedFile = new BlobStoreIndexShardSnapshot.FileInfo(
                fileToUpdate.name(),
                fileToUpdate.metadata(),
                ByteSizeValue.ONE
            );
            assertFalse(fileToUpdate.isSame(updatedFile));

            BlobStoreIndexShardSnapshots result = BlobStoreIndexShardSnapshots.EMPTY;
            for (final var snapshotFiles : blobStoreIndexShardSnapshots.snapshots()) {
                result = result.withAddedSnapshot(
                    new SnapshotFiles(
                        snapshotFiles.snapshot(),
                        snapshotFiles.indexFiles()
                            .stream()
                            .map(fileInfo -> fileInfo.name().equals(fileToUpdate.name()) ? updatedFile : fileInfo)
                            .toList(),
                        snapshotFiles.shardStateIdentifier()
                    )
                );
            }
            return result;
        }, "snapshot shard generation mismatch");
    }

    public void testBlobInSnapshotNotShardGeneration() throws IOException {
        final var testContext = createTestContext();
        final var snapshotToUpdate = randomFrom(testContext.snapshotNames());
        runInconsistentShardGenerationBlobTest(testContext, blobStoreIndexShardSnapshots -> {
            BlobStoreIndexShardSnapshots result = BlobStoreIndexShardSnapshots.EMPTY;
            for (final var snapshotFiles : blobStoreIndexShardSnapshots.snapshots()) {
                if (snapshotFiles.snapshot().equals(snapshotToUpdate)) {
                    final var indexFilesCopy = new ArrayList<>(snapshotFiles.indexFiles());
                    indexFilesCopy.remove(between(0, indexFilesCopy.size() - 1));
                    result = result.withAddedSnapshot(
                        new SnapshotFiles(snapshotToUpdate, indexFilesCopy, snapshotFiles.shardStateIdentifier())
                    );
                } else {
                    result = result.withAddedSnapshot(snapshotFiles);
                }
            }
            return result;
        }, "blob in snapshot but not shard generation");
    }

    private void runInconsistentShardGenerationBlobTest(
        TestContext testContext,
        UnaryOperator<BlobStoreIndexShardSnapshots> shardGenerationUpdater,
        String expectedAnomaly
    ) throws IOException {

        final var shardGenerationBlob = BlobStoreCorruptionUtils.getRandomFileToCorrupt(
            testContext.repositoryRootPath(),
            RepositoryFileType.SHARD_GENERATION
        );

        final BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots;
        try (var inputStream = Files.newInputStream(shardGenerationBlob)) {
            blobStoreIndexShardSnapshots = INDEX_SHARD_SNAPSHOTS_FORMAT.deserialize(
                testContext.repositoryName(),
                xContentRegistry(),
                inputStream
            );
        }

        final Response response;
        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
            try (var outputStream = Files.newOutputStream(shardGenerationBlob)) {
                INDEX_SHARD_SNAPSHOTS_FORMAT.serialize(
                    shardGenerationUpdater.apply(blobStoreIndexShardSnapshots),
                    shardGenerationBlob.toString(),
                    randomBoolean(),
                    outputStream
                );
            }
            response = getRestClient().performRequest(testContext.getVerifyIntegrityRequest());
        } finally {
            assertAcked(
                client().admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, testContext.repositoryName())
            );
        }
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(getAnomalies(ObjectPath.createFromResponse(response)), equalTo(Set.of(expectedAnomaly)));
    }

    private Set<String> getAnomalies(ObjectPath responseObjectPath) throws IOException {
        final var logEntryCount = responseObjectPath.evaluateArraySize("log");
        final var anomalies = new HashSet<String>();
        for (int i = 0; i < logEntryCount; i++) {
            final String maybeAnomaly = responseObjectPath.evaluate("log." + i + ".anomaly");
            if (maybeAnomaly != null) {
                anomalies.add(maybeAnomaly);
            }
        }

        assertThat(responseObjectPath.evaluate("results.total_anomalies"), greaterThanOrEqualTo(anomalies.size()));
        if (anomalies.size() > 0) {
            assertEquals("fail", responseObjectPath.evaluate("results.result"));
        }

        return anomalies;
    }

    private record TestContext(String repositoryName, Path repositoryRootPath, List<String> indexNames, List<String> snapshotNames) {
        Request getVerifyIntegrityRequest() {
            final var request = new Request("POST", "/_snapshot/" + repositoryName + "/_verify_integrity");
            if (randomBoolean()) {
                request.addParameter("human", null);
            }
            if (randomBoolean()) {
                request.addParameter("pretty", null);
            }
            return request;
        }
    }

    private TestContext createTestContext() {
        final var repositoryName = randomIdentifier();
        final var repositoryRootPath = randomRepoPath();

        createRepository(repositoryName, FsRepository.TYPE, repositoryRootPath);

        final var indexNames = randomList(1, 3, ESTestCase::randomIdentifier);
        for (var indexName : indexNames) {
            createIndexWithRandomDocs(indexName, between(1, 100));
            flushAndRefresh(indexName);
        }

        final var snapshotNames = randomList(1, 3, ESTestCase::randomIdentifier);
        for (var snapshotName : snapshotNames) {
            createSnapshot(repositoryName, snapshotName, indexNames);
        }

        return new TestContext(repositoryName, repositoryRootPath, indexNames, snapshotNames);
    }

    private static String getCoordinatingNodeName() {
        if (internalCluster().size() == 1) {
            internalCluster().startNode();
        }
        return randomValueOtherThan(internalCluster().getMasterName(), () -> internalCluster().getRandomNodeName());
    }
}
