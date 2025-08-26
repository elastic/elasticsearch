/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.mock;

public class RepositoriesServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private RepositoriesService repositoriesService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        threadPool = new TestThreadPool(RepositoriesService.class.getName());

        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(), boundAddress.publishAddress()),
            null,
            Collections.emptySet()
        );
        clusterService = ClusterServiceUtils.createClusterService(threadPool);

        DiscoveryNode localNode = DiscoveryNodeUtils.builder("local").name("local").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        var actionFilters = new ActionFilters(Set.of());
        client.initialize(
            Map.of(
                VerifyNodeRepositoryCoordinationAction.TYPE,
                new VerifyNodeRepositoryCoordinationAction.LocalAction(actionFilters, transportService, clusterService, client)
            ),
            transportService.getTaskManager(),
            localNode::getId,
            transportService.getLocalNodeConnection(),
            null
        );

        // cluster utils publisher does not call AckListener, making some method calls hang indefinitely
        // in this test we have a single master node, and it acknowledges cluster state immediately
        final var publisher = ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService());
        clusterService.getMasterService().setClusterStatePublisher((evt, pub, ack) -> {
            publisher.publish(evt, pub, ack);
            ack.onCommit(TimeValue.ZERO);
            ack.onNodeAck(clusterService.localNode(), null);
        });

        Map<String, Repository.Factory> typesRegistry = Map.of(
            TestRepository.TYPE,
            TestRepository::new,
            UnstableRepository.TYPE,
            UnstableRepository::new,
            VerificationFailRepository.TYPE,
            VerificationFailRepository::new,
            MeteredRepositoryTypeA.TYPE,
            metadata -> new MeteredRepositoryTypeA(metadata, clusterService),
            MeteredRepositoryTypeB.TYPE,
            metadata -> new MeteredRepositoryTypeB(metadata, clusterService)
        );
        repositoriesService = new RepositoriesService(
            Settings.EMPTY,
            clusterService,
            typesRegistry,
            typesRegistry,
            threadPool,
            client,
            List.of()
        );

        clusterService.start();
        repositoriesService.start();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
        clusterService.stop();
        repositoriesService.stop();
    }

    public void testRegisterInternalRepository() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertEquals(repoName, repository.getMetadata().name());
        assertEquals(TestRepository.TYPE, repository.getMetadata().type());
        assertEquals(Settings.EMPTY, repository.getMetadata().settings());
        assertTrue(((TestRepository) repository).isStarted);
    }

    public void testUnregisterInternalRepository() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertFalse(((TestRepository) repository).isClosed);
        repositoriesService.unregisterInternalRepository(repoName);
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        assertTrue(((TestRepository) repository).isClosed);
    }

    public void testRegisterWillNotUpdateIfInternalRepositoryWithNameExists() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertFalse(((TestRepository) repository).isClosed);
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        assertFalse(((TestRepository) repository).isClosed);
        Repository repository2 = repositoriesService.repository(repoName);
        assertSame(repository, repository2);
    }

    public void testRegisterRejectsInvalidRepositoryNames() {
        assertThrowsOnRegister("");
        assertThrowsOnRegister("contains#InvalidCharacter");
        for (char c : Arrays.asList('\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',')) {
            assertThrowsOnRegister("contains" + c + "InvalidCharacters");
        }
    }

    public void testPutRepositoryVerificationFails() {
        var repoName = randomAlphaOfLengthBetween(10, 25);
        var request = new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).name(repoName)
            .type(VerificationFailRepository.TYPE)
            .verify(true);
        var resultListener = new SubscribableListener<AcknowledgedResponse>();
        repositoriesService.registerRepository(request, resultListener);
        var failure = safeAwaitFailure(resultListener);
        assertThat(failure, isA(RepositoryVerificationException.class));
        // also make sure that cluster state does not include failed repo
        assertThrows(RepositoryMissingException.class, () -> { repositoriesService.repository(repoName); });
    }

    public void testPutRepositoryVerificationFailsOnExisting() {
        var repoName = randomAlphaOfLengthBetween(10, 25);
        var request = new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).name(repoName)
            .type(TestRepository.TYPE)
            .verify(true);
        var resultListener = new SubscribableListener<AcknowledgedResponse>();
        repositoriesService.registerRepository(request, resultListener);
        var ackResponse = safeAwait(resultListener);
        assertTrue(ackResponse.isAcknowledged());

        // try to update existing repository with faulty repo and make sure it is not applied
        request = new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).name(repoName)
            .type(VerificationFailRepository.TYPE)
            .verify(true);
        resultListener = new SubscribableListener<>();
        repositoriesService.registerRepository(request, resultListener);
        var failure = safeAwaitFailure(resultListener);
        assertThat(failure, isA(RepositoryVerificationException.class));
        var repository = repositoriesService.repository(repoName);
        assertEquals(repository.getMetadata().type(), TestRepository.TYPE);
    }

    public void testPutRepositorySkipVerification() {
        var repoName = randomAlphaOfLengthBetween(10, 25);
        var request = new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).name(repoName)
            .type(VerificationFailRepository.TYPE)
            .verify(false);
        var resultListener = new SubscribableListener<AcknowledgedResponse>();
        repositoriesService.registerRepository(request, resultListener);
        var ackResponse = safeAwait(resultListener);
        assertTrue(ackResponse.isAcknowledged());
    }

    public void testRepositoriesStatsCanHaveTheSameNameAndDifferentTypeOverTime() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));

        ClusterState clusterStateWithRepoTypeA = createClusterStateWithRepo(repoName, MeteredRepositoryTypeA.TYPE);

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", emptyState(), clusterStateWithRepoTypeA));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));

        ClusterState clusterStateWithRepoTypeB = createClusterStateWithRepo(repoName, MeteredRepositoryTypeB.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeB, emptyState()));

        List<RepositoryStatsSnapshot> repositoriesStats = repositoriesService.repositoriesStats();
        assertThat(repositoriesStats.size(), equalTo(2));
        RepositoryStatsSnapshot repositoryStatsTypeA = repositoriesStats.get(0);
        assertThat(repositoryStatsTypeA.getRepositoryInfo().type, equalTo(MeteredRepositoryTypeA.TYPE));
        assertThat(repositoryStatsTypeA.getRepositoryStats(), equalTo(MeteredRepositoryTypeA.STATS));

        RepositoryStatsSnapshot repositoryStatsTypeB = repositoriesStats.get(1);
        assertThat(repositoryStatsTypeB.getRepositoryInfo().type, equalTo(MeteredRepositoryTypeB.TYPE));
        assertThat(repositoryStatsTypeB.getRepositoryStats(), equalTo(MeteredRepositoryTypeB.STATS));
    }

    // this can happen when the repository plugin is removed, but repository is still exist
    public void testHandlesUnknownRepositoryTypeWhenApplyingClusterState() {
        var repoName = randomAlphaOfLengthBetween(10, 25);

        var clusterState = createClusterStateWithRepo(repoName, "unknown");
        repositoriesService.applyClusterState(new ClusterChangedEvent("starting", clusterState, emptyState()));

        var repo = repositoriesService.repository(repoName);
        assertThat(repo, isA(UnknownTypeRepository.class));
    }

    public void testRemoveUnknownRepositoryTypeWhenApplyingClusterState() {
        var repoName = randomAlphaOfLengthBetween(10, 25);

        var clusterState = createClusterStateWithRepo(repoName, "unknown");
        repositoriesService.applyClusterState(new ClusterChangedEvent("starting", clusterState, emptyState()));
        repositoriesService.applyClusterState(new ClusterChangedEvent("removing repo", emptyState(), clusterState));

        assertThat(
            expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName)).getMessage(),
            equalTo("[" + repoName + "] missing")
        );
    }

    public void testRegisterRepositoryFailsForUnknownType() {
        var repoName = randomAlphaOfLengthBetween(10, 25);
        var request = new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).name(repoName).type("unknown");

        repositoriesService.registerRepository(request, new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                fail("Should not register unknown repository type");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, isA(RepositoryException.class));
                assertThat(e.getMessage(), equalTo("[" + repoName + "] repository type [unknown] does not exist"));
            }
        });
    }

    // test InvalidRepository is returned if repository failed to create
    public void testHandlesCreationFailureWhenApplyingClusterState() {
        var repoName = randomAlphaOfLengthBetween(10, 25);

        var clusterState = createClusterStateWithRepo(repoName, UnstableRepository.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("put unstable repository", clusterState, emptyState()));

        var repo = repositoriesService.repository(repoName);
        assertThat(repo, isA(InvalidRepository.class));
    }

    // test InvalidRepository can be replaced if current repo is created successfully
    public void testReplaceInvalidRepositoryWhenCreationSuccess() {
        var repoName = randomAlphaOfLengthBetween(10, 25);

        var clusterState = createClusterStateWithRepo(repoName, UnstableRepository.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("put unstable repository", clusterState, emptyState()));

        var repo = repositoriesService.repository(repoName);
        assertThat(repo, isA(InvalidRepository.class));

        clusterState = createClusterStateWithRepo(repoName, TestRepository.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("put test repository", clusterState, emptyState()));
        repo = repositoriesService.repository(repoName);
        assertThat(repo, isA(TestRepository.class));
    }

    // test remove InvalidRepository when current repo is removed in cluster state
    public void testRemoveInvalidRepositoryTypeWhenApplyingClusterState() {
        var repoName = randomAlphaOfLengthBetween(10, 25);

        var clusterState = createClusterStateWithRepo(repoName, UnstableRepository.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("put unstable repository", clusterState, emptyState()));
        repositoriesService.applyClusterState(new ClusterChangedEvent("removing repo", emptyState(), clusterState));
        assertThat(
            expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName)).getMessage(),
            equalTo("[" + repoName + "] missing")
        );
    }

    public void testRepositoriesThrottlingStats() {
        var repoName = randomAlphaOfLengthBetween(10, 25);
        var clusterState = createClusterStateWithRepo(repoName, TestRepository.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("put test repository", clusterState, emptyState()));
        RepositoriesStats throttlingStats = repositoriesService.getRepositoriesThrottlingStats();
        assertTrue(throttlingStats.getRepositoryThrottlingStats().containsKey(repoName));
        assertNotNull(throttlingStats.getRepositoryThrottlingStats().get(repoName));
    }

    // InvalidRepository is created when current node is non-master node and failed to create repository by applying cluster state from
    // master. When current node become master node later and same repository is put again, current node can create repository successfully
    // and replace previous InvalidRepository
    public void testRegisterRepositorySuccessAfterCreationFailed() {
        // 1. repository creation failed when current node is non-master node and apply cluster state from master
        var repoName = randomAlphaOfLengthBetween(10, 25);

        var clusterState = createClusterStateWithRepo(repoName, UnstableRepository.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("put unstable repository", clusterState, emptyState()));

        var repo = repositoriesService.repository(repoName);
        assertThat(repo, isA(InvalidRepository.class));

        // 2. repository creation successfully when current node become master node and repository is put again
        var request = new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).name(repoName).type(TestRepository.TYPE);

        var resultListener = new SubscribableListener<AcknowledgedResponse>();
        repositoriesService.registerRepository(request, resultListener);
        var response = safeAwait(resultListener);
        assertTrue(response.isAcknowledged());
        assertThat(repositoriesService.repository(repoName), isA(TestRepository.class));
    }

    public void testCannotSetRepositoryReadonlyFlagDuringGenerationChange() {
        final var repoName = randomAlphaOfLengthBetween(10, 25);
        final long originalGeneration = randomFrom(RepositoryData.EMPTY_REPO_GEN, 0L, 1L, randomLongBetween(2, Long.MAX_VALUE - 1));
        final long newGeneration = originalGeneration + 1;

        safeAwait(
            SubscribableListener

                .newForked(
                    l -> repositoriesService.registerRepository(
                        new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).type(TestRepository.TYPE),
                        l.map(ignored -> null)
                    )
                )
                .andThen(l -> updateGenerations(repoName, originalGeneration, newGeneration, l))
                .andThenAccept(ignored -> {
                    final var metadata = repositoriesService.repository(repoName).getMetadata();
                    assertEquals(originalGeneration, metadata.generation());
                    assertEquals(newGeneration, metadata.pendingGeneration());
                    assertNull(metadata.settings().getAsBoolean(BlobStoreRepository.READONLY_SETTING_KEY, null));
                })
                .andThen(
                    l -> repositoriesService.registerRepository(
                        new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).type(TestRepository.TYPE)
                            .settings(Settings.builder().put(BlobStoreRepository.READONLY_SETTING_KEY, true)),
                        ActionTestUtils.assertNoSuccessListener(e -> {
                            assertEquals(
                                Strings.format(
                                    """
                                        [%s] trying to modify or unregister repository that is currently used \
                                        (currently updating root blob generation from [%d] to [%d], cannot update readonly flag)""",
                                    repoName,
                                    originalGeneration,
                                    newGeneration
                                ),
                                asInstanceOf(RepositoryConflictException.class, e).getMessage()
                            );
                            l.onResponse(null);
                        })
                    )
                )
                .andThenAccept(ignored -> {
                    final var metadata = repositoriesService.repository(repoName).getMetadata();
                    assertEquals(originalGeneration, metadata.generation());
                    assertEquals(newGeneration, metadata.pendingGeneration());
                    assertNull(metadata.settings().getAsBoolean(BlobStoreRepository.READONLY_SETTING_KEY, null));
                })
                .andThen(l -> updateGenerations(repoName, newGeneration, newGeneration, l))
                .andThenAccept(ignored -> {
                    final var metadata = repositoriesService.repository(repoName).getMetadata();
                    assertEquals(newGeneration, metadata.generation());
                    assertEquals(newGeneration, metadata.pendingGeneration());
                    assertNull(metadata.settings().getAsBoolean(BlobStoreRepository.READONLY_SETTING_KEY, null));
                })
                .andThen(
                    l -> repositoriesService.registerRepository(
                        new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).type(TestRepository.TYPE)
                            .settings(Settings.builder().put(BlobStoreRepository.READONLY_SETTING_KEY, true)),
                        l.map(ignored -> null)
                    )
                )
                .andThenAccept(
                    ignored -> assertTrue(
                        repositoriesService.repository(repoName)
                            .getMetadata()
                            .settings()
                            .getAsBoolean(BlobStoreRepository.READONLY_SETTING_KEY, null)
                    )
                )
        );
    }

    private void updateGenerations(String repositoryName, long safeGeneration, long pendingGeneration, ActionListener<?> listener) {
        clusterService.submitUnbatchedStateUpdateTask("update repo generations", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return new ClusterState.Builder(currentState).metadata(
                    Metadata.builder(currentState.metadata())
                        .putCustom(
                            RepositoriesMetadata.TYPE,
                            RepositoriesMetadata.get(currentState).withUpdatedGeneration(repositoryName, safeGeneration, pendingGeneration)
                        )
                ).build();
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                listener.onResponse(null);
            }
        });
    }

    private ClusterState createClusterStateWithRepo(String repoName, String repoType) {
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.putCustom(
            RepositoriesMetadata.TYPE,
            new RepositoriesMetadata(Collections.singletonList(new RepositoryMetadata(repoName, repoType, Settings.EMPTY)))
        );
        state.metadata(mdBuilder);

        return state.build();
    }

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).build();
    }

    private void assertThrowsOnRegister(String repoName) {
        expectThrows(
            RepositoryException.class,
            () -> repositoriesService.registerRepository(
                new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName),
                null
            )
        );
    }

    private static class TestRepository implements Repository {

        private static final String TYPE = "internal";
        private RepositoryMetadata metadata;
        private boolean isClosed;
        private boolean isStarted;

        private TestRepository(RepositoryMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public RepositoryMetadata getMetadata() {
            return metadata;
        }

        @Override
        public void getSnapshotInfo(
            Collection<SnapshotId> snapshotIds,
            boolean abortOnFailure,
            BooleanSupplier isCancelled,
            CheckedConsumer<SnapshotInfo, Exception> consumer,
            ActionListener<Void> listener
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
            return null;
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
            return null;
        }

        @Override
        public void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener) {
            listener.onResponse(RepositoryData.EMPTY);
        }

        @Override
        public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
            finalizeSnapshotContext.onResponse(null);
        }

        @Override
        public void deleteSnapshots(
            Collection<SnapshotId> snapshotIds,
            long repositoryDataGeneration,
            IndexVersion minimumNodeVersion,
            ActionListener<RepositoryData> repositoryDataUpdateListener,
            Runnable onCompletion
        ) {
            repositoryDataUpdateListener.onFailure(new UnsupportedOperationException());
        }

        @Override
        public long getSnapshotThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public long getRestoreThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public String startVerification() {
            return null;
        }

        @Override
        public void endVerification(String verificationToken) {

        }

        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {

        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public void snapshotShard(SnapshotShardContext context) {

        }

        @Override
        public void restoreShard(
            Store store,
            SnapshotId snapshotId,
            IndexId indexId,
            ShardId snapshotShardId,
            RecoveryState recoveryState,
            ActionListener<Void> listener
        ) {

        }

        @Override
        public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
            return null;
        }

        @Override
        public void updateState(final ClusterState state) {
            metadata = RepositoriesMetadata.get(state).repository(metadata.name());
        }

        @Override
        public void cloneShardSnapshot(
            SnapshotId source,
            SnapshotId target,
            RepositoryShardId shardId,
            ShardGeneration shardGeneration,
            ActionListener<ShardSnapshotResult> listener
        ) {

        }

        @Override
        public void awaitIdle() {}

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void start() {
            isStarted = true;
        }

        @Override
        public void stop() {

        }

        @Override
        public void close() {
            isClosed = true;
        }
    }

    private static class UnstableRepository extends TestRepository {
        private static final String TYPE = "unstable";

        private UnstableRepository(RepositoryMetadata metadata) {
            super(metadata);
            throw new RepositoryException(TYPE, "failed to create unstable repository");
        }
    }

    private static class VerificationFailRepository extends TestRepository {
        public static final String TYPE = "verify-fail";

        private VerificationFailRepository(RepositoryMetadata metadata) {
            super(metadata);
        }

        @Override
        public String startVerification() {
            throw new RepositoryVerificationException(TYPE, "failed to validate repository");
        }
    }

    private static class MeteredRepositoryTypeA extends MeteredBlobStoreRepository {
        private static final String TYPE = "type-a";
        private static final RepositoryStats STATS = new RepositoryStats(Map.of("GET", 10L));

        private MeteredRepositoryTypeA(RepositoryMetadata metadata, ClusterService clusterService) {
            super(
                metadata,
                mock(NamedXContentRegistry.class),
                clusterService,
                MockBigArrays.NON_RECYCLING_INSTANCE,
                mock(RecoverySettings.class),
                BlobPath.EMPTY,
                Map.of("bucket", "bucket-a")
            );
        }

        @Override
        protected BlobStore createBlobStore() {
            return mock(BlobStore.class);
        }

        @Override
        public RepositoryStats stats() {
            return STATS;
        }
    }

    private static class MeteredRepositoryTypeB extends MeteredBlobStoreRepository {
        private static final String TYPE = "type-b";
        private static final RepositoryStats STATS = new RepositoryStats(Map.of("LIST", 20L));

        private MeteredRepositoryTypeB(RepositoryMetadata metadata, ClusterService clusterService) {
            super(
                metadata,
                mock(NamedXContentRegistry.class),
                clusterService,
                MockBigArrays.NON_RECYCLING_INSTANCE,
                mock(RecoverySettings.class),
                BlobPath.EMPTY,
                Map.of("bucket", "bucket-b")
            );
        }

        @Override
        protected BlobStore createBlobStore() {
            return mock(BlobStore.class);
        }

        @Override
        public RepositoryStats stats() {
            return STATS;
        }
    }
}
