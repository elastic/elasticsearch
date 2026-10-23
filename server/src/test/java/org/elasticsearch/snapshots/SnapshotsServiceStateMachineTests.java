/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.VerifyNodeRepositoryAction;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.FakeTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.ClusterConnectionManager;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.junit.Before;

import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.snapshots.SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME;
import static org.hamcrest.Matchers.hasItem;

public class SnapshotsServiceStateMachineTests extends ESTestCase {

    private static final String REPOSITORY_TYPE = "fake";
    private DeterministicTaskQueue deterministicTaskQueue;
    private ThreadPool threadPool;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        deterministicTaskQueue = new DeterministicTaskQueue();
        deterministicTaskQueue.setExecutionDelayVariabilityMillis(1000);
        threadPool = deterministicTaskQueue.getThreadPool();
    }

    public void testBasicActivities() {
        final var repositoryName = "repo";
        try (var ts = createServices(Settings.EMPTY)) {
            final var future = new PlainActionFuture<Void>();
            SubscribableListener

                .<AcknowledgedResponse>newForked(
                    l -> ts.repositoriesService.registerRepository(new PutRepositoryRequest(repositoryName).type(REPOSITORY_TYPE), l)
                )

                .<SnapshotInfo>andThen((l, r) -> {
                    logger.info("--> create snapshot");
                    ts.snapshotsService.executeSnapshot(new CreateSnapshotRequest(repositoryName, "snap-1"), l);
                })

                .<SnapshotInfo>andThen((l, r) -> {
                    logger.info("--> create snapshot");
                    ts.snapshotsService.executeSnapshot(new CreateSnapshotRequest(repositoryName, "snap-2"), l);
                })

                .<Void>andThen((l, r) -> {
                    logger.info("--> clone snapshot");
                    ts.snapshotsService.cloneSnapshot(
                        new CloneSnapshotRequest(repositoryName, "snap-2", "snap-3", new String[] { "*" }),
                        l
                    );
                })

                .<Void>andThen((l, r) -> {
                    final var snapshotNames = Set.of("snap-1", "snap-2", "snap-3");
                    ts.snapshotsService.deleteSnapshots(
                        new DeleteSnapshotRequest(repositoryName, randomArray(1, 2, String[]::new, () -> randomFrom(snapshotNames))),
                        l
                    );
                })

                // TODO: repository cleanup
                .addListener(future.map(ignored -> null));

            deterministicTaskQueue.runAllTasksInTimeOrder();
            assertTrue(future.isDone());
            future.actionGet();

            ts.assertStates();
        }
    }

    public void testMaybeFailActivities() {
        final var repositoryName = "repo";
        try (var ts = createServices(Settings.EMPTY)) {
            ts.shardSnapshotsMayFail.set(true);
            final var future = new PlainActionFuture<Void>();
            SubscribableListener

                .<AcknowledgedResponse>newForked(
                    l -> ts.repositoriesService.registerRepository(new PutRepositoryRequest(repositoryName).type(REPOSITORY_TYPE), l)
                )

                .<SnapshotInfo>andThen((l, r) -> {
                    logger.info("--> create snapshot");
                    ts.snapshotsService.executeSnapshot(new CreateSnapshotRequest(repositoryName, "snap-1"), l);
                })

                .<SnapshotInfo>andThen((l, r) -> {
                    logger.info("--> create snapshot");
                    ts.snapshotsService.executeSnapshot(new CreateSnapshotRequest(repositoryName, "snap-2"), l);
                })

                .<Void>andThen((l, r) -> {
                    logger.info("--> clone snapshot");
                    ts.snapshotsService.cloneSnapshot(
                        new CloneSnapshotRequest(repositoryName, "snap-2", "snap-3", new String[] { "*" }),
                        l
                    );
                })

                .<Void>andThen((l0, r) -> {
                    final var snapshotNames = Set.of("snap-1", "snap-2", "snap-3", "snap-4", "snap-5");
                    try (var refs = new RefCountingRunnable(() -> l0.onResponse(null))) {
                        runAsync(
                            refs.acquireListener(),
                            l -> ts.snapshotsService.executeSnapshot(
                                new CreateSnapshotRequest(repositoryName, randomFrom(snapshotNames)),
                                l.map(ignored -> null)
                            )
                        );

                        runAsync(
                            refs.acquireListener(),
                            l -> ts.snapshotsService.deleteSnapshots(
                                new DeleteSnapshotRequest(
                                    repositoryName,
                                    randomArray(1, 2, String[]::new, () -> randomFrom(snapshotNames))
                                ),
                                l
                            )
                        );

                        runAsync(
                            refs.acquireListener(),
                            l -> ts.snapshotsService.cloneSnapshot(
                                new CloneSnapshotRequest(
                                    repositoryName,
                                    randomFrom(snapshotNames),
                                    randomFrom(snapshotNames),
                                    new String[] { "*" }
                                ),
                                l
                            )
                        );
                    }
                })

                .addListener(future.map(ignored -> null));

            deterministicTaskQueue.runAllTasksInTimeOrder();
            assertTrue(future.isDone());
            future.actionGet();

            ts.assertStates();
        }
    }

    // TODO testRandomActivities
    // TODO node leaves cluster while holding shards (shards may also be ABORTED; may also let an ongoing delete start)
    // TODO index deleted while snapshot running
    // TODO primary shard moves from INITIALIZING to STARTED
    // TODO master failover, new master continues work of old master & old master completes listeners
    // TODO clones!
    // TODO name collisions (both running & completed)
    // TODO cleanups
    // TODO concurrency limit
    // TODO invalid snapshot name
    // TODO repository removed concurrently with snap operation
    // TODO metadata filtering (drop global metadata, and datastream metadata)
    // TODO snapshot a datastream concurrently with rollover (looks like we drop datastream metadata in this case)
    // TODO interleaved finalizations, finalizing snapshot not at head of list
    // TODO removing completed (failed?) snapshot ID from enqueued snapshot deletion
    // TODO concurrent ops in multiple repositories
    // TODO wildcard deletes (including match-none)
    // TODO delete with concurrent restore
    // TODO delete with concurrent clone
    // TODO aborting snapshot with no started shards
    // TODO batching deletes (adding snapshots to WAITING delete, and skipping snapshots in STARTED delete)
    // TODO retries in executeConsistentStateUpdate
    // TODO snapshot/clone enqueued behind running deletion

    private <T> void runAsync(ActionListener<T> listener, CheckedConsumer<ActionListener<T>, Exception> consumer) {
        threadPool.generic().execute(ActionRunnable.wrap(listener, consumer));
    }

    class TestServices implements Releasable {
        final TransportService transportService;
        final MasterService masterService;
        final ClusterService clusterService;
        final RepositoriesService repositoriesService;
        final SnapshotsService snapshotsService;
        private final AtomicBoolean shardSnapshotsMayFail;

        TestServices(
            TransportService transportService,
            MasterService masterService,
            ClusterService clusterService,
            RepositoriesService repositoriesService,
            SnapshotsService snapshotsService,
            AtomicBoolean shardSnapshotsMayFail
        ) {
            this.transportService = transportService;
            this.masterService = masterService;
            this.clusterService = clusterService;
            this.repositoriesService = repositoriesService;
            this.snapshotsService = snapshotsService;
            this.shardSnapshotsMayFail = shardSnapshotsMayFail;
        }

        public void start() {
            transportService.start();
            clusterService.start();
            repositoriesService.start();
            snapshotsService.start();
            transportService.acceptIncomingRequests();
        }

        public void assertStates() {
            assertTrue(snapshotsService.assertAllListenersResolved());
            final var repository = (FakeRepository) repositoriesService.repository("repo");
            repository.assertShardGenerationsPresent();
            // TODO repository.assertShardGenerationsUnique();
            // TODO also verify no leaked global metadata and snapshot info
            // TODO also verify no leaked index metadata
            logger.info("--> final states: {}", repository.repositoryShardStates);
        }

        @Override
        public void close() {
            snapshotsService.stop();
            repositoriesService.stop();
            clusterService.stop();
            transportService.stop();
            snapshotsService.close();
            repositoriesService.close();
            clusterService.close();
            transportService.close();
        }
    }

    private TestServices createServices(Settings settings) {
        final var clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);
        final var threadContext = threadPool.getThreadContext();
        final var localNode = DiscoveryNodeUtils.create("local");
        final var transportService = createTransportService(settings, localNode, clusterSettings, threadContext);

        final var clusterApplierService = new ClusterApplierService("test", settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };
        clusterApplierService.setNodeConnectionsService(new NodeConnectionsService(settings, threadPool, transportService));

        final String clusterUUID = UUIDs.randomBase64UUID(random());
        final var initialRepositoryData = RepositoryData.EMPTY.withClusterUuid(clusterUUID);

        final int dataNodeCount = between(1, 10);
        final var discoveryNodes = DiscoveryNodes.builder();
        discoveryNodes.add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId());
        for (int i = 0; i < dataNodeCount; i++) {
            discoveryNodes.add(DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DiscoveryNodeRole.DATA_ROLE)).build());
        }

        final var indexName = "test-index";
        final var indexMetadata = IndexMetadata.builder(indexName)
            .settings(
                indexSettings(IndexVersion.current(), between(1, 3), 0).put(SETTING_CREATION_DATE, System.currentTimeMillis())
                    .put(SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random()))
            )
            .build();
        final var indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int i = 0; i < indexMetadata.getRoutingNumShards(); i++) {
            indexRoutingTable.addShard(
                TestShardRouting.newShardRouting(
                    new ShardId(indexMetadata.getIndex(), i),
                    "node-" + between(0, dataNodeCount - 1),
                    true,
                    ShardRoutingState.STARTED
                )
            );
        }

        clusterApplierService.setInitialState(
            ClusterState.builder(ClusterName.DEFAULT)
                .nodes(discoveryNodes)
                .metadata(Metadata.builder().put(indexMetadata, false).clusterUUID(clusterUUID))
                .routingTable(RoutingTable.builder().add(indexRoutingTable))
                .build()
        );

        final var masterService = new MasterService(settings, clusterSettings, threadPool, transportService.getTaskManager()) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor(threadContext::preserveContext);
            }
        };
        masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            ackListener.onCommit(TimeValue.ZERO);
            final var newState = clusterStatePublicationEvent.getNewState();
            logger.info(
                """
                    --> cluster state version [{}] in term [{}]
                    SnapshotsInProgress
                    {}
                    SnapshotDeletionsInProgress
                    {}
                    RepositoryCleanupInProgress
                    {}
                    """,
                newState.version(),
                newState.term(),
                Strings.toString(SnapshotsInProgress.get(newState), true, true),
                Strings.toString(SnapshotDeletionsInProgress.get(newState), true, true),
                Strings.toString(RepositoryCleanupInProgress.get(newState), true, true)
            );
            clusterApplierService.onNewClusterState(
                clusterStatePublicationEvent.getSummary().toString(),
                clusterStatePublicationEvent::getNewState,
                publishListener.delegateFailureAndWrap((l, v) -> {
                    l.onResponse(v);
                    for (final var discoveryNode : newState.nodes()) {
                        ackListener.onNodeAck(discoveryNode, null);
                    }
                })
            );
        });
        masterService.setClusterStateSupplier(clusterApplierService::state);

        final var clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);

        final var repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            transportService,
            Map.of(REPOSITORY_TYPE, repositoryMetadata -> new FakeRepository(repositoryMetadata, initialRepositoryData, masterService)),
            Map.of(),
            threadPool,
            List.of()
        );

        final var systemIndices = new SystemIndices(List.of());

        final var snapshotsService = new SnapshotsService(
            settings,
            clusterService,
            new IndexNameExpressionResolver(threadPool.getThreadContext(), systemIndices),
            repositoriesService,
            transportService,
            new ActionFilters(Set.of()),
            systemIndices
        );

        final AtomicBoolean shardSnapshotsMayFail = new AtomicBoolean(false);

        clusterService.addStateApplier(new ClusterStateApplier() {

            private record OngoingShardSnapshot(Snapshot snapshot, ShardId shardId, RepositoryShardId repositoryShardId, String nodeId) {}

            private final Set<OngoingShardSnapshot> ongoingShardSnapshots = new HashSet<>();

            @Override
            public void applyClusterState(ClusterChangedEvent event) {
                final var activeSnapshotIds = SnapshotsInProgress.get(event.state())
                    .asStream()
                    .map(SnapshotsInProgress.Entry::snapshot)
                    .collect(Collectors.toSet());
                ongoingShardSnapshots.removeIf(ongoingShardSnapshot -> activeSnapshotIds.contains(ongoingShardSnapshot.snapshot) == false);

                SnapshotsInProgress.get(event.state()).asStream().forEach(snapshotInProgress -> {
                    if (snapshotInProgress.isClone() == false) {
                        for (final var shardSnapshotStatusEntry : snapshotInProgress.shards().entrySet()) {
                            switch (shardSnapshotStatusEntry.getValue().state()) {
                                case INIT -> {
                                    final var shardId = shardSnapshotStatusEntry.getKey();
                                    final var ongoingShardSnapshot = new OngoingShardSnapshot(
                                        snapshotInProgress.snapshot(),
                                        shardId,
                                        new RepositoryShardId(snapshotInProgress.indices().get(shardId.getIndexName()), shardId.id()),
                                        shardSnapshotStatusEntry.getValue().nodeId()
                                    );
                                    if (ongoingShardSnapshots.add(ongoingShardSnapshot)) {
                                        runAsync(new ActionListener<Void>() {
                                            @Override
                                            public void onResponse(Void unused) {}

                                            @Override
                                            public void onFailure(Exception e) {
                                                throw new AssertionError("unexpected", e);
                                            }
                                        },
                                            l -> doShardSnapshot(
                                                snapshotInProgress,
                                                ongoingShardSnapshot,
                                                shardSnapshotStatusEntry.getValue().generation(),
                                                l
                                            )
                                        );
                                    }
                                }
                                case ABORTED -> {
                                    final var shardId = shardSnapshotStatusEntry.getKey();
                                    final var ongoingShardSnapshot = new OngoingShardSnapshot(
                                        snapshotInProgress.snapshot(),
                                        shardId,
                                        new RepositoryShardId(snapshotInProgress.indices().get(shardId.getIndexName()), shardId.id()),
                                        shardSnapshotStatusEntry.getValue().nodeId()
                                    );
                                    // TODO can a shard bypass INIT and move straight to ABORTED?
                                    // Let's assert that it cannot for now, but we might need to treat ABORTED shards like INIT ones.
                                    assertThat(ongoingShardSnapshots, hasItem(ongoingShardSnapshot));
                                }
                            }
                        }
                    }
                });
            }

            private void doShardSnapshot(
                SnapshotsInProgress.Entry snapshotInProgress,
                OngoingShardSnapshot ongoingShardSnapshot,
                ShardGeneration originalShardGeneration,
                ActionListener<Void> listener
            ) {
                SubscribableListener

                    // perform shard snapshot
                    .<ShardSnapshotStatus>newForked(l -> ActionListener.completeWith(l, () -> {
                        final var repository = (FakeRepository) repositoriesService.repository(snapshotInProgress.repository());
                        final var repositoryShardState = repository.getShardState(ongoingShardSnapshot.repositoryShardId());
                        logger.info(
                            "--> doShardSnapshot[{}]: [{}] vs {}",
                            ongoingShardSnapshot.repositoryShardId(),
                            originalShardGeneration,
                            repositoryShardState.shardGenerations()
                        );
                        assertThat(
                            ongoingShardSnapshot.repositoryShardId().toString(),
                            repositoryShardState.shardGenerations(),
                            hasItem(originalShardGeneration)
                        );

                        final var newShardGeneration = ShardGeneration.newGeneration(random());

                        final ShardSnapshotStatus result;
                        if (shardSnapshotsMayFail.get() && rarely()) {
                            boolean createGeneration = randomBoolean();
                            if (createGeneration) {
                                repositoryShardState.shardGenerations().remove(ShardGenerations.NEW_SHARD_GEN);
                                repositoryShardState.shardGenerations().add(newShardGeneration);
                                // TODO might also fail after writing shard gen but without returning?
                            }
                            result = new ShardSnapshotStatus(
                                ongoingShardSnapshot.nodeId(),
                                SnapshotsInProgress.ShardState.FAILED,
                                "simulated",
                                createGeneration ? newShardGeneration : originalShardGeneration
                            );
                        } else {
                            repositoryShardState.shardGenerations().remove(ShardGenerations.NEW_SHARD_GEN);
                            repositoryShardState.shardGenerations().add(newShardGeneration);
                            result = ShardSnapshotStatus.success(
                                ongoingShardSnapshot.nodeId(),
                                new ShardSnapshotResult(newShardGeneration, ByteSizeValue.ZERO, 1)
                            );
                        }
                        logger.info(
                            "--> doShardSnapshot[{}] created shard gen [{}] returning [{}]",
                            ongoingShardSnapshot.repositoryShardId(),
                            newShardGeneration,
                            result
                        );
                        return result;
                    }))

                    // respond to master
                    // TODO might not even be able to notify master, then what? No retries, does snapshot just hang?
                    .<Void>andThen(
                        threadPool.generic(),
                        null,
                        (l, shardSnapshotStatus) -> transportService.sendRequest(
                            transportService.getLocalNodeConnection(),
                            UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
                            new UpdateIndexShardSnapshotStatusRequest(
                                ongoingShardSnapshot.snapshot,
                                ongoingShardSnapshot.shardId(),
                                shardSnapshotStatus
                            ),
                            TransportRequestOptions.EMPTY,
                            new ActionListenerResponseHandler<>(
                                l.map(ignored -> null),
                                in -> ActionResponse.Empty.INSTANCE,
                                threadPool.generic()
                            )
                        )
                    )

                    // check for no errors
                    .addListener(listener);
            }
        });

        final var testServices = new TestServices(
            transportService,
            masterService,
            clusterService,
            repositoriesService,
            snapshotsService,
            shardSnapshotsMayFail
        );
        testServices.start();
        return testServices;

    }

    private TransportService createTransportService(
        Settings settings,
        DiscoveryNode localNode,
        ClusterSettings clusterSettings,
        ThreadContext threadContext
    ) {
        final var transport = createFakeTransport();
        return new TransportService(
            settings,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            ignored -> localNode,
            clusterSettings,
            new ClusterConnectionManager(settings, transport, threadContext),
            new TaskManager(settings, threadPool, Set.of()),
            Tracer.NOOP
        );
    }

    private static FakeTransport createFakeTransport() {
        return new FakeTransport() {
            @Override
            public void openConnection(DiscoveryNode discoveryNode, ConnectionProfile profile, ActionListener<Connection> listener) {
                listener.onResponse(new CloseableConnection() {
                    @Override
                    public DiscoveryNode getNode() {
                        return discoveryNode;
                    }

                    @Override
                    public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                        throws TransportException {

                        switch (action) {
                            case TransportService.HANDSHAKE_ACTION_NAME -> lookupResponseHandler(requestId, action).handleResponse(
                                new TransportService.HandshakeResponse(
                                    Version.CURRENT,
                                    Build.current().hash(),
                                    discoveryNode,
                                    ClusterName.DEFAULT
                                )
                            );
                            case VerifyNodeRepositoryAction.ACTION_NAME -> lookupResponseHandler(requestId, action).handleResponse(
                                TransportResponse.Empty.INSTANCE
                            );
                            default -> throw new UnsupportedOperationException("unexpected action [" + action + "]");
                        }
                    }

                    @SuppressWarnings("unchecked")
                    private TransportResponseHandler<TransportResponse> lookupResponseHandler(long requestId, String action) {
                        return Objects.requireNonNull(
                            (TransportResponseHandler<TransportResponse>) getResponseHandlers().onResponseReceived(
                                requestId,
                                TransportMessageListener.NOOP_LISTENER
                            ),
                            action
                        );
                    }

                    @Override
                    public TransportVersion getTransportVersion() {
                        return TransportVersion.current();
                    }
                });
            }
        };
    }

    record RepositoryShardState(Set<ShardGeneration> shardGenerations) {}

    class FakeRepository extends AbstractLifecycleComponent implements Repository {

        private final Map<RepositoryShardId, RepositoryShardState> repositoryShardStates = new HashMap<>();
        private RepositoryData repositoryData;
        private RepositoryMetadata repositoryMetadata;
        private final MasterService masterService;
        private final Semaphore writeRepositoryDataPermits = new Semaphore(1);
        private final Map<SnapshotId, SnapshotInfo> snapshotInfos = new HashMap<>();
        private final Map<SnapshotId, Metadata> globalMetadatas = new HashMap<>();
        private final Map<String, IndexMetadata> indexMetadatas = new HashMap<>();

        FakeRepository(RepositoryMetadata repositoryMetadata, RepositoryData initialRepositoryData, MasterService masterService) {
            this.repositoryMetadata = repositoryMetadata;
            this.repositoryData = initialRepositoryData;
            this.masterService = masterService;
        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}

        @Override
        protected void doClose() {}

        @Override
        public RepositoryMetadata getMetadata() {
            return repositoryMetadata;
        }

        @Override
        public void getSnapshotInfo(GetSnapshotInfoContext context) {
            for (SnapshotId snapshotId : context.snapshotIds()) {
                threadPool.generic().execute(ActionRunnable.supply(context, () -> {
                    var snapshotInfo = snapshotInfos.get(snapshotId);
                    if (snapshotInfo == null) {
                        throw new SnapshotMissingException(repositoryMetadata.name(), snapshotId, null);
                    } else {
                        return snapshotInfo;
                    }
                }));
            }
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
            final var globalMetadata = globalMetadatas.get(snapshotId);
            if (globalMetadata == null) {
                throw new SnapshotMissingException(repositoryMetadata.name(), snapshotId, new NoSuchFileException("global metadata"));
            }
            return globalMetadata;
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
            final var indexMetadata = indexMetadatas.get(repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, index));
            if (indexMetadata == null) {
                throw new SnapshotMissingException(repositoryMetadata.name(), snapshotId, new NoSuchFileException(index.toString()));
            }
            return indexMetadata;
        }

        @Override
        public void getRepositoryData(Executor executor, ActionListener<RepositoryData> listener) {
            if (randomBoolean()) {
                listener.onResponse(repositoryData);
            } else {
                executor.execute(ActionRunnable.supply(listener, () -> repositoryData));
            }
        }

        @Override
        public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
            assertTrue(writeRepositoryDataPermits.tryAcquire()); // TODO will not hold on master failover
            snapshotInfos.put(finalizeSnapshotContext.snapshotInfo().snapshotId(), finalizeSnapshotContext.snapshotInfo());

            final Map<IndexId, String> currentSnapshotIndexMetadataIdentifierByIndex = new HashMap<>();
            final Map<String, String> newIndexMetaBlobNamesByIdentifier = new HashMap<>();

            SubscribableListener
                // get current repo data
                .<RepositoryData>newForked(l -> getRepositoryData(threadPool.generic(), l))

                // capture index metadata
                // TODO this duplicates quite some work in BlobStoreRepository, should we move more of it into SnapshotsServce?
                .<RepositoryData>andThen(threadPool.generic(), null, (l, currentRepositoryData) -> {
                    try (
                        var listeners = new RefCountingListener(
                            new ThreadedActionListener<>(threadPool.generic(), l.map(ignored -> currentRepositoryData))
                        )
                    ) {
                        for (IndexId indexId : finalizeSnapshotContext.updatedShardGenerations().indices()) {
                            threadPool.generic().execute(ActionRunnable.run(listeners.acquire(), () -> {
                                final var indexMetadata = finalizeSnapshotContext.clusterMetadata().index(indexId.getName());
                                final var indexMetadataIdentifier = IndexMetaDataGenerations.buildUniqueIdentifier(indexMetadata);
                                currentSnapshotIndexMetadataIdentifierByIndex.put(indexId, indexMetadataIdentifier);
                                if (currentRepositoryData.indexMetaDataGenerations().getIndexMetaBlobId(indexMetadataIdentifier) == null) {
                                    final var newIndexMetaBlobId = UUIDs.randomBase64UUID(random());
                                    newIndexMetaBlobNamesByIdentifier.put(indexMetadataIdentifier, newIndexMetaBlobId);
                                    FakeRepository.this.indexMetadatas.put(newIndexMetaBlobId, indexMetadata);
                                }
                            }));
                        }

                        threadPool.generic()
                            .execute(
                                ActionRunnable.run(
                                    listeners.acquire(),
                                    () -> globalMetadatas.put(
                                        finalizeSnapshotContext.snapshotInfo().snapshotId(),
                                        finalizeSnapshotContext.clusterMetadata()
                                    )
                                )
                            );
                    }
                })

                // compute new repo data
                .<RepositoryData>andThen(
                    threadPool.generic(),
                    null,
                    (l, currentRepositoryData) -> l.onResponse(
                        currentRepositoryData.addSnapshot(
                            finalizeSnapshotContext.snapshotInfo().snapshotId(),
                            new RepositoryData.SnapshotDetails(
                                finalizeSnapshotContext.snapshotInfo().state(),
                                IndexVersion.current(),
                                finalizeSnapshotContext.snapshotInfo().startTime(),
                                finalizeSnapshotContext.snapshotInfo().endTime(),
                                null
                            ),
                            finalizeSnapshotContext.updatedShardGenerations(),
                            currentSnapshotIndexMetadataIdentifierByIndex,
                            newIndexMetaBlobNamesByIdentifier
                        )
                    )
                )

                // store new repo data
                .<RepositoryData>andThen(
                    threadPool.generic(),
                    null,
                    (l, updatedRepositoryData) -> updateRepositoryData(
                        "finalize snapshot " + finalizeSnapshotContext.snapshotInfo().snapshotId(),
                        updatedRepositoryData,
                        finalizeSnapshotContext::updatedClusterState,
                        l.map(ignored -> updatedRepositoryData)
                    )
                )

                // fork background cleanup
                .<RepositoryData>andThen((l, updatedRepositoryData) -> {
                    l.onResponse(updatedRepositoryData);
                    try (var refs = new RefCountingRunnable(() -> finalizeSnapshotContext.onDone(finalizeSnapshotContext.snapshotInfo()))) {
                        for (final var obsoleteShardGeneration : finalizeSnapshotContext.obsoleteShardGenerations().entrySet()) {
                            final var repositoryShardState = getShardState(obsoleteShardGeneration.getKey());
                            for (final var shardGeneration : obsoleteShardGeneration.getValue()) {
                                threadPool.generic().execute(ActionRunnable.run(refs.acquireListener(), () -> {
                                    logger.info(
                                        "removing shard gen [{}] from [{}] in create",
                                        shardGeneration,
                                        obsoleteShardGeneration.getKey()
                                    );
                                    assertTrue(repositoryShardState.shardGenerations().remove(shardGeneration));
                                }));
                            }
                        }
                    }
                })

                // complete the context
                .addListener(
                    ActionListener.runBefore(finalizeSnapshotContext, writeRepositoryDataPermits::release),
                    threadPool.generic(),
                    null
                );
        }

        @Override
        public void deleteSnapshots(
            Collection<SnapshotId> snapshotIds,
            long repositoryStateId,
            IndexVersion repositoryIndexVersion,
            SnapshotDeleteListener snapshotDeleteListener
        ) {
            record DeleteResult(RepositoryData updatedRepositoryData, List<Runnable> cleanups) {}

            assertTrue(writeRepositoryDataPermits.tryAcquire()); // TODO will not hold on master failover
            SubscribableListener
                // get current repo data
                .<RepositoryData>newForked(l -> getRepositoryData(threadPool.generic(), l))

                // compute new repository data
                .<DeleteResult>andThen(threadPool.generic(), null, (l, currentRepositoryData) -> {
                    assertEquals(repositoryStateId, currentRepositoryData.getGenId());
                    final var updatedShardGenerations = ShardGenerations.builder();
                    final var currentShardGenerations = currentRepositoryData.shardGenerations();
                    final var cleanups = new ArrayList<Runnable>();
                    for (final var repositoryShardStateEntry : repositoryShardStates.entrySet()) {
                        final var shardId = repositoryShardStateEntry.getKey();
                        final var currentShardGeneration = currentShardGenerations.getShardGen(shardId.index(), shardId.shardId());
                        final ShardGeneration updatedShardGeneration;
                        final Set<ShardGeneration> shardGenerationsToRemove;
                        if (currentShardGeneration == null) {
                            // no successful snapshots of this shard
                            updatedShardGeneration = null;
                            shardGenerationsToRemove = Set.copyOf(repositoryShardStateEntry.getValue().shardGenerations());
                        } else if (randomBoolean()) {
                            // shard generation is unchanged
                            assertThat(
                                shardId + " should have gen " + currentShardGeneration,
                                repositoryShardStateEntry.getValue().shardGenerations(),
                                hasItem(currentShardGeneration)
                            );
                            updatedShardGeneration = currentShardGeneration;
                            shardGenerationsToRemove = Set.of();
                        } else {
                            // new shard generation created
                            updatedShardGeneration = new ShardGeneration(randomAlphaOfLength(10));
                            shardGenerationsToRemove = Set.copyOf(repositoryShardStateEntry.getValue().shardGenerations());
                            assertTrue(repositoryShardStateEntry.getValue().shardGenerations().add(updatedShardGeneration));
                        }
                        updatedShardGenerations.put(shardId.index(), shardId.shardId(), updatedShardGeneration);
                        cleanups.add(() -> repositoryShardStateEntry.getValue().shardGenerations().removeAll(shardGenerationsToRemove));
                    }

                    l.onResponse(new DeleteResult(repositoryData.removeSnapshots(snapshotIds, updatedShardGenerations.build()), cleanups));
                })

                // update repository data
                .<DeleteResult>andThen(
                    threadPool.generic(),
                    null,
                    (l, deleteResult) -> updateRepositoryData(
                        "deleting snapshots " + snapshotIds,
                        deleteResult.updatedRepositoryData(),
                        cs -> cs,
                        l.map(v -> deleteResult)
                    )
                )

                // fork background cleanup
                .<RepositoryData>andThen((l, deleteResult) -> {
                    l.onResponse(deleteResult.updatedRepositoryData());
                    try (var refs = new RefCountingRunnable(snapshotDeleteListener::onDone)) {
                        for (final var snapshotId : snapshotIds) {
                            threadPool.generic()
                                .execute(
                                    ActionRunnable.run(refs.acquireListener(), () -> { assertNotNull(snapshotInfos.remove(snapshotId)); })
                                );
                        }
                        for (final var cleanup : deleteResult.cleanups()) {
                            threadPool.generic().execute(ActionRunnable.run(refs.acquireListener(), cleanup::run));
                        }
                    }
                })

                // complete the context
                .addListener(ActionListener.runBefore(new ActionListener<>() {
                    @Override
                    public void onResponse(RepositoryData repositoryData) {
                        snapshotDeleteListener.onRepositoryDataWritten(repositoryData);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        snapshotDeleteListener.onFailure(e);
                    }
                }, writeRepositoryDataPermits::release), threadPool.generic(), null);
        }

        private void updateRepositoryData(
            String description,
            RepositoryData updatedRepositoryData,
            UnaryOperator<ClusterState> clusterStateOperator,
            ActionListener<Void> listener
        ) {
            masterService.submitUnbatchedStateUpdateTask(description, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assertShardGenerationsPresent();

                    final var newRepositoriesMetadata = RepositoriesMetadata.get(currentState)
                        .withUpdatedGeneration(
                            repositoryMetadata.name(),
                            updatedRepositoryData.getGenId(),
                            updatedRepositoryData.getGenId()
                        );

                    return clusterStateOperator.apply(
                        currentState.copyAndUpdateMetadata(mdb -> mdb.putCustom(RepositoriesMetadata.TYPE, newRepositoriesMetadata))
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    logger.info(
                        "--> update repositoryData [{}]\n{}",
                        description,
                        Strings.toString(
                            (ToXContentObject) (builder, p) -> updatedRepositoryData.snapshotsToXContent(builder, IndexVersion.current()),
                            true,
                            true
                        )
                    );
                    repositoryData = updatedRepositoryData;
                    assertShardGenerationsPresent();
                    listener.onResponse(null);
                }
            });
        }

        void assertShardGenerationsPresent() {
            for (var indexId : repositoryData.shardGenerations().indices()) {
                final var indexShardGenerations = repositoryData.shardGenerations().getGens(indexId);
                for (int i = 0; i < indexShardGenerations.size(); i++) {
                    final var expectedShardGeneration = indexShardGenerations.get(i);
                    if (expectedShardGeneration != null && expectedShardGeneration != ShardGenerations.NEW_SHARD_GEN) {
                        final var repositoryShardId = new RepositoryShardId(indexId, i);
                        assertThat(
                            repositoryShardId.toString(),
                            repositoryShardStates.get(repositoryShardId).shardGenerations(),
                            hasItem(expectedShardGeneration)
                        );
                    }
                }
            }
        }

        void assertShardGenerationsUnique() {
            for (var indexId : repositoryData.shardGenerations().indices()) {
                final var indexShardGenerations = repositoryData.shardGenerations().getGens(indexId);
                for (int i = 0; i < indexShardGenerations.size(); i++) {
                    final var expectedShardGeneration = indexShardGenerations.get(i);
                    final var repositoryShardId = new RepositoryShardId(indexId, i);
                    assertEquals(
                        repositoryShardId.toString(),
                        expectedShardGeneration != null && expectedShardGeneration != ShardGenerations.NEW_SHARD_GEN
                            ? Set.of(expectedShardGeneration)
                            : Set.of(),
                        repositoryShardStates.get(repositoryShardId).shardGenerations()
                    );
                }
            }
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
            return randomAlphaOfLength(10);
        }

        @Override
        public void endVerification(String verificationToken) {}

        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {}

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public void snapshotShard(SnapshotShardContext snapshotShardContext) {
            fail("these tests only look at master-node behaviour, should not be calling this");
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
            listener.onFailure(new UnsupportedOperationException());
        }

        @Override
        public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void updateState(ClusterState state) {
            repositoryMetadata = RepositoriesMetadata.get(state).repository(repositoryMetadata.name());
        }

        @Override
        public void cloneShardSnapshot(
            SnapshotId source,
            SnapshotId target,
            RepositoryShardId shardId,
            ShardGeneration shardGeneration,
            ActionListener<ShardSnapshotResult> listener
        ) {
            threadPool.generic().execute(ActionRunnable.supply(listener, () -> {
                // TODO might fail
                final var shardGenerations = repositoryShardStates.get(shardId).shardGenerations();
                assertThat(shardGenerations, hasItem(shardGeneration));
                final var newGeneration = ShardGeneration.newGeneration(random());
                assertTrue(shardGenerations.add(newGeneration));
                return new ShardSnapshotResult(newGeneration, ByteSizeValue.ZERO, 1);
            }));
        }

        @Override
        public void awaitIdle() {}

        public RepositoryShardState getShardState(RepositoryShardId repositoryShardId) {
            return repositoryShardStates.computeIfAbsent(repositoryShardId, this::newRepositoryShardState);
        }

        private RepositoryShardState newRepositoryShardState(RepositoryShardId ignored) {
            final var shardGenerations = new HashSet<ShardGeneration>();
            shardGenerations.add(ShardGenerations.NEW_SHARD_GEN);
            return new RepositoryShardState(shardGenerations);
        }

        @Override
        public String toString() {
            return FakeRepository.class.getSimpleName();
        }
    }

}
