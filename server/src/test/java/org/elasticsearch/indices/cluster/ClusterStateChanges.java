/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.TransportOpenIndexAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.support.master.TransportMasterNodeActionUtils;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardUpdateTask;
import org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.coordination.JoinReason;
import org.elasticsearch.cluster.coordination.JoinTask;
import org.elasticsearch.cluster.coordination.NodeJoinExecutor;
import org.elasticsearch.cluster.coordination.NodeLeftExecutor;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataVerifier;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.metadata.MetadataIndexStateServiceUtils;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RandomAllocationDeciderTests;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.PublicSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.ShardLimitValidator;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.test.CheckedFunctionUtils.anyCheckedFunction;
import static org.elasticsearch.test.ESTestCase.between;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterStateChanges {
    private static final Settings SETTINGS = Settings.builder().put(PATH_HOME_SETTING.getKey(), "dummy").build();

    private final TransportService transportService;
    private final AllocationService allocationService;
    private final ClusterService clusterService;
    private final ShardStateAction.ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;
    private final ShardStateAction.ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;

    // transport actions
    private final TransportOpenIndexAction transportOpenIndexAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final TransportUpdateSettingsAction transportUpdateSettingsAction;
    private final TransportClusterRerouteAction transportClusterRerouteAction;
    private final TransportCreateIndexAction transportCreateIndexAction;

    private final NodeLeftExecutor nodeLeftExecutor;

    @SuppressWarnings("unchecked")
    public ClusterStateChanges(NamedXContentRegistry xContentRegistry, ThreadPool threadPool) {
        ClusterSettings clusterSettings = createBuiltInClusterSettings(SETTINGS);
        allocationService = new AllocationService(
            new AllocationDeciders(
                new HashSet<>(
                    Arrays.asList(
                        new SameShardAllocationDecider(clusterSettings),
                        new ReplicaAfterPrimaryActiveAllocationDecider(),
                        new RandomAllocationDeciderTests.RandomAllocationDecider(getRandom())
                    )
                )
            ),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(SETTINGS),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        shardFailedClusterStateTaskExecutor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocationService, null);
        shardStartedClusterStateTaskExecutor = new ShardStateAction.ShardStartedClusterStateTaskExecutor(allocationService, null);
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
        DestructiveOperations destructiveOperations = new DestructiveOperations(SETTINGS, clusterSettings);
        Environment environment = TestEnvironment.newEnvironment(SETTINGS);
        Transport transport = mock(Transport.class); // it's not used

        final var masterService = new MasterService(
            SETTINGS,
            clusterSettings,
            threadPool,
            new TaskManager(SETTINGS, threadPool, Collections.emptySet())
        ) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                // run master tasks inline, no need to fork to a separate thread
                return new StoppableExecutorServiceWrapper(EsExecutors.DIRECT_EXECUTOR_SERVICE);
            }
        };
        // mocks
        clusterService = new ClusterService(SETTINGS, clusterSettings, masterService, null);
        resetMasterService();
        masterService.start();

        IndicesService indicesService = mock(IndicesService.class);
        // MetadataCreateIndexService uses withTempIndexService to check mappings -> fake it here
        try {
            when(indicesService.withTempIndexService(any(IndexMetadata.class), anyCheckedFunction())).then(invocationOnMock -> {
                IndexService indexService = mock(IndexService.class);
                IndexMetadata indexMetadata = (IndexMetadata) invocationOnMock.getArguments()[0];
                when(indexService.index()).thenReturn(indexMetadata.getIndex());
                MapperService mapperService = mock(MapperService.class);
                when(indexService.mapperService()).thenReturn(mapperService);
                when(mapperService.documentMapper()).thenReturn(null);
                when(indexService.getIndexEventListener()).thenReturn(new IndexEventListener() {
                });
                when(indexService.getIndexSortSupplier()).thenReturn(() -> null);
                return ((CheckedFunction<IndexService, ?, ?>) invocationOnMock.getArguments()[1]).apply(indexService);
            });
        } catch (Exception e) {
            /*
             * Catch Exception because Eclipse uses the lower bound for
             * CheckedFunction's exception type so it thinks the "when" call
             * can throw Exception. javac seems to be ok inferring something
             * else.
             */
            throw new IllegalStateException(e);
        }

        // services
        transportService = new TransportService(
            SETTINGS,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(SETTINGS, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            clusterSettings,
            Collections.emptySet(),
            Tracer.NOOP
        ) {
            @Override
            public Transport.Connection getConnection(DiscoveryNode node) {
                Transport.Connection conn = mock(Transport.Connection.class);
                when(conn.getTransportVersion()).thenReturn(TransportVersion.current());
                return conn;
            }
        };
        IndexMetadataVerifier indexMetadataVerifier = new IndexMetadataVerifier(
            SETTINGS,
            clusterService,
            xContentRegistry,
            null,
            null,
            null
        ) {
            // metadata upgrader should do nothing
            @Override
            public IndexMetadata verifyIndexMetadata(IndexMetadata indexMetadata, IndexVersion minimumIndexCompatibilityVersion) {
                return indexMetadata;
            }
        };
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        Map<ActionType<? extends ActionResponse>, TransportAction<? extends ActionRequest, ? extends ActionResponse>> actions =
            new HashMap<>();
        actions.put(
            TransportVerifyShardBeforeCloseAction.TYPE,
            new TransportVerifyShardBeforeCloseAction(
                SETTINGS,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                null,
                actionFilters
            )
        );
        client.initialize(
            actions,
            transportService.getTaskManager(),
            null,
            transportService.getLocalNodeConnection(),
            null,
            new NamedWriteableRegistry(List.of())
        );

        ShardLimitValidator shardLimitValidator = new ShardLimitValidator(SETTINGS, clusterService);
        MetadataIndexStateService indexStateService = new MetadataIndexStateService(
            clusterService,
            allocationService,
            indexMetadataVerifier,
            indicesService,
            shardLimitValidator,
            client,
            threadPool
        );
        MetadataDeleteIndexService deleteIndexService = new MetadataDeleteIndexService(SETTINGS, clusterService, allocationService);
        MetadataUpdateSettingsService metadataUpdateSettingsService = new MetadataUpdateSettingsService(
            clusterService,
            allocationService,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            indicesService,
            shardLimitValidator,
            threadPool
        );
        MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(
            SETTINGS,
            clusterService,
            indicesService,
            allocationService,
            shardLimitValidator,
            environment,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            threadPool,
            xContentRegistry,
            EmptySystemIndices.INSTANCE,
            true,
            new IndexSettingProviders(Set.of())
        );

        transportOpenIndexAction = new TransportOpenIndexAction(
            transportService,
            clusterService,
            threadPool,
            indexStateService,
            actionFilters,
            indexNameExpressionResolver,
            destructiveOperations
        );
        transportDeleteIndexAction = new TransportDeleteIndexAction(
            transportService,
            clusterService,
            threadPool,
            deleteIndexService,
            actionFilters,
            indexNameExpressionResolver,
            destructiveOperations
        );
        transportUpdateSettingsAction = new TransportUpdateSettingsAction(
            transportService,
            clusterService,
            threadPool,
            metadataUpdateSettingsService,
            actionFilters,
            indexNameExpressionResolver,
            EmptySystemIndices.INSTANCE
        );
        transportClusterRerouteAction = new TransportClusterRerouteAction(
            transportService,
            clusterService,
            threadPool,
            allocationService,
            actionFilters,
            indexNameExpressionResolver
        );
        transportCreateIndexAction = new TransportCreateIndexAction(
            transportService,
            clusterService,
            threadPool,
            createIndexService,
            actionFilters,
            indexNameExpressionResolver,
            EmptySystemIndices.INSTANCE,
            new PublicSettings.DefaultPublicSettings()
        );

        nodeLeftExecutor = new NodeLeftExecutor(allocationService);
    }

    private void resetMasterService() {
        final var masterService = clusterService.getMasterService();
        masterService.setClusterStateSupplier(() -> { throw new AssertionError("should not be called"); });
        masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            throw new AssertionError("should not be called");
        });
    }

    public ClusterState createIndex(ClusterState state, CreateIndexRequest request) {
        assertSame("simulated cluster state update will never activate any shards", ActiveShardCount.NONE, request.waitForActiveShards());
        return execute(transportCreateIndexAction, request, state);
    }

    public ClusterState closeIndices(ClusterState state, CloseIndexRequest request) {
        final Index[] concreteIndices = Arrays.stream(request.indices())
            .map(index -> state.metadata().index(index).getIndex())
            .toArray(Index[]::new);

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        ClusterState newState = MetadataIndexStateServiceUtils.addIndexClosedBlocks(concreteIndices, blockedIndices, state);

        newState = MetadataIndexStateServiceUtils.closeRoutingTable(
            newState,
            blockedIndices,
            blockedIndices.keySet().stream().collect(toMap(Function.identity(), CloseIndexResponse.IndexResult::new))
        );
        return allocationService.reroute(newState, "indices closed", ActionListener.noop());
    }

    public ClusterState openIndices(ClusterState state, OpenIndexRequest request) {
        assertSame("simulated cluster state update will never activate any shards", ActiveShardCount.NONE, request.waitForActiveShards());
        return execute(transportOpenIndexAction, request, state);
    }

    public ClusterState deleteIndices(ClusterState state, DeleteIndexRequest request) {
        return execute(transportDeleteIndexAction, request, state);
    }

    public ClusterState updateSettings(ClusterState state, UpdateSettingsRequest request) {
        return execute(transportUpdateSettingsAction, request, state);
    }

    public ClusterState reroute(ClusterState state, ClusterRerouteRequest request) {
        return execute(transportClusterRerouteAction, request, state);
    }

    private static final JoinReason DUMMY_REASON = new JoinReason("dummy reason", null);

    public ClusterState addNode(ClusterState clusterState, DiscoveryNode discoveryNode, TransportVersion transportVersion) {
        return runTasks(
            new NodeJoinExecutor(allocationService, (s, p, r) -> {}),
            clusterState,
            List.of(JoinTask.singleNode(discoveryNode, transportVersion, DUMMY_REASON, ActionListener.running(() -> {
                throw new AssertionError("should not complete publication");
            }), clusterState.term()))
        );
    }

    public ClusterState joinNodesAndBecomeMaster(ClusterState clusterState, List<DiscoveryNode> nodes, TransportVersion transportVersion) {
        return runTasks(
            new NodeJoinExecutor(allocationService, (s, p, r) -> {}),
            clusterState,
            List.of(
                JoinTask.completingElection(
                    nodes.stream()
                        .map(node -> new JoinTask.NodeJoinTask(node, transportVersion, DUMMY_REASON, ActionListener.running(() -> {
                            throw new AssertionError("should not complete publication");
                        }))),
                    clusterState.term() + between(1, 10)
                )
            )
        );
    }

    public ClusterState removeNodes(ClusterState clusterState, List<DiscoveryNode> nodes) {
        return runTasks(
            nodeLeftExecutor,
            clusterState,
            nodes.stream().map(n -> new NodeLeftExecutor.Task(n, "dummy reason", () -> {})).toList()
        );
    }

    public ClusterState applyFailedShards(ClusterState clusterState, List<FailedShard> failedShards) {
        List<FailedShardUpdateTask> entries = failedShards.stream()
            .map(
                failedShard -> new FailedShardUpdateTask(
                    new ShardStateAction.FailedShardEntry(
                        failedShard.routingEntry().shardId(),
                        failedShard.routingEntry().allocationId().getId(),
                        0L,
                        failedShard.message(),
                        failedShard.failure(),
                        failedShard.markAsStale()
                    ),
                    createTestListener()
                )
            )
            .toList();
        return runTasks(shardFailedClusterStateTaskExecutor, clusterState, entries);
    }

    public ClusterState applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards) {
        final Map<ShardRouting, Long> entries = startedShards.stream().collect(toMap(Function.identity(), startedShard -> {
            final IndexMetadata indexMetadata = clusterState.metadata().index(startedShard.shardId().getIndex());
            return indexMetadata != null ? indexMetadata.primaryTerm(startedShard.shardId().id()) : 0L;
        }));
        return applyStartedShards(clusterState, entries);
    }

    public ClusterState applyStartedShards(ClusterState clusterState, Map<ShardRouting, Long> startedShards) {
        return runTasks(
            shardStartedClusterStateTaskExecutor,
            clusterState,
            startedShards.entrySet()
                .stream()
                .map(
                    e -> new StartedShardUpdateTask(
                        new StartedShardEntry(
                            e.getKey().shardId(),
                            e.getKey().allocationId().getId(),
                            e.getValue(),
                            "shard started",
                            ShardLongFieldRange.UNKNOWN
                        ),
                        createTestListener()
                    )
                )
                .toList()
        );
    }

    private <T extends ClusterStateTaskListener> ClusterState runTasks(
        ClusterStateTaskExecutor<T> executor,
        ClusterState clusterState,
        List<T> entries
    ) {
        try {
            return ClusterStateTaskExecutorUtils.executeAndThrowFirstFailure(clusterState, executor, entries);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private <Request extends MasterNodeRequest<Request>, Response extends ActionResponse> ClusterState execute(
        TransportMasterNodeAction<Request, Response> masterNodeAction,
        Request request,
        ClusterState clusterState
    ) {
        try {
            final var newClusterStateFuture = new PlainActionFuture<ClusterState>();
            final var masterService = clusterService.getMasterService();
            masterService.setClusterStateSupplier(() -> clusterState);
            masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
                ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
                assertFalse(newClusterStateFuture.isDone());
                newClusterStateFuture.onResponse(clusterStatePublicationEvent.getNewState());
                ackListener.onCommit(TimeValue.ZERO);
                for (final var discoveryNode : clusterStatePublicationEvent.getNewState().nodes()) {
                    ackListener.onNodeAck(discoveryNode, null);
                }
                publishListener.onResponse(null);
            });
            try {
                // if the operation completes without publishing a state then the state must be unchanged
                TransportMasterNodeActionUtils.runMasterOperation(
                    masterNodeAction,
                    request,
                    clusterState,
                    newClusterStateFuture.map(ignored -> clusterState)
                );
                assertTrue("operation should have completed synchronously", newClusterStateFuture.isDone());
                return newClusterStateFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } finally {
            resetMasterService();
        }
    }

    private ActionListener<TransportResponse.Empty> createTestListener() {
        return ActionListener.running(() -> { throw new AssertionError("task should not complete"); });
    }
}
