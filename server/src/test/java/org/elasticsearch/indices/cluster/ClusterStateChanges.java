/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.close.TransportCloseIndexAction;
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
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.action.support.master.TransportMasterNodeActionUtils;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.coordination.JoinTaskExecutor;
import org.elasticsearch.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.AliasValidator;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateServiceUtils;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.cluster.metadata.MetaDataUpdateSettingsService;
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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterStateChanges {
    private static final Settings SETTINGS = Settings.builder()
            .put(PATH_HOME_SETTING.getKey(), "dummy")
            .build();

    private static final Logger logger = LogManager.getLogger(ClusterStateChanges.class);
    private final AllocationService allocationService;
    private final ClusterService clusterService;
    private final ShardStateAction.ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;
    private final ShardStateAction.ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;

    // transport actions
    private final TransportCloseIndexAction transportCloseIndexAction;
    private final TransportOpenIndexAction transportOpenIndexAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final TransportUpdateSettingsAction transportUpdateSettingsAction;
    private final TransportClusterRerouteAction transportClusterRerouteAction;
    private final TransportCreateIndexAction transportCreateIndexAction;

    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final JoinTaskExecutor joinTaskExecutor;

    public ClusterStateChanges(NamedXContentRegistry xContentRegistry, ThreadPool threadPool) {
        ClusterSettings clusterSettings = new ClusterSettings(SETTINGS, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allocationService = new AllocationService(new AllocationDeciders(
            new HashSet<>(Arrays.asList(new SameShardAllocationDecider(SETTINGS, clusterSettings),
                new ReplicaAfterPrimaryActiveAllocationDecider(),
                new RandomAllocationDeciderTests.RandomAllocationDecider(getRandom())))),
            new TestGatewayAllocator(), new BalancedShardsAllocator(SETTINGS),
            EmptyClusterInfoService.INSTANCE);
        shardFailedClusterStateTaskExecutor
            = new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocationService, null, logger);
        shardStartedClusterStateTaskExecutor
            = new ShardStateAction.ShardStartedClusterStateTaskExecutor(allocationService, null, logger);
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
        DestructiveOperations destructiveOperations = new DestructiveOperations(SETTINGS, clusterSettings);
        Environment environment = TestEnvironment.newEnvironment(SETTINGS);
        Transport transport = mock(Transport.class); // it's not used

        // mocks
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        IndicesService indicesService = mock(IndicesService.class);
        // MetaDataCreateIndexService creates indices using its IndicesService instance to check mappings -> fake it here
        try {
            @SuppressWarnings("unchecked") final List<IndexEventListener> listeners = anyList();
            when(indicesService.createIndex(any(IndexMetaData.class), listeners))
                .then(invocationOnMock -> {
                    IndexService indexService = mock(IndexService.class);
                    IndexMetaData indexMetaData = (IndexMetaData)invocationOnMock.getArguments()[0];
                    when(indexService.index()).thenReturn(indexMetaData.getIndex());
                    MapperService mapperService = mock(MapperService.class);
                    when(indexService.mapperService()).thenReturn(mapperService);
                    when(mapperService.documentMapper()).thenReturn(null);
                    when(indexService.getIndexEventListener()).thenReturn(new IndexEventListener() {});
                    when(indexService.getIndexSortSupplier()).thenReturn(() -> null);
                    return indexService;
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

        // services
        TransportService transportService = new TransportService(SETTINGS, transport, threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(SETTINGS, boundAddress.publishAddress(), UUIDs.randomBase64UUID()), clusterSettings,
            Collections.emptySet());
        MetaDataIndexUpgradeService metaDataIndexUpgradeService = new MetaDataIndexUpgradeService(SETTINGS, xContentRegistry, null, null,
            null) {
            // metaData upgrader should do nothing
            @Override
            public IndexMetaData upgradeIndexMetaData(IndexMetaData indexMetaData, Version minimumIndexCompatibilityVersion) {
                return indexMetaData;
            }
        };
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        Map<ActionType, TransportAction> actions = new HashMap<>();
        actions.put(TransportVerifyShardBeforeCloseAction.TYPE, new TransportVerifyShardBeforeCloseAction(SETTINGS,
            transportService, clusterService, indicesService, threadPool, null, actionFilters, indexNameExpressionResolver));
        client.initialize(actions, transportService.getTaskManager(), null, null);

        MetaDataIndexStateService indexStateService = new MetaDataIndexStateService(clusterService, allocationService,
            metaDataIndexUpgradeService, indicesService, threadPool, client);
        MetaDataDeleteIndexService deleteIndexService = new MetaDataDeleteIndexService(SETTINGS, clusterService, allocationService);
        MetaDataUpdateSettingsService metaDataUpdateSettingsService = new MetaDataUpdateSettingsService(clusterService,
            allocationService, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, indicesService, threadPool);
        MetaDataCreateIndexService createIndexService = new MetaDataCreateIndexService(SETTINGS, clusterService, indicesService,
            allocationService, new AliasValidator(), environment,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS, threadPool, xContentRegistry, true);

        transportCloseIndexAction = new TransportCloseIndexAction(SETTINGS, transportService, clusterService, threadPool,
            indexStateService, clusterSettings, actionFilters, indexNameExpressionResolver, destructiveOperations);
        transportOpenIndexAction = new TransportOpenIndexAction(transportService,
            clusterService, threadPool, indexStateService, actionFilters, indexNameExpressionResolver, destructiveOperations);
        transportDeleteIndexAction = new TransportDeleteIndexAction(transportService,
            clusterService, threadPool, deleteIndexService, actionFilters, indexNameExpressionResolver, destructiveOperations);
        transportUpdateSettingsAction = new TransportUpdateSettingsAction(
            transportService, clusterService, threadPool, metaDataUpdateSettingsService, actionFilters, indexNameExpressionResolver);
        transportClusterRerouteAction = new TransportClusterRerouteAction(
            transportService, clusterService, threadPool, allocationService, actionFilters, indexNameExpressionResolver);
        transportCreateIndexAction = new TransportCreateIndexAction(
            transportService, clusterService, threadPool, createIndexService, actionFilters, indexNameExpressionResolver);

        nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        joinTaskExecutor = new JoinTaskExecutor(allocationService, logger, (s, p, r) -> {});
    }

    public ClusterState createIndex(ClusterState state, CreateIndexRequest request) {
        return execute(transportCreateIndexAction, request, state);
    }

    public ClusterState closeIndices(ClusterState state, CloseIndexRequest request) {
        final Index[] concreteIndices = Arrays.stream(request.indices())
            .map(index -> state.metaData().index(index).getIndex()).toArray(Index[]::new);

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        ClusterState newState = MetaDataIndexStateServiceUtils.addIndexClosedBlocks(concreteIndices, blockedIndices, state);

        newState = MetaDataIndexStateServiceUtils.closeRoutingTable(newState, blockedIndices,
            blockedIndices.keySet().stream().collect(Collectors.toMap(Function.identity(), CloseIndexResponse.IndexResult::new)));
        return allocationService.reroute(newState, "indices closed");
    }

    public ClusterState openIndices(ClusterState state, OpenIndexRequest request) {
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

    public ClusterState addNodes(ClusterState clusterState, List<DiscoveryNode> nodes) {
        return runTasks(joinTaskExecutor, clusterState, nodes.stream().map(node -> new JoinTaskExecutor.Task(node, "dummy reason"))
            .collect(Collectors.toList()));
    }

    public ClusterState joinNodesAndBecomeMaster(ClusterState clusterState, List<DiscoveryNode> nodes) {
        List<JoinTaskExecutor.Task> joinNodes = new ArrayList<>();
        joinNodes.add(JoinTaskExecutor.newBecomeMasterTask());
        joinNodes.add(JoinTaskExecutor.newFinishElectionTask());
        joinNodes.addAll(nodes.stream().map(node -> new JoinTaskExecutor.Task(node, "dummy reason"))
            .collect(Collectors.toList()));

        return runTasks(joinTaskExecutor, clusterState, joinNodes);
    }

    public ClusterState removeNodes(ClusterState clusterState, List<DiscoveryNode> nodes) {
        return runTasks(nodeRemovalExecutor, clusterState, nodes.stream()
            .map(n -> new NodeRemovalClusterStateTaskExecutor.Task(n, "dummy reason")).collect(Collectors.toList()));
    }

    public ClusterState applyFailedShards(ClusterState clusterState, List<FailedShard> failedShards) {
        List<FailedShardEntry> entries = failedShards.stream().map(failedShard ->
            new FailedShardEntry(failedShard.getRoutingEntry().shardId(), failedShard.getRoutingEntry().allocationId().getId(),
                0L, failedShard.getMessage(), failedShard.getFailure(), failedShard.markAsStale()))
            .collect(Collectors.toList());
        return runTasks(shardFailedClusterStateTaskExecutor, clusterState, entries);
    }

    public ClusterState applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards) {
        final Map<ShardRouting, Long> entries = startedShards.stream()
            .collect(Collectors.toMap(Function.identity(), startedShard -> {
                final IndexMetaData indexMetaData = clusterState.metaData().index(startedShard.shardId().getIndex());
                return indexMetaData != null ? indexMetaData.primaryTerm(startedShard.shardId().id()) : 0L;
            }));
        return applyStartedShards(clusterState, entries);
    }

    public ClusterState applyStartedShards(ClusterState clusterState, Map<ShardRouting, Long> startedShards) {
        return runTasks(shardStartedClusterStateTaskExecutor, clusterState, startedShards.entrySet().stream()
            .map(e -> new StartedShardEntry(e.getKey().shardId(), e.getKey().allocationId().getId(), e.getValue(), "shard started"))
            .collect(Collectors.toList()));
    }

    private <T> ClusterState runTasks(ClusterStateTaskExecutor<T> executor, ClusterState clusterState, List<T> entries) {
        try {
            ClusterTasksResult<T> result = executor.execute(clusterState, entries);
            for (ClusterStateTaskExecutor.TaskResult taskResult : result.executionResults.values()) {
                if (taskResult.isSuccess() == false) {
                    throw taskResult.getFailure();
                }
            }
            return result.resultingState;
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private <Request extends MasterNodeRequest<Request>, Response extends ActionResponse> ClusterState execute(
        TransportMasterNodeAction<Request, Response> masterNodeAction, Request request, ClusterState clusterState) {
        return executeClusterStateUpdateTask(clusterState, () -> {
            try {
                TransportMasterNodeActionUtils.runMasterOperation(masterNodeAction, request, clusterState, new PlainActionFuture<>());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ClusterState executeClusterStateUpdateTask(ClusterState state, Runnable runnable) {
        ClusterState[] result = new ClusterState[1];
        doAnswer(invocationOnMock -> {
            ClusterStateUpdateTask task = (ClusterStateUpdateTask)invocationOnMock.getArguments()[1];
            result[0] = task.execute(state);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        runnable.run();
        assertThat(result[0], notNullValue());
        return result[0];
    }
}
