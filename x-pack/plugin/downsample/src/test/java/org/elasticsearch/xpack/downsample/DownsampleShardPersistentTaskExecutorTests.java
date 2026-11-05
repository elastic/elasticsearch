/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.downsample.DownsampleShardIndexerStatus;
import org.elasticsearch.xpack.core.downsample.DownsampleShardPersistentTaskState;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.xpack.downsample.DownsampleActionSingleNodeTests.randomSamplingMethod;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class DownsampleShardPersistentTaskExecutorTests extends ESTestCase {

    private ProjectId projectId;
    private ClusterState initialClusterState;
    private DownsampleShardPersistentTaskExecutor executor;
    // A real client needs a running node. The executor only hands it the local delegating action, and must never hand it a search,
    // so a mock that records what it was given is enough.
    private Client client;
    private ThreadPool threadPool;
    // The executor reads the target index from the cluster state to bind the resume watermark to it, so it needs a real cluster
    // service holding a state with the target index.
    private ClusterService clusterService;
    private String targetIndexUuid;

    @Before
    public void setup() {
        projectId = randomProjectIdOrDefault();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Instant start = now.minus(2, ChronoUnit.HOURS);
        Instant end = now.plus(40, ChronoUnit.MINUTES);
        initialClusterState = DataStreamTestHelper.getClusterStateWithDataStream(
            projectId,
            "metrics-app1",
            List.of(new Tuple<>(start, end))
        );
        targetIndexUuid = UUIDs.randomBase64UUID();
        var withTargetIndex = ClusterState.builder(initialClusterState)
            .putProjectMetadata(
                ProjectMetadata.builder(initialClusterState.metadata().getProject(projectId))
                    .put(
                        IndexMetadata.builder(targetIndex()).settings(indexSettings(IndexVersion.current(), targetIndexUuid, 1, 0)).build(),
                        false
                    )
            )
            // Without any node version the cluster state reports the minimum compatible transport version, as in a mixed cluster
            .putCompatibilityVersions("node", TransportVersion.current(), Map.of())
            .build();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(withTargetIndex, threadPool);
        client = mock(Client.class);
        executor = new DownsampleShardPersistentTaskExecutor(client, clusterService, DownsampleShardTask.TASK_NAME, mock(Executor.class));
    }

    @After
    public void cleanup() {
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * A task that already ran published the watermark it reached in its state. When it is reassigned it must resume from there,
     * and never from the target index, which may hold documents past the watermark whose predecessors were never written.
     */
    public void testNodeOperationResumesFromPublishedState() {
        BytesRef watermark = new BytesRef(randomByteArrayOfLength(16));
        var state = new DownsampleShardPersistentTaskState(
            randomFrom(DownsampleShardIndexerStatus.STARTED, DownsampleShardIndexerStatus.FAILED),
            watermark,
            targetIndexUuid
        );
        var params = params();
        executor.nodeOperation(shardTask(params), params, state);

        verify(client, never()).search(any(), any());
        var initialState = delegatedInitialState();
        assertThat(initialState.tsid(), equalTo(watermark));
        assertThat(initialState.targetIndexUuid(), equalTo(targetIndexUuid));
    }

    /**
     * A task without a state never ran. It starts from the beginning of the shard, even if the target index is already partially
     * populated, because the target alone cannot tell which of its documents have all their predecessors. The state it starts
     * with is bound to the current target index, so that the watermarks it publishes are too.
     */
    public void testNodeOperationStartsFreshTaskFromTheBeginning() {
        var params = params();
        var state = randomBoolean()
            ? null
            : new DownsampleShardPersistentTaskState(
                randomFrom(DownsampleShardIndexerStatus.STARTED, DownsampleShardIndexerStatus.FAILED),
                null,
                randomBoolean() ? targetIndexUuid : null
            );
        executor.nodeOperation(shardTask(params), params, state);

        verify(client, never()).search(any(), any());
        var initialState = delegatedInitialState();
        assertThat(initialState.tsid(), nullValue());
        assertThat(initialState.targetIndexUuid(), equalTo(targetIndexUuid));
    }

    /**
     * A watermark is only valid for the target index it was reached on. If the target was re-created under the same name, or the
     * watermark was published by a node that did not record the target, the task starts from the beginning of the shard.
     */
    public void testNodeOperationIgnoresWatermarkOfAnotherTargetIndex() {
        var state = new DownsampleShardPersistentTaskState(
            randomFrom(DownsampleShardIndexerStatus.STARTED, DownsampleShardIndexerStatus.FAILED),
            new BytesRef(randomByteArrayOfLength(16)),
            randomBoolean() ? null : UUIDs.randomBase64UUID()
        );
        var params = params();
        executor.nodeOperation(shardTask(params), params, state);

        var initialState = delegatedInitialState();
        assertThat(initialState.tsid(), nullValue());
        assertThat(initialState.targetIndexUuid(), equalTo(targetIndexUuid));
    }

    /**
     * While the cluster has nodes that do not know the target index UUID, a master among them would persist a watermark in a form
     * this version cannot read back. The task then neither resumes from a watermark nor publishes one, which the missing UUID in
     * its initial state tells the indexer.
     */
    public void testNodeOperationPublishesNoWatermarkInMixedCluster() {
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state())
                .putCompatibilityVersions(
                    "old-node",
                    TransportVersionUtils.getPreviousVersion(DownsampleShardPersistentTaskState.DOWNSAMPLE_RESUME_WATERMARK_TARGET_UUID),
                    Map.of()
                )
                .build()
        );
        var state = randomBoolean()
            ? null
            : new DownsampleShardPersistentTaskState(
                randomFrom(DownsampleShardIndexerStatus.STARTED, DownsampleShardIndexerStatus.FAILED),
                new BytesRef(randomByteArrayOfLength(16)),
                targetIndexUuid
            );
        var params = params();
        executor.nodeOperation(shardTask(params), params, state);

        var initialState = delegatedInitialState();
        assertThat(initialState.tsid(), nullValue());
        assertThat(initialState.targetIndexUuid(), nullValue());
    }

    /**
     * Without a target index the task is about to fail. It must still not resume from a watermark it cannot bind to an index.
     */
    public void testNodeOperationIgnoresWatermarkWhenTargetIndexIsMissing() {
        ClusterServiceUtils.setState(clusterService, initialClusterState);
        var state = new DownsampleShardPersistentTaskState(
            randomFrom(DownsampleShardIndexerStatus.STARTED, DownsampleShardIndexerStatus.FAILED),
            new BytesRef(randomByteArrayOfLength(16)),
            targetIndexUuid
        );
        var params = params();
        executor.nodeOperation(shardTask(params), params, state);

        var initialState = delegatedInitialState();
        assertThat(initialState.tsid(), nullValue());
        assertThat(initialState.targetIndexUuid(), nullValue());
    }

    private DownsampleShardPersistentTaskState delegatedInitialState() {
        var request = ArgumentCaptor.forClass(DownsampleShardPersistentTaskExecutor.DelegatingAction.Request.class);
        verify(client).execute(eq(DownsampleShardPersistentTaskExecutor.DelegatingAction.INSTANCE), request.capture(), any());
        var initialState = request.getValue().initialState();
        assertThat(initialState.downsampleShardIndexerStatus(), equalTo(DownsampleShardIndexerStatus.INITIALIZED));
        return initialState;
    }

    private String targetIndex() {
        var backingIndex = initialClusterState.metadata().getProject(projectId).dataStreams().get("metrics-app1").getWriteIndex();
        return "downsample-" + backingIndex.getName();
    }

    private DownsampleShardTaskParams params() {
        var backingIndex = initialClusterState.metadata().getProject(projectId).dataStreams().get("metrics-app1").getWriteIndex();
        return new DownsampleShardTaskParams(
            new DownsampleConfig(new DateHistogramInterval("1h"), randomSamplingMethod()),
            targetIndex(),
            1,
            1,
            new ShardId(backingIndex, 0),
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Map.of()
        );
    }

    private static DownsampleShardTask shardTask(DownsampleShardTaskParams params) {
        return new DownsampleShardTask(
            randomNonNegativeLong(),
            "type",
            "action",
            TaskId.EMPTY_TASK_ID,
            params.downsampleIndex(),
            params.indexStartTimeMillis(),
            params.indexEndTimeMillis(),
            params.downsampleConfig(),
            Map.of(),
            params.shardId()
        );
    }

    public void testGetAssignment() {
        var backingIndex = initialClusterState.metadata().getProject(projectId).dataStreams().get("metrics-app1").getWriteIndex();
        var nodeWithPrimary = newNode();
        var nodeWithReplica = newNode();
        var otherNode = newNode();
        var shardId = new ShardId(backingIndex, 0);
        var clusterState = ClusterState.builder(initialClusterState)
            .nodes(new DiscoveryNodes.Builder().add(nodeWithPrimary).add(nodeWithReplica).add(otherNode).build())
            .putRoutingTable(
                projectId,
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(backingIndex)
                            .addShard(shardRoutingBuilder(shardId, nodeWithPrimary.getId(), true, STARTED).withRecoverySource(null).build())
                            .addShard(
                                shardRoutingBuilder(shardId, nodeWithReplica.getId(), false, STARTED).withRecoverySource(null).build()
                            )
                    )
                    .build()
            )
            .build();

        var params = new DownsampleShardTaskParams(
            new DownsampleConfig(new DateHistogramInterval("1h"), randomSamplingMethod()),
            shardId.getIndexName(),
            1,
            1,
            shardId,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Map.of()
        );
        var result = executor.getAssignment(params, Set.of(nodeWithPrimary, nodeWithReplica, otherNode), clusterState, projectId);
        assertThat(result.getExecutorNode(), anyOf(equalTo(nodeWithPrimary.getId()), equalTo(nodeWithReplica.getId())));
    }

    public void testAssignLeastLoadedNode() {
        var backingIndex = initialClusterState.metadata().getProject(projectId).dataStreams().get("metrics-app1").getWriteIndex();
        var leastLoadedNodeWithShard = newNode();
        var loadedNodeWithShard = newNode();
        var otherNode = newNode();
        var shardId = new ShardId(backingIndex, 0);
        ClusterPersistentTasksCustomMetadata.Builder persistentTaskBuilder = ClusterPersistentTasksCustomMetadata.builder();
        addAssignedPersistedTasks(persistentTaskBuilder, loadedNodeWithShard.getId(), randomIntBetween(3, 5));
        addAssignedPersistedTasks(persistentTaskBuilder, leastLoadedNodeWithShard.getId(), randomIntBetween(0, 2));
        var clusterState = ClusterState.builder(initialClusterState)
            .metadata(
                Metadata.builder(initialClusterState.metadata())
                    .putCustom(ClusterPersistentTasksCustomMetadata.TYPE, persistentTaskBuilder.build())
            )
            .nodes(new DiscoveryNodes.Builder().add(leastLoadedNodeWithShard).add(loadedNodeWithShard).add(otherNode).build())
            .putRoutingTable(
                projectId,
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(backingIndex)
                            .addShard(
                                shardRoutingBuilder(shardId, leastLoadedNodeWithShard.getId(), true, STARTED).withRecoverySource(null)
                                    .build()
                            )
                            .addShard(
                                shardRoutingBuilder(shardId, loadedNodeWithShard.getId(), false, STARTED).withRecoverySource(null).build()
                            )
                    )
                    .build()
            )
            .build();

        var params = new DownsampleShardTaskParams(
            new DownsampleConfig(new DateHistogramInterval("1h"), randomSamplingMethod()),
            shardId.getIndexName(),
            1,
            1,
            shardId,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Map.of()
        );
        var result = executor.getAssignment(
            params,
            Set.of(leastLoadedNodeWithShard, loadedNodeWithShard, otherNode),
            clusterState,
            projectId
        );
        assertThat(result.getExecutorNode(), equalTo(leastLoadedNodeWithShard.getId()));
    }

    public void testGetAssignmentMissingIndex() {
        var backingIndex = initialClusterState.metadata().getProject(projectId).dataStreams().get("metrics-app1").getWriteIndex();
        var node = newNode();
        var shardId = new ShardId(backingIndex, 0);
        var clusterState = ClusterState.builder(initialClusterState)
            .nodes(new DiscoveryNodes.Builder().add(node).build())
            .putRoutingTable(
                projectId,
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(backingIndex)
                            .addShard(shardRoutingBuilder(shardId, node.getId(), true, STARTED).withRecoverySource(null).build())
                    )
                    .build()
            )
            .build();

        var missingShardId = new ShardId(new Index("another_index", "uid"), 0);
        var params = new DownsampleShardTaskParams(
            new DownsampleConfig(new DateHistogramInterval("1h"), randomSamplingMethod()),
            missingShardId.getIndexName(),
            1,
            1,
            missingShardId,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Map.of()
        );
        var result = executor.getAssignment(params, Set.of(node), clusterState, projectId);
        assertThat(result.getExecutorNode(), equalTo(node.getId()));
        assertThat(result.getExplanation(), equalTo("a node to fail and stop this persistent task"));
    }

    public void testNoPossibleAssignment() {
        var backingIndex = initialClusterState.metadata().getProject(projectId).dataStreams().get("metrics-app1").getWriteIndex();
        var nodeWithPrimary = newNode();
        var nodeWithReplica = newNode();
        var otherNode = newNode();
        var shardId = new ShardId(backingIndex, 0);
        var clusterState = ClusterState.builder(initialClusterState)
            .nodes(new DiscoveryNodes.Builder().add(nodeWithPrimary).add(nodeWithReplica).add(otherNode).build())
            .putRoutingTable(
                projectId,
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(backingIndex)
                            .addShard(shardRoutingBuilder(shardId, nodeWithPrimary.getId(), true, STARTED).withRecoverySource(null).build())
                            .addShard(
                                shardRoutingBuilder(shardId, nodeWithReplica.getId(), false, STARTED).withRecoverySource(null).build()
                            )
                    )
                    .build()
            )
            .build();

        var params = new DownsampleShardTaskParams(
            new DownsampleConfig(new DateHistogramInterval("1h"), randomSamplingMethod()),
            shardId.getIndexName(),
            1,
            1,
            shardId,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Map.of()
        );
        var result = executor.getAssignment(params, Set.of(otherNode), clusterState, projectId);
        assertThat(result.getExecutorNode(), nullValue());
    }

    public void testOnlySearchableShardsAreConsidered() {
        var backingIndex = initialClusterState.metadata().getProject(projectId).dataStreams().get("metrics-app1").getWriteIndex();
        var searchNode = newNode(Set.of(DiscoveryNodeRole.SEARCH_ROLE));
        var indexNode = newNode(Set.of(DiscoveryNodeRole.INDEX_ROLE));
        var shardId = new ShardId(backingIndex, 0);
        ShardRouting indexOnlyShard = shardRoutingBuilder(shardId, indexNode.getId(), true, STARTED).withRecoverySource(null)
            .withRole(ShardRouting.Role.INDEX_ONLY)
            .build();
        var clusterState = ClusterState.builder(initialClusterState)
            .nodes(new DiscoveryNodes.Builder().add(indexNode).add(searchNode).build())
            .putRoutingTable(
                projectId,
                RoutingTable.builder().add(IndexRoutingTable.builder(backingIndex).addShard(indexOnlyShard)).build()
            )
            .build();

        var params = new DownsampleShardTaskParams(
            new DownsampleConfig(new DateHistogramInterval("1h"), randomSamplingMethod()),
            shardId.getIndexName(),
            1,
            1,
            shardId,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Map.of()
        );
        var result = executor.getAssignment(params, Set.of(indexNode, searchNode), clusterState, projectId);
        assertThat(result.getExecutorNode(), nullValue());

        // Assign a copy of the shard to a search node
        clusterState = ClusterState.builder(clusterState)
            .putRoutingTable(
                projectId,
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(backingIndex)
                            .addShard(indexOnlyShard)
                            .addShard(
                                shardRoutingBuilder(shardId, searchNode.getId(), false, STARTED).withRecoverySource(null)
                                    .withRole(ShardRouting.Role.SEARCH_ONLY)
                                    .build()
                            )
                    )
                    .build()
            )
            .build();
        result = executor.getAssignment(params, Set.of(indexNode, searchNode), clusterState, projectId);
        assertThat(result.getExecutorNode(), equalTo(searchNode.getId()));
    }

    private static DiscoveryNode newNode() {
        return newNode(DiscoveryNodeRole.roles());
    }

    private static DiscoveryNode newNode(Set<DiscoveryNodeRole> nodes) {
        return DiscoveryNodeUtils.create(
            "node_" + UUIDs.randomBase64UUID(random()),
            buildNewFakeTransportAddress(),
            Map.of(),
            DiscoveryNodeRole.roles()
        );
    }

    private static void addAssignedPersistedTasks(ClusterPersistentTasksCustomMetadata.Builder builder, String nodeId, int count) {
        Index index = new Index(randomAlphaOfLength(10), randomUUID());
        for (int i = 0; i < count; i++) {
            var taskId = DownsampleShardTask.TASK_NAME + "_" + index.getName() + "_" + i;
            var params = new DownsampleShardTaskParams(
                new DownsampleConfig(new DateHistogramInterval("1h"), randomSamplingMethod()),
                "downsample-" + index.getName(),
                1,
                1,
                new ShardId(index, i),
                Strings.EMPTY_ARRAY,
                Strings.EMPTY_ARRAY,
                Strings.EMPTY_ARRAY,
                Map.of()
            );
            builder.addTask(
                taskId,
                DownsampleShardTask.TASK_NAME,
                params,
                new PersistentTasksCustomMetadata.Assignment(nodeId, "test assignment")
            ).updateTaskState(taskId, new DownsampleShardPersistentTaskState(DownsampleShardIndexerStatus.INITIALIZED, null));
        }
    }

}
