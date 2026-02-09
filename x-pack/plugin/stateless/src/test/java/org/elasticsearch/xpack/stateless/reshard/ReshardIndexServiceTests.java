/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReshardIndexServiceTests extends ESTestCase {

    public void testSourceStateTransition() throws Exception {
        var numShards = randomIntBetween(1, 10);
        var multiple = 2;
        var numShardsAfter = numShards * multiple;
        var index = new Index("test-index", INDEX_UUID_NA_VALUE);
        var projectId = randomProjectIdOrDefault();

        var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(numShards, multiple);
        var indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), numShardsAfter, 1))
            .numberOfShards(numShardsAfter)
            .reshardingMetadata(reshardingMetadata)
            .build();
        var project = ProjectMetadata.builder(projectId).put(indexMetadata, true).build();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .putRoutingTable(
                projectId,
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build()
            )
            .build();

        var transitionSourceStateExecutor = new ReshardIndexService.TransitionSourceStateExecutor();

        for (var newTargetState : IndexReshardingState.Split.TargetShardState.values()) {
            if (newTargetState == IndexReshardingState.Split.TargetShardState.CLONE) continue;
            for (int sourceId = 0; sourceId < numShards; sourceId++) {
                for (int targetId = sourceId + numShards; targetId < numShards * multiple; targetId += numShards) {
                    clusterState = transitionSplitTargetToNewState(new ShardId(index, targetId), newTargetState, clusterState, projectId);

                    ActionListener<Void> listener = ActionListener.wrap(unused -> {}, ESTestCase::fail);
                    var shardId = new ShardId(index, sourceId);
                    boolean allTargetsDone = targetId + numShards >= numShards * multiple;
                    boolean expectSuccess = newTargetState == IndexReshardingState.Split.TargetShardState.DONE && allTargetsDone;
                    var transitionSourceStateTask = new ReshardIndexService.TransitionSourceStateTask(
                        shardId,
                        IndexReshardingState.Split.SourceShardState.DONE,
                        listener
                    );
                    if (expectSuccess) {
                        clusterState = transitionSourceStateExecutor.executeTask(transitionSourceStateTask, clusterState).v1();
                    } else {
                        var finalClusterState = clusterState;
                        var error = expectThrows(
                            AssertionError.class,
                            () -> transitionSourceStateExecutor.executeTask(transitionSourceStateTask, finalClusterState)
                        );
                        assertThat(error.getMessage(), containsString("can only move source shard to DONE when all targets are DONE"));
                    }
                }
            }
        }
    }

    public void testMaybeAwaitSplitDoesNotThrow() {
        final var indicesService = mock(IndicesService.class);

        final var svc = new ReshardIndexService(mock(ClusterService.class), null, null, indicesService);

        final var badIndex = new Index("badindex", INDEX_UUID_NA_VALUE);
        final var badIndexShard = new ShardId(badIndex, 0);
        when(indicesService.indexServiceSafe(eq(badIndex))).thenThrow(new IndexNotFoundException(badIndex));
        svc.maybeAwaitSplit(badIndexShard, ActionListener.wrap(ignored -> fail("should have failed"), e -> {
            assertThat(e, instanceOf(IndexNotFoundException.class));
        }));

        final var index = new Index("index", INDEX_UUID_NA_VALUE);
        final var indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(eq(index))).thenReturn(indexService);

        final var badShard = new ShardId(index, 1);
        when(indexService.getShard(1)).thenThrow(new ShardNotFoundException(badShard));
        svc.maybeAwaitSplit(badShard, ActionListener.wrap(ignored -> fail("should have failed"), e -> {
            assertThat(e, instanceOf(ShardNotFoundException.class));
        }));
    }

    public void testMaybeAwaitSplit() throws InterruptedException {
        final var svc = new ReshardIndexService(mock(ClusterService.class), null, null, mock(IndicesService.class));
        final var index = new Index("index", INDEX_UUID_NA_VALUE);
        final var sourceShard = new ShardId(index, 0);
        final var targetShard = new ShardId(index, 1);

        final ActionListener<Void> failOnFailure = ActionListener.wrap(ignored -> {}, e -> fail("should not have failed"));

        // no split in progress should directly complete listener
        svc.maybeAwaitSplit(null, sourceShard, failOnFailure);

        final var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(1, 2);
        // source shard never waits
        svc.maybeAwaitSplit(reshardingMetadata, sourceShard, failOnFailure);

        // target shard does not wait if it is already done
        final var targetDoneMetadata = reshardingMetadata.transitionSplitTargetToNewState(
            targetShard,
            IndexReshardingState.Split.TargetShardState.HANDOFF
        )
            .transitionSplitTargetToNewState(targetShard, IndexReshardingState.Split.TargetShardState.SPLIT)
            .transitionSplitTargetToNewState(targetShard, IndexReshardingState.Split.TargetShardState.DONE);
        svc.maybeAwaitSplit(targetDoneMetadata, targetShard, failOnFailure);

        // target shard waits if split completion is not yet done
        final var completed = new AtomicBoolean();
        final var completedLatch = new CountDownLatch(1);
        final ActionListener<Void> waitingListener = ActionListener.wrap(ignored -> {
            completed.set(true);
            completedLatch.countDown();
        }, e -> fail("should not have failed"));
        svc.maybeAwaitSplit(reshardingMetadata, targetShard, waitingListener);
        assertFalse(completed.get());

        // and is notified when split completes
        svc.notifySplitCompletion(targetShard);
        assertTrue(completedLatch.await(SAFE_AWAIT_TIMEOUT.getMillis(), TimeUnit.MILLISECONDS));

        // after completion, new listeners complete directly
        svc.maybeAwaitSplit(reshardingMetadata, targetShard, failOnFailure);

        // failure propagates to listeners
        final ActionListener<Void> failingListener = ActionListener.wrap(ignored -> fail("should have failed"), e -> {
            assertEquals("split failed", e.getMessage());
        });
        svc.notifySplitFailure(targetShard, new Exception("split failed"));
        svc.maybeAwaitSplit(reshardingMetadata, targetShard, failingListener);

        // a failed split can later succeed
        svc.notifySplitCompletion(targetShard);
        svc.maybeAwaitSplit(reshardingMetadata, targetShard, failOnFailure);
    }

    private ClusterState transitionSplitTargetToNewState(
        ShardId shardId,
        IndexReshardingState.Split.TargetShardState newTargetState,
        ClusterState clusterState,
        ProjectId projectId
    ) {
        var projectMetadata = clusterState.metadata().getProject(projectId);
        var reshardingMetadata = projectMetadata.index(shardId.getIndex())
            .getReshardingMetadata()
            .transitionSplitTargetToNewState(shardId, newTargetState);
        var indexMetadata = IndexMetadata.builder(projectMetadata.index(shardId.getIndex())).reshardingMetadata(reshardingMetadata).build();
        var updatedProject = ProjectMetadata.builder(projectMetadata).put(indexMetadata, true).build();
        return ClusterState.builder(clusterState).putProjectMetadata(updatedProject).build();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testWillNotReshardInvalidIndexVersion() throws Exception {
        var projectId = randomProjectIdOrDefault();
        var index = new Index("test-index", INDEX_UUID_NA_VALUE);
        var priorVersion = IndexVersion.fromId(
            randomIntBetween(IndexVersions.FIRST_DETACHED_INDEX_VERSION.id(), IndexVersions.MOD_ROUTING_FUNCTION.id() - 1)
        );
        var indexMetadata = IndexMetadata.builder(index.getName()).settings(indexSettings(priorVersion, 1, 0)).build();
        var project = ProjectMetadata.builder(projectId).put(indexMetadata, true).build();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(project)
            .putRoutingTable(
                projectId,
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build()
            )
            .build();

        var clusterService = mock(ClusterService.class);
        ArgumentCaptor<SimpleBatchedExecutor> executorCaptor = ArgumentCaptor.forClass(SimpleBatchedExecutor.class);
        ArgumentCaptor<ClusterStateTaskListener> taskCaptor = ArgumentCaptor.forClass(ClusterStateTaskListener.class);
        MasterServiceTaskQueue mockQueue = mock(MasterServiceTaskQueue.class);
        when(clusterService.createTaskQueue(eq("reshard-index"), any(), executorCaptor.capture())).thenReturn(mockQueue);
        // Return mock queues for other task queues created in constructor
        when(clusterService.createTaskQueue(argThat(name -> "reshard-index".equals(name) == false), any(), any())).thenReturn(mockQueue);

        var svc = new ReshardIndexService(
            clusterService,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            null,
            mock(IndicesService.class)
        );

        var request = new ReshardIndexClusterStateUpdateRequest(projectId, index, -1);
        svc.reshardIndex(TimeValue.THIRTY_SECONDS, request, ActionListener.noop());

        // Capture the task that was submitted
        verify(mockQueue).submitTask(eq("reshard-index [test-index]"), taskCaptor.capture(), eq(TimeValue.THIRTY_SECONDS));

        // Execute the captured task through the captured executor with our cluster state
        SimpleBatchedExecutor executor = executorCaptor.getValue();
        ClusterStateTaskListener task = taskCaptor.getValue();

        var exception = expectThrows(IllegalArgumentException.class, () -> executor.executeTask(task, clusterState));
        assertThat(
            exception.getMessage(),
            containsString("resharding a index [" + index + "] with a version prior to " + IndexVersions.MOD_ROUTING_FUNCTION)
        );
    }
}
