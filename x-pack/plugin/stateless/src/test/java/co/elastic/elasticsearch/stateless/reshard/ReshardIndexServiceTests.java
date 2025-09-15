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

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.hamcrest.Matchers.containsString;

public class ReshardIndexServiceTests extends ESTestCase {

    public void testSourceStateTransition() throws Exception {
        var numShards = randomIntBetween(1, 10);
        var multiple = 2;
        var numShardsAfter = numShards * multiple;
        var index = new Index("test-index", INDEX_UUID_NA_VALUE);

        var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(numShards, multiple);
        var indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), numShardsAfter, 1))
            .numberOfShards(numShardsAfter)
            .reshardingMetadata(reshardingMetadata)
            .build();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata).build())
            .build();

        var transitionSourceStateExecutor = new ReshardIndexService.TransitionSourceStateExecutor();

        for (var newTargetState : IndexReshardingState.Split.TargetShardState.values()) {
            if (newTargetState == IndexReshardingState.Split.TargetShardState.CLONE) continue;
            for (int sourceId = 0; sourceId < numShards; sourceId++) {
                for (int targetId = sourceId + numShards; targetId < numShards * multiple; targetId += numShards) {
                    clusterState = transitionSplitTargetToNewState(new ShardId(index, targetId), newTargetState, clusterState);

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

    private ClusterState transitionSplitTargetToNewState(
        ShardId shardId,
        IndexReshardingState.Split.TargetShardState newTargetState,
        ClusterState clusterState
    ) {
        var reshardingMetadata = clusterState.projectState()
            .metadata()
            .index(shardId.getIndexName())
            .getReshardingMetadata()
            .transitionSplitTargetToNewState(shardId, newTargetState);
        var metadata = clusterState.metadata();
        var indexMetadata = IndexMetadata.builder(metadata.indexMetadata(shardId.getIndex()))
            .reshardingMetadata(reshardingMetadata)
            .build();
        return ClusterState.builder(clusterState).metadata(Metadata.builder(metadata).put(indexMetadata, true)).build();
    }
}
