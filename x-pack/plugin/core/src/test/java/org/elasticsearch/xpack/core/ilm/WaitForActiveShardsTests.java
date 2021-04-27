/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class WaitForActiveShardsTests extends AbstractStepTestCase<WaitForActiveShardsStep> {

    @Override
    public WaitForActiveShardsStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        return new WaitForActiveShardsStep(stepKey, nextStepKey);
    }

    @Override
    public WaitForActiveShardsStep mutateInstance(WaitForActiveShardsStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new WaitForActiveShardsStep(key, nextKey);
    }

    @Override
    public WaitForActiveShardsStep copyInstance(WaitForActiveShardsStep instance) {
        return new WaitForActiveShardsStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testIsConditionMetThrowsExceptionWhenRolloverAliasIsNotSet() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putAlias(AliasMetadata.builder(alias))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        try {
            createRandomInstance().isConditionMet(indexMetadata.getIndex(), clusterState);
            fail("expected the invocation to fail");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), is("setting [" + RolloverAction.LIFECYCLE_ROLLOVER_ALIAS
                + "] is not set on index [" + indexMetadata.getIndex().getName() + "]"));
        }
    }

    public void testResultEvaluatedOnWriteIndexAliasWhenExists() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata originalIndex = IndexMetadata.builder("index-000000")
            .putAlias(AliasMetadata.builder(alias).writeIndex(false))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(1)
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata rolledIndex = IndexMetadata.builder("index-000001")
            .putAlias(AliasMetadata.builder(alias).writeIndex(true))
            .settings(settings(Version.CURRENT)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put("index.write.wait_for_active_shards", "all")
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexRoutingTable.Builder routingTable = new IndexRoutingTable.Builder(rolledIndex.getIndex());
        routingTable.addShard(TestShardRouting.newShardRouting(rolledIndex.getIndex().getName(), 0, "node", null, true,
            ShardRoutingState.STARTED));
        routingTable.addShard(TestShardRouting.newShardRouting(rolledIndex.getIndex().getName(), 0, "node2", null, false,
            ShardRoutingState.STARTED));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(originalIndex, true)
                .put(rolledIndex, true)
                .build())
            .routingTable(RoutingTable.builder().add(routingTable.build()).build())
            .build();

        assertThat("the rolled index has both the primary and the replica shards started so the condition should be met",
            createRandomInstance().isConditionMet(originalIndex.getIndex(), clusterState).isComplete(), is(true));
    }

    public void testResultEvaluatedOnOnlyIndexTheAliasPointsToIfWriteIndexIsNull() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata originalIndex = IndexMetadata.builder("index-000000")
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(1)
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata rolledIndex = IndexMetadata.builder("index-000001")
            .putAlias(AliasMetadata.builder(alias).writeIndex(false))
            .settings(settings(Version.CURRENT)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put("index.write.wait_for_active_shards", "all")
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexRoutingTable.Builder routingTable = new IndexRoutingTable.Builder(rolledIndex.getIndex());
        routingTable.addShard(TestShardRouting.newShardRouting(rolledIndex.getIndex().getName(), 0, "node", null, true,
            ShardRoutingState.STARTED));
        routingTable.addShard(TestShardRouting.newShardRouting(rolledIndex.getIndex().getName(), 0, "node2", null, false,
            ShardRoutingState.STARTED));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(originalIndex, true)
                .put(rolledIndex, true)
                .build())
            .routingTable(RoutingTable.builder().add(routingTable.build()).build())
            .build();

        assertThat("the index the alias is pointing to has both the primary and the replica shards started so the condition should be" +
            " met", createRandomInstance().isConditionMet(originalIndex.getIndex(), clusterState).isComplete(), is(true));
    }

    public void testResultEvaluatedOnDataStream() throws IOException {
        String dataStreamName = "test-datastream";
        IndexMetadata originalIndexMeta = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        IndexMetadata rolledIndexMeta= IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
            .settings(settings(Version.CURRENT).put("index.write.wait_for_active_shards", "3"))
            .numberOfShards(1).numberOfReplicas(3).build();

        IndexRoutingTable.Builder routingTable = new IndexRoutingTable.Builder(rolledIndexMeta.getIndex());
        routingTable.addShard(TestShardRouting.newShardRouting(rolledIndexMeta.getIndex().getName(), 0, "node", null, true,
            ShardRoutingState.STARTED));
        routingTable.addShard(TestShardRouting.newShardRouting(rolledIndexMeta.getIndex().getName(), 0, "node2", null, false,
            ShardRoutingState.STARTED));

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(new DataStream(dataStreamName, createTimestampField("@timestamp"),
                        List.of(originalIndexMeta.getIndex(), rolledIndexMeta.getIndex())))
                    .put(originalIndexMeta, true)
                    .put(rolledIndexMeta, true)
            )
            .routingTable(RoutingTable.builder().add(routingTable.build()).build())
            .build();

        WaitForActiveShardsStep waitForActiveShardsStep = createRandomInstance();

        ClusterStateWaitStep.Result result = waitForActiveShardsStep.isConditionMet(originalIndexMeta.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));

        XContentBuilder expected = new WaitForActiveShardsStep.ActiveShardsInfo(2, "3", false).toXContent(JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS);
        String actualResultAsString = Strings.toString(result.getInfomationContext());
        assertThat(actualResultAsString, is(Strings.toString(expected)));
        assertThat(actualResultAsString, containsString("waiting for [3] shards to become active, but only [2] are active"));
    }

    public void testResultReportsMeaningfulMessage() throws IOException {
        String alias = randomAlphaOfLength(5);
        IndexMetadata originalIndex = IndexMetadata.builder("index-000000")
            .putAlias(AliasMetadata.builder(alias).writeIndex(false))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(1)
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata rolledIndex = IndexMetadata.builder("index-000001")
            .putAlias(AliasMetadata.builder(alias).writeIndex(true))
            .settings(settings(Version.CURRENT)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put("index.write.wait_for_active_shards", "3")
            )
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();
        IndexRoutingTable.Builder routingTable = new IndexRoutingTable.Builder(rolledIndex.getIndex());
        routingTable.addShard(TestShardRouting.newShardRouting(rolledIndex.getIndex().getName(), 0, "node", null, true,
            ShardRoutingState.STARTED));
        routingTable.addShard(TestShardRouting.newShardRouting(rolledIndex.getIndex().getName(), 0, "node2", null, false,
            ShardRoutingState.STARTED));
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(originalIndex, true)
                .put(rolledIndex, true)
                .build())
            .routingTable(RoutingTable.builder().add(routingTable.build()).build())
            .build();

        ClusterStateWaitStep.Result result = createRandomInstance().isConditionMet(originalIndex.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));

        XContentBuilder expected = new WaitForActiveShardsStep.ActiveShardsInfo(2, "3", false).toXContent(JsonXContent.contentBuilder(),
            ToXContent.EMPTY_PARAMS);
        String actualResultAsString = Strings.toString(result.getInfomationContext());
        assertThat(actualResultAsString, is(Strings.toString(expected)));
        assertThat(actualResultAsString, containsString("waiting for [3] shards to become active, but only [2] are active"));
    }

    public void testResultReportsErrorMessage() {
        String alias = randomAlphaOfLength(5);
        IndexMetadata rolledIndex = IndexMetadata.builder("index-000001")
            .putAlias(AliasMetadata.builder(alias).writeIndex(true))
            .settings(settings(Version.CURRENT)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put("index.write.wait_for_active_shards", "3")
            )
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(rolledIndex, true).build())
            .build();

        WaitForActiveShardsStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(new Index("index-000000", UUID.randomUUID().toString()),
            clusterState);
        assertThat(result.isComplete(), is(false));

        String actualResultAsString = Strings.toString(result.getInfomationContext());
        assertThat(actualResultAsString,
            containsString("[" + step.getKey().getAction() + "] lifecycle action for index [index-000000] executed but " +
                "index no longer exists"));
    }
}
