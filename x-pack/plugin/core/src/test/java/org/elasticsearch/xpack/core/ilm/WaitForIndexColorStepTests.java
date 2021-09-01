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
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsNull.notNullValue;

public class WaitForIndexColorStepTests extends AbstractStepTestCase<WaitForIndexColorStep> {

    private static ClusterHealthStatus randomColor() {
        String[] colors = new String[]{"green", "yellow", "red"};
        int randomColor = randomIntBetween(0, colors.length - 1);
        return ClusterHealthStatus.fromString(colors[randomColor]);
    }

    @Override
    protected WaitForIndexColorStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        ClusterHealthStatus color = randomColor();
        String indexPrefix = randomAlphaOfLengthBetween(1, 10);
        return new WaitForIndexColorStep(stepKey, nextStepKey, color, indexPrefix);
    }

    @Override
    protected WaitForIndexColorStep mutateInstance(WaitForIndexColorStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        ClusterHealthStatus color = instance.getColor(), newColor = randomColor();
        String indexPrefix = instance.getIndexNamePrefix();

        while (color.equals(newColor)) {
            newColor = randomColor();
        }

        switch (between(0, 2)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                color = newColor;
                break;
            case 3:
                indexPrefix = randomAlphaOfLengthBetween(1, 10);
                break;
        }

        return new WaitForIndexColorStep(key, nextKey, color, indexPrefix);
    }

    @Override
    protected WaitForIndexColorStep copyInstance(WaitForIndexColorStep instance) {
        return new WaitForIndexColorStep(instance.getKey(), instance.getNextStepKey(), instance.getColor(), instance.getIndexNamePrefix());
    }

    public void testConditionMetForGreen() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("test_index", 0, "1", true, ShardRoutingState.STARTED);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.GREEN);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionNotMetForGreen() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("test_index", 0, "1", true, ShardRoutingState.INITIALIZING);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.GREEN);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexColorStep.Info info = (WaitForIndexColorStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is not green; not all shards are active"));
    }

    public void testConditionNotMetNoIndexRoutingTable() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.YELLOW);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexColorStep.Info info = (WaitForIndexColorStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is red; no indexRoutingTable"));
    }

    public void testConditionMetForYellow() {
        IndexMetadata indexMetadata = IndexMetadata.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("index2", 0, "1", true, ShardRoutingState.STARTED);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.YELLOW);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionNotMetForYellow() {
        IndexMetadata indexMetadata = IndexMetadata.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("index2", 0, "1", true, ShardRoutingState.INITIALIZING);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.YELLOW);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexColorStep.Info info = (WaitForIndexColorStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is red; not all primary shards are active"));
    }

    public void testConditionNotMetNoIndexRoutingTableForYellow() {
        IndexMetadata indexMetadata = IndexMetadata.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.YELLOW);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexColorStep.Info info = (WaitForIndexColorStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is red; no indexRoutingTable"));
    }

    public void testStepReturnsFalseIfTargetIndexIsMissing() {
        IndexMetadata originalIndex = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();

        String indexPrefix = randomAlphaOfLengthBetween(5, 10) + "-";
        ShardRouting shardRouting =
            TestShardRouting.newShardRouting(originalIndex.getIndex().getName(), 0, "1", true, ShardRoutingState.STARTED);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(originalIndex.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(originalIndex, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.GREEN, indexPrefix);
        ClusterStateWaitStep.Result result = step.isConditionMet(originalIndex.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexColorStep.Info info = (WaitForIndexColorStep.Info) result.getInfomationContext();
        String targetIndex = indexPrefix + originalIndex.getIndex().getName();
        assertThat(info.getMessage(), is("[" + step.getKey().getAction() + "] lifecycle action for index [" +
            originalIndex.getIndex().getName() + "] executed but the target index [" + targetIndex + "] does not exist"));
    }

    public void testStepWaitsForTargetIndexHealthWhenPrefixConfigured() {
        IndexMetadata originalIndex = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();
        ShardRouting originalShardRouting =
            TestShardRouting.newShardRouting(originalIndex.getIndex().getName(), 0, "1", true, ShardRoutingState.STARTED);
        IndexRoutingTable originalIndexRoutingTable = IndexRoutingTable.builder(originalIndex.getIndex())
            .addShard(originalShardRouting).build();

        String indexPrefix = randomAlphaOfLengthBetween(5, 10) + "-";
        String targetIndexName = indexPrefix + originalIndex.getIndex().getName();
        IndexMetadata targetIndex = IndexMetadata.builder(targetIndexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();

        {
            ShardRouting targetShardRouting =
                TestShardRouting.newShardRouting(targetIndexName, 0, "1", true, ShardRoutingState.INITIALIZING);
            IndexRoutingTable targetIndexRoutingTable = IndexRoutingTable.builder(originalIndex.getIndex())
                .addShard(targetShardRouting).build();

            ClusterState clusterTargetInitializing = ClusterState.builder(new ClusterName("_name"))
                .metadata(Metadata.builder().put(originalIndex, true).put(targetIndex, true).build())
                .routingTable(RoutingTable.builder().add(originalIndexRoutingTable).add(targetIndexRoutingTable).build())
                .build();

            WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.GREEN);
            ClusterStateWaitStep.Result result = step.isConditionMet(originalIndex.getIndex(), clusterTargetInitializing);
            assertThat(result.isComplete(), is(false));
            WaitForIndexColorStep.Info info = (WaitForIndexColorStep.Info) result.getInfomationContext();
            assertThat(info.getMessage(), is("index is not green; not all shards are active"));
        }

        {
            ShardRouting targetShardRouting =
                TestShardRouting.newShardRouting(targetIndexName, 0, "1", true, ShardRoutingState.STARTED);
            IndexRoutingTable targetIndexRoutingTable = IndexRoutingTable.builder(originalIndex.getIndex())
                .addShard(targetShardRouting).build();

            ClusterState clusterTargetInitializing = ClusterState.builder(new ClusterName("_name"))
                .metadata(Metadata.builder().put(originalIndex, true).put(targetIndex, true).build())
                .routingTable(RoutingTable.builder().add(originalIndexRoutingTable).add(targetIndexRoutingTable).build())
                .build();

            WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.GREEN);
            ClusterStateWaitStep.Result result = step.isConditionMet(originalIndex.getIndex(), clusterTargetInitializing);
            assertThat(result.isComplete(), is(true));
            assertThat(result.getInfomationContext(), nullValue());
        }
    }
}

