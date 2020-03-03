/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
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
        return new WaitForIndexColorStep(stepKey, nextStepKey, color);
    }

    @Override
    protected WaitForIndexColorStep mutateInstance(WaitForIndexColorStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        ClusterHealthStatus color = instance.getColor(), newColor = randomColor();
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
        }

        return new WaitForIndexColorStep(key, nextKey, color);
    }

    @Override
    protected WaitForIndexColorStep copyInstance(WaitForIndexColorStep instance) {
        return new WaitForIndexColorStep(instance.getKey(), instance.getNextStepKey(), instance.getColor());
    }

    public void testConditionMetForGreen() {
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("test_index", 0, "1", true, ShardRoutingState.STARTED);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.GREEN);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionNotMetForGreen() {
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("test_index", 0, "1", true, ShardRoutingState.INITIALIZING);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
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
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
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
        IndexMetaData indexMetadata = IndexMetaData.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("index2", 0, "1", true, ShardRoutingState.STARTED);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.YELLOW);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionNotMetForYellow() {
        IndexMetaData indexMetadata = IndexMetaData.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("index2", 0, "1", true, ShardRoutingState.INITIALIZING);
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting).build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
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
        IndexMetaData indexMetadata = IndexMetaData.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().build())
            .build();

        WaitForIndexColorStep step = new WaitForIndexColorStep(randomStepKey(), randomStepKey(), ClusterHealthStatus.YELLOW);
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexColorStep.Info info = (WaitForIndexColorStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is red; no indexRoutingTable"));
    }
}

