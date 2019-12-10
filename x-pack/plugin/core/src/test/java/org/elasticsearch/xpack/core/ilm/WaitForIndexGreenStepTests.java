/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
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

public class WaitForIndexGreenStepTests extends AbstractStepTestCase<WaitForIndexGreenStep> {

    @Override
    protected WaitForIndexGreenStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new WaitForIndexGreenStep(stepKey, nextStepKey);
    }

    @Override
    protected WaitForIndexGreenStep mutateInstance(WaitForIndexGreenStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return new WaitForIndexGreenStep(key, nextKey);
    }

    @Override
    protected WaitForIndexGreenStep copyInstance(WaitForIndexGreenStep instance) {
        return new WaitForIndexGreenStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
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

        WaitForIndexGreenStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionNotMet() {
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

        WaitForIndexGreenStep step = new WaitForIndexGreenStep(randomStepKey(), randomStepKey());
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexGreenStep.Info info = (WaitForIndexGreenStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is not green; not all shards are active"));
    }

    public void testConditionNotMetWithYellow() {
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();

        ShardRouting shardRouting =
            TestShardRouting.newShardRouting("test_index", 0, "1", true, ShardRoutingState.STARTED);

        ShardRouting replicaShardRouting =
            TestShardRouting.newShardRouting("test_index", 0, "2", false, ShardRoutingState.INITIALIZING);

        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(indexMetadata.getIndex())
            .addShard(shardRouting)
            .addShard(replicaShardRouting)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().add(indexRoutingTable).build())
            .build();

        WaitForIndexGreenStep step = new WaitForIndexGreenStep(randomStepKey(), randomStepKey());
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexGreenStep.Info info = (WaitForIndexGreenStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is yellow; not all replica shards are active"));
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

        WaitForIndexGreenStep step = new WaitForIndexGreenStep(randomStepKey(), randomStepKey());
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForIndexGreenStep.Info info = (WaitForIndexGreenStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is red; no IndexRoutingTable"));
    }
}

