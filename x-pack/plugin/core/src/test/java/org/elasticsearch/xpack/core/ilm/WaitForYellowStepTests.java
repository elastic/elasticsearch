/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
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

public class WaitForYellowStepTests extends AbstractStepTestCase<WaitForYellowStep> {

    @Override
    protected WaitForYellowStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new WaitForYellowStep(stepKey, nextStepKey);
    }

    @Override
    protected WaitForYellowStep mutateInstance(WaitForYellowStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return new WaitForYellowStep(key, nextKey);
    }

    @Override
    protected WaitForYellowStep copyInstance(WaitForYellowStep instance) {
        return new WaitForYellowStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
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

        WaitForYellowStep step = new WaitForYellowStep(randomStepKey(), randomStepKey());
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionNotMet() {
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

        WaitForYellowStep step = new WaitForYellowStep(randomStepKey(), randomStepKey());
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForYellowStep.Info info = (WaitForYellowStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is red; not all primary shards are active"));
    }

    public void testConditionNotMetNoIndexRoutingTable() {
        IndexMetadata indexMetadata = IndexMetadata.builder("former-follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .routingTable(RoutingTable.builder().build())
            .build();

        WaitForYellowStep step = new WaitForYellowStep(randomStepKey(), randomStepKey());
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        WaitForYellowStep.Info info = (WaitForYellowStep.Info) result.getInfomationContext();
        assertThat(info, notNullValue());
        assertThat(info.getMessage(), equalTo("index is red; no IndexRoutingTable"));
    }
}
