/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import static org.hamcrest.Matchers.is;

public class CheckTargetShardsCountStepTests extends AbstractStepTestCase<CheckTargetShardsCountStep> {

    @Override
    protected CheckTargetShardsCountStep createRandomInstance() {
        return new CheckTargetShardsCountStep(randomStepKey(), randomStepKey(), null);
    }

    @Override
    protected CheckTargetShardsCountStep mutateInstance(CheckTargetShardsCountStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new CheckTargetShardsCountStep(key, nextKey, null);
    }

    @Override
    protected CheckTargetShardsCountStep copyInstance(CheckTargetShardsCountStep instance) {
        return new CheckTargetShardsCountStep(instance.getKey(), instance.getNextStepKey(), instance.getNumberOfShards());
    }

    public void testStepCompleteIfTargetShardsCountIsValid() {
        String policyName = "test-ilm-policy";
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(10)
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        CheckTargetShardsCountStep checkTargetShardsCountStep = new CheckTargetShardsCountStep(randomStepKey(), randomStepKey(), 2);

        ClusterStateWaitStep.Result result = checkTargetShardsCountStep.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
    }

    public void testStepIncompleteIfTargetShardsCountNotValid() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
            .numberOfShards(10)
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        CheckTargetShardsCountStep checkTargetShardsCountStep = new CheckTargetShardsCountStep(randomStepKey(), randomStepKey(), 3);

        ClusterStateWaitStep.Result result = checkTargetShardsCountStep.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        SingleMessageFieldInfo info = (SingleMessageFieldInfo) result.getInfomationContext();
        assertThat(
            info.getMessage(),
            is(
                "lifecycle action of policy ["
                    + policyName
                    + "] for index ["
                    + indexName
                    + "] cannot make progress because the target shards count [3] must be a factor of the source index's shards count [10]"
            )
        );
    }
}
