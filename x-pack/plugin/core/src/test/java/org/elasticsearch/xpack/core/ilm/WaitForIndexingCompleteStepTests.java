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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WaitForIndexingCompleteStepTests extends AbstractStepTestCase<WaitForIndexingCompleteStep> {

    @Override
    protected WaitForIndexingCompleteStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new WaitForIndexingCompleteStep(stepKey, nextStepKey);
    }

    @Override
    protected WaitForIndexingCompleteStep mutateInstance(WaitForIndexingCompleteStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(nextKey.getPhase(), nextKey.getAction(), nextKey.getName() + randomAlphaOfLength(5));
        }

        return new WaitForIndexingCompleteStep(key, nextKey);
    }

    @Override
    protected WaitForIndexingCompleteStep copyInstance(WaitForIndexingCompleteStep instance) {
        return new WaitForIndexingCompleteStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        WaitForIndexingCompleteStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionMetNotAFollowerIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        WaitForIndexingCompleteStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionNotMet() {
        Settings.Builder indexSettings = settings(Version.CURRENT);
        if (randomBoolean()) {
            indexSettings.put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "false");
        }
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(indexSettings)
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        WaitForIndexingCompleteStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        assertThat(result.getInfomationContext(), notNullValue());
        WaitForIndexingCompleteStep.IndexingNotCompleteInfo info = (WaitForIndexingCompleteStep.IndexingNotCompleteInfo) result
            .getInfomationContext();
        assertThat(
            info.getMessage(),
            equalTo(
                "waiting for the [index.lifecycle.indexing_complete] setting to be set to "
                    + "true on the leader index, it is currently [false]"
            )
        );
    }

    public void testIndexDeleted() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster")).metadata(Metadata.builder().build()).build();

        WaitForIndexingCompleteStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(new Index("this-index-doesnt-exist", "uuid"), clusterState);
        assertThat(result.isComplete(), is(false));
        assertThat(result.getInfomationContext(), nullValue());
    }
}
