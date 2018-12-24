/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WaitForIndexingCompleteTests extends AbstractStepTestCase<WaitForIndexingComplete> {

    @Override
    protected WaitForIndexingComplete createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new WaitForIndexingComplete(stepKey, nextStepKey);
    }

    @Override
    protected WaitForIndexingComplete mutateInstance(WaitForIndexingComplete instance) {
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

        return new WaitForIndexingComplete(key, nextKey);
    }

    @Override
    protected WaitForIndexingComplete copyInstance(WaitForIndexingComplete instance) {
        return new WaitForIndexingComplete(instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom("ccr", Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
            .build();

        WaitForIndexingComplete step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionMetNotAFollowerIndex() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
            .build();

        WaitForIndexingComplete step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
        assertThat(result.getInfomationContext(), nullValue());
    }

    public void testConditionNotMet() {
        Settings.Builder indexSettings = settings(Version.CURRENT);
        if (randomBoolean()) {
            indexSettings.put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "false");
        }
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(indexSettings)
            .putCustom("ccr", Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metaData(MetaData.builder().put(indexMetadata, true).build())
            .build();

        WaitForIndexingComplete step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        assertThat(result.getInfomationContext(), notNullValue());
        WaitForIndexingComplete.Info info = (WaitForIndexingComplete.Info) result.getInfomationContext();
        assertThat(info.getIndexSettings(), equalTo(indexMetadata.getSettings()));
        assertThat(info.getMessage(), equalTo("the [index.lifecycle.indexing_complete] setting has not been set to true"));
    }
}
