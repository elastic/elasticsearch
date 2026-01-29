/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Map;

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
            key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }

        return new WaitForIndexingCompleteStep(key, nextKey);
    }

    @Override
    protected WaitForIndexingCompleteStep copyInstance(WaitForIndexingCompleteStep instance) {
        return new WaitForIndexingCompleteStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), state);
        assertThat(result.complete(), is(true));
        assertThat(result.informationContext(), nullValue());
    }

    public void testConditionMetNotAFollowerIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), state);
        assertThat(result.complete(), is(true));
        assertThat(result.informationContext(), nullValue());
    }

    public void testConditionNotMet() {
        Settings.Builder indexSettings = settings(IndexVersion.current());
        if (randomBoolean()) {
            indexSettings.put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "false");
        }
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(indexSettings)
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        WaitForIndexingCompleteStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(indexMetadata.getIndex(), state);
        assertThat(result.complete(), is(false));
        assertThat(result.informationContext(), notNullValue());
        WaitForIndexingCompleteStep.IndexingNotCompleteInfo info = (WaitForIndexingCompleteStep.IndexingNotCompleteInfo) result
            .informationContext();
        assertThat(
            info.getMessage(),
            equalTo(
                "waiting for the [index.lifecycle.indexing_complete] setting to be set to "
                    + "true on the leader index, it is currently [false]"
            )
        );
    }

    public void testIndexDeleted() {
        ProjectState state = projectStateWithEmptyProject();

        WaitForIndexingCompleteStep step = createRandomInstance();
        ClusterStateWaitStep.Result result = step.isConditionMet(new Index("this-index-doesnt-exist", "uuid"), state);
        assertThat(result.complete(), is(false));
        assertThat(result.informationContext(), nullValue());
    }
}
