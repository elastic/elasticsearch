/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;

public class InitializePolicyContextStepTests extends AbstractStepTestCase<InitializePolicyContextStep> {

    @Override
    public InitializePolicyContextStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        return new InitializePolicyContextStep(stepKey, nextStepKey);
    }

    @Override
    public InitializePolicyContextStep mutateInstance(InitializePolicyContextStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new InitializePolicyContextStep(key, nextKey);
    }

    @Override
    public InitializePolicyContextStep copyInstance(InitializePolicyContextStep instance) {
        return new InitializePolicyContextStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testAddCreationDate() {
        long creationDate = randomNonNegativeLong();
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(IndexVersion.current()))
            .creationDate(creationDate)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(IndexMetadata.builder(indexMetadata)).build();
        Metadata metadata = Metadata.builder().persistentSettings(settings(IndexVersion.current()).build()).put(project).build();
        Index index = indexMetadata.getIndex();
        ProjectState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build().projectState(project.id());
        InitializePolicyContextStep step = new InitializePolicyContextStep(null, null);
        ProjectState newState = step.performAction(index, state);
        assertThat(getIndexLifecycleDate(index, newState), equalTo(creationDate));
    }

    public void testDoNothing() {
        long creationDate = randomNonNegativeLong();
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setIndexCreationDate(creationDate);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(IndexVersion.current()))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .creationDate(creationDate)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).put(IndexMetadata.builder(indexMetadata)).build();
        Metadata metadata = Metadata.builder().persistentSettings(settings(IndexVersion.current()).build()).put(project).build();
        Index index = indexMetadata.getIndex();
        ProjectState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build().projectState(project.id());
        InitializePolicyContextStep step = new InitializePolicyContextStep(null, null);
        ProjectState newState = step.performAction(index, state);
        assertTrue(newState == state);
    }

    private long getIndexLifecycleDate(Index index, ProjectState projectState) {
        return projectState.metadata().index(index).getLifecycleExecutionState().lifecycleDate();
    }
}
