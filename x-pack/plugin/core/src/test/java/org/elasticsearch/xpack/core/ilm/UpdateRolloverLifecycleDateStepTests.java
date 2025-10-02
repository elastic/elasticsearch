/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.List;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;

public class UpdateRolloverLifecycleDateStepTests extends AbstractStepTestCase<UpdateRolloverLifecycleDateStep> {

    @Override
    public UpdateRolloverLifecycleDateStep createRandomInstance() {
        return createRandomInstanceWithFallbackTime(null);
    }

    public UpdateRolloverLifecycleDateStep createRandomInstanceWithFallbackTime(LongSupplier fallbackTimeSupplier) {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        return new UpdateRolloverLifecycleDateStep(stepKey, nextStepKey, fallbackTimeSupplier);
    }

    @Override
    public UpdateRolloverLifecycleDateStep mutateInstance(UpdateRolloverLifecycleDateStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }

        return new UpdateRolloverLifecycleDateStep(key, nextKey, null);
    }

    @Override
    public UpdateRolloverLifecycleDateStep copyInstance(UpdateRolloverLifecycleDateStep instance) {
        return new UpdateRolloverLifecycleDateStep(instance.getKey(), instance.getNextStepKey(), null);
    }

    @SuppressWarnings("unchecked")
    public void testPerformAction() {
        String alias = randomAlphaOfLength(3);
        long creationDate = randomLongBetween(0, 1000000);
        long rolloverTime = randomValueOtherThan(creationDate, () -> randomNonNegativeLong());
        IndexMetadata newIndexMetadata = IndexMetadata.builder(randomAlphaOfLength(11))
            .settings(settings(IndexVersion.current()))
            .creationDate(creationDate)
            .putAlias(AliasMetadata.builder(alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .putRolloverInfo(new RolloverInfo(alias, List.of(), rolloverTime))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ProjectState state = projectStateFromProject(
            ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false).put(newIndexMetadata, false)
        );

        UpdateRolloverLifecycleDateStep step = createRandomInstance();
        ProjectState newState = step.performAction(indexMetadata.getIndex(), state);
        long actualRolloverTime = newState.metadata().index(indexMetadata.getIndex()).getLifecycleExecutionState().lifecycleDate();
        assertThat(actualRolloverTime, equalTo(rolloverTime));
    }

    public void testPerformActionOnDataStream() {
        long creationDate = randomLongBetween(0, 1000000);
        long rolloverTime = randomValueOtherThan(creationDate, () -> randomNonNegativeLong());
        String dataStreamName = "test-datastream";
        IndexMetadata originalIndexMeta = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .putRolloverInfo(new RolloverInfo(dataStreamName, List.of(), rolloverTime))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        IndexMetadata rolledIndexMeta = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ProjectState state = projectStateFromProject(
            ProjectMetadata.builder(randomProjectIdOrDefault())
                .put(DataStreamTestHelper.newInstance(dataStreamName, List.of(originalIndexMeta.getIndex(), rolledIndexMeta.getIndex())))
                .put(originalIndexMeta, true)
                .put(rolledIndexMeta, true)
        );

        UpdateRolloverLifecycleDateStep step = createRandomInstance();
        ProjectState newState = step.performAction(originalIndexMeta.getIndex(), state);
        long actualRolloverTime = newState.metadata().index(originalIndexMeta.getIndex()).getLifecycleExecutionState().lifecycleDate();
        assertThat(actualRolloverTime, equalTo(rolloverTime));
    }

    public void testPerformActionBeforeRolloverHappened() {
        String alias = randomAlphaOfLength(3);
        long creationDate = randomLongBetween(0, 1000000);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(11))
            .settings(settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .creationDate(creationDate)
            .putAlias(AliasMetadata.builder(alias))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));
        UpdateRolloverLifecycleDateStep step = createRandomInstance();

        IllegalStateException exceptionThrown = expectThrows(
            IllegalStateException.class,
            () -> step.performAction(indexMetadata.getIndex(), state)
        );
        assertThat(
            exceptionThrown.getMessage(),
            equalTo(
                "no rollover info found for ["
                    + indexMetadata.getIndex().getName()
                    + "] with rollover target ["
                    + alias
                    + "], the "
                    + "index has not yet rolled over with that target"
            )
        );
    }

    public void testPerformActionWithNoRolloverAliasSetting() {
        long creationDate = randomLongBetween(0, 1000000);
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(11))
            .settings(settings(IndexVersion.current()))
            .creationDate(creationDate)
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));
        UpdateRolloverLifecycleDateStep step = createRandomInstance();

        IllegalStateException exceptionThrown = expectThrows(
            IllegalStateException.class,
            () -> step.performAction(indexMetadata.getIndex(), state)
        );
        assertThat(
            exceptionThrown.getMessage(),
            equalTo("setting [index.lifecycle.rollover_alias] is not set on index [" + indexMetadata.getIndex().getName() + "]")
        );
    }

    public void testPerformActionWithIndexingComplete() {
        String alias = randomAlphaOfLength(3);
        long creationDate = randomLongBetween(0, 1000000);
        long rolloverTime = randomValueOtherThan(creationDate, () -> randomNonNegativeLong());

        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(
                settings(IndexVersion.current()).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                    .put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true)
            )
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        ProjectState state = projectStateFromProject(ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, false));

        UpdateRolloverLifecycleDateStep step = createRandomInstanceWithFallbackTime(() -> rolloverTime);
        ProjectState newState = step.performAction(indexMetadata.getIndex(), state);
        long actualRolloverTime = newState.metadata().index(indexMetadata.getIndex()).getLifecycleExecutionState().lifecycleDate();
        assertThat(actualRolloverTime, equalTo(rolloverTime));
    }
}
