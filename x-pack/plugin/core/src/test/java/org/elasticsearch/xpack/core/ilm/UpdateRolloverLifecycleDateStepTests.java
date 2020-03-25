/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;


import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.util.Collections;
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
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
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
        IndexMetaData newIndexMetaData = IndexMetaData.builder(randomAlphaOfLength(11))
            .settings(settings(Version.CURRENT)).creationDate(creationDate)
            .putAlias(AliasMetaData.builder(alias)).numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5)).build();
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .putRolloverInfo(new RolloverInfo(alias, Collections.emptyList(), rolloverTime))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder()
                .put(indexMetaData, false)
                .put(newIndexMetaData, false)).build();

        UpdateRolloverLifecycleDateStep step = createRandomInstance();
        ClusterState newState = step.performAction(indexMetaData.getIndex(), clusterState);
        long actualRolloverTime = LifecycleExecutionState
            .fromIndexMetadata(newState.metaData().index(indexMetaData.getIndex()))
            .getLifecycleDate();
        assertThat(actualRolloverTime, equalTo(rolloverTime));
    }

    public void testPerformActionBeforeRolloverHappened() {
        String alias = randomAlphaOfLength(3);
        long creationDate = randomLongBetween(0, 1000000);
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(11))
            .settings(settings(Version.CURRENT).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias))
            .creationDate(creationDate).putAlias(AliasMetaData.builder(alias)).numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData, false)).build();
        UpdateRolloverLifecycleDateStep step = createRandomInstance();

        IllegalStateException exceptionThrown = expectThrows(IllegalStateException.class,
            () -> step.performAction(indexMetaData.getIndex(), clusterState));
        assertThat(exceptionThrown.getMessage(),
            equalTo("no rollover info found for [" + indexMetaData.getIndex().getName() + "] with alias [" + alias + "], the index " +
                "has not yet rolled over with that alias"));
    }

    public void testPerformActionWithNoRolloverAliasSetting() {
        long creationDate = randomLongBetween(0, 1000000);
        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(11))
            .settings(settings(Version.CURRENT)).creationDate(creationDate).numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData, false)).build();
        UpdateRolloverLifecycleDateStep step = createRandomInstance();

        IllegalStateException exceptionThrown = expectThrows(IllegalStateException.class,
            () -> step.performAction(indexMetaData.getIndex(), clusterState));
        assertThat(exceptionThrown.getMessage(),
            equalTo("setting [index.lifecycle.rollover_alias] is not set on index [" + indexMetaData.getIndex().getName() +"]"));
    }

    public void testPerformActionWithIndexingComplete() {
        String alias = randomAlphaOfLength(3);
        long creationDate = randomLongBetween(0, 1000000);
        long rolloverTime = randomValueOtherThan(creationDate, () -> randomNonNegativeLong());

        IndexMetaData indexMetaData = IndexMetaData.builder(randomAlphaOfLength(10))
            .settings(settings(Version.CURRENT)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, true))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder()
                .put(indexMetaData, false)).build();

        UpdateRolloverLifecycleDateStep step = createRandomInstanceWithFallbackTime(() -> rolloverTime);
        ClusterState newState = step.performAction(indexMetaData.getIndex(), clusterState);
        long actualRolloverTime = LifecycleExecutionState
            .fromIndexMetadata(newState.metaData().index(indexMetaData.getIndex()))
            .getLifecycleDate();
        assertThat(actualRolloverTime, equalTo(rolloverTime));
    }
}
