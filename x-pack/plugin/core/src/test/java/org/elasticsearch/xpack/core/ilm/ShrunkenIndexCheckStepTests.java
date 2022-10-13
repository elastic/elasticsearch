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
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep.Result;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;

public class ShrunkenIndexCheckStepTests extends AbstractStepTestCase<ShrunkenIndexCheckStep> {

    @Override
    public ShrunkenIndexCheckStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String shrunkIndexPrefix = randomAlphaOfLength(10);
        return new ShrunkenIndexCheckStep(stepKey, nextStepKey);
    }

    @Override
    public ShrunkenIndexCheckStep mutateInstance(ShrunkenIndexCheckStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ShrunkenIndexCheckStep(key, nextKey);
    }

    @Override
    public ShrunkenIndexCheckStep copyInstance(ShrunkenIndexCheckStep instance) {
        return new ShrunkenIndexCheckStep(instance.getKey(), instance.getNextStepKey());
    }

    public void testConditionMet() {
        ShrunkenIndexCheckStep step = createRandomInstance();
        String sourceIndex = randomAlphaOfLengthBetween(1, 10);
        IndexMetadata indexMetadata = IndexMetadata.builder(SHRUNKEN_INDEX_PREFIX + sourceIndex)
            .settings(settings(Version.CURRENT).put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetadata.builder(indexMetadata))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertTrue(result.isComplete());
        assertNull(result.getInfomationContext());
    }

    public void testConditionNotMetBecauseNotSameShrunkenIndex() {
        ShrunkenIndexCheckStep step = createRandomInstance();
        String sourceIndex = randomAlphaOfLengthBetween(1, 10);
        IndexMetadata shrinkIndexMetadata = IndexMetadata.builder(sourceIndex + "hello")
            .settings(settings(Version.CURRENT).put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetadata.builder(shrinkIndexMetadata))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        Result result = step.isConditionMet(shrinkIndexMetadata.getIndex(), clusterState);
        assertFalse(result.isComplete());
        assertEquals(new ShrunkenIndexCheckStep.Info(sourceIndex), result.getInfomationContext());
    }

    public void testConditionNotMetBecauseSourceIndexExists() {
        ShrunkenIndexCheckStep step = createRandomInstance();
        String sourceIndex = randomAlphaOfLengthBetween(1, 10);
        IndexMetadata originalIndexMetadata = IndexMetadata.builder(sourceIndex)
            .settings(settings(Version.CURRENT))
            .numberOfShards(100)
            .numberOfReplicas(0)
            .build();
        IndexMetadata shrinkIndexMetadata = IndexMetadata.builder(SHRUNKEN_INDEX_PREFIX + sourceIndex)
            .settings(settings(Version.CURRENT).put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetadata.builder(originalIndexMetadata))
            .put(IndexMetadata.builder(shrinkIndexMetadata))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        Result result = step.isConditionMet(shrinkIndexMetadata.getIndex(), clusterState);
        assertFalse(result.isComplete());
        assertEquals(new ShrunkenIndexCheckStep.Info(sourceIndex), result.getInfomationContext());
    }

    public void testIllegalState() {
        ShrunkenIndexCheckStep step = createRandomInstance();
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Metadata metadata = Metadata.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetadata.builder(indexMetadata))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> step.isConditionMet(indexMetadata.getIndex(), clusterState)
        );
        assertThat(
            exception.getMessage(),
            equalTo("step[is-shrunken-index] is checking an un-shrunken index[" + indexMetadata.getIndex().getName() + "]")
        );
    }
}
