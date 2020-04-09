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
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep.Result;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import static org.hamcrest.Matchers.equalTo;

public class ShrunkenIndexCheckStepTests extends AbstractStepTestCase<ShrunkenIndexCheckStep> {

    @Override
    public ShrunkenIndexCheckStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String shrunkIndexPrefix = randomAlphaOfLength(10);
        return new ShrunkenIndexCheckStep(stepKey, nextStepKey, shrunkIndexPrefix);
    }

    @Override
    public ShrunkenIndexCheckStep mutateInstance(ShrunkenIndexCheckStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        String shrunkIndexPrefix = instance.getShrunkIndexPrefix();
        switch (between(0, 2)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            shrunkIndexPrefix += randomAlphaOfLength(5);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new ShrunkenIndexCheckStep(key, nextKey, shrunkIndexPrefix);
    }

    @Override
    public ShrunkenIndexCheckStep copyInstance(ShrunkenIndexCheckStep instance) {
        return new ShrunkenIndexCheckStep(instance.getKey(), instance.getNextStepKey(), instance.getShrunkIndexPrefix());
    }

    public void testConditionMet() {
        ShrunkenIndexCheckStep step = createRandomInstance();
        String sourceIndex = randomAlphaOfLengthBetween(1, 10);
        IndexMetaData indexMetadata = IndexMetaData.builder(step.getShrunkIndexPrefix() + sourceIndex)
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        Result result = step.isConditionMet(indexMetadata.getIndex(), clusterState);
        assertTrue(result.isComplete());
        assertNull(result.getInfomationContext());
    }

    public void testConditionNotMetBecauseNotSameShrunkenIndex() {
        ShrunkenIndexCheckStep step = createRandomInstance();
        String sourceIndex = randomAlphaOfLengthBetween(1, 10);
        IndexMetaData shrinkIndexMetadata = IndexMetaData.builder(sourceIndex + "hello")
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(shrinkIndexMetadata))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        Result result = step.isConditionMet(shrinkIndexMetadata.getIndex(), clusterState);
        assertFalse(result.isComplete());
        assertEquals(new ShrunkenIndexCheckStep.Info(sourceIndex), result.getInfomationContext());
    }

    public void testConditionNotMetBecauseSourceIndexExists() {
        ShrunkenIndexCheckStep step = createRandomInstance();
        String sourceIndex = randomAlphaOfLengthBetween(1, 10);
        IndexMetaData originalIndexMetadata = IndexMetaData.builder(sourceIndex)
            .settings(settings(Version.CURRENT))
            .numberOfShards(100)
            .numberOfReplicas(0).build();
        IndexMetaData shrinkIndexMetadata = IndexMetaData.builder(step.getShrunkIndexPrefix() + sourceIndex)
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_RESIZE_SOURCE_NAME_KEY, sourceIndex))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(originalIndexMetadata))
            .put(IndexMetaData.builder(shrinkIndexMetadata))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        Result result = step.isConditionMet(shrinkIndexMetadata.getIndex(), clusterState);
        assertFalse(result.isComplete());
        assertEquals(new ShrunkenIndexCheckStep.Info(sourceIndex), result.getInfomationContext());
    }

    public void testIllegalState() {
        ShrunkenIndexCheckStep step = createRandomInstance();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> step.isConditionMet(indexMetadata.getIndex(), clusterState));
        assertThat(exception.getMessage(),
            equalTo("step[is-shrunken-index] is checking an un-shrunken index[" + indexMetadata.getIndex().getName() + "]"));
    }
}
