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
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ShrunkenIndexCheckStepTests extends ESTestCase {

    public void testConditionMet() {
        String sourceIndex = randomAlphaOfLengthBetween(1, 10);
        IndexMetaData indexMetadata = IndexMetaData.builder(ShrinkStep.SHRUNKEN_INDEX_PREFIX + sourceIndex)
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME_KEY, sourceIndex))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        ShrunkenIndexCheckStep step = new ShrunkenIndexCheckStep(null, null);
        assertTrue(step.isConditionMet(indexMetadata.getIndex(), clusterState));
    }

    public void testConditionNotMetBecauseNotSameShrunkenIndex() {
        String sourceIndex = randomAlphaOfLengthBetween(1, 10);
        IndexMetaData indexMetadata = IndexMetaData.builder(sourceIndex + "hello")
            .settings(settings(Version.CURRENT).put(IndexMetaData.INDEX_SHRINK_SOURCE_NAME_KEY, sourceIndex))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        ShrunkenIndexCheckStep step = new ShrunkenIndexCheckStep(null, null);
        assertFalse(step.isConditionMet(indexMetadata.getIndex(), clusterState));
    }

    public void testIllegalState() {
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        ShrunkenIndexCheckStep step = new ShrunkenIndexCheckStep(null, null);
        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> step.isConditionMet(indexMetadata.getIndex(), clusterState));
        assertThat(exception.getMessage(),
            equalTo("step[is-shrunken-index] is checking an un-shrunken index[" + indexMetadata.getIndex().getName() + "]"));
    }
}
