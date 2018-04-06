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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

public class PhaseAfterStepTests extends ESTestCase {

    public void testConditionMet() {
        long creationDate = randomNonNegativeLong();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, creationDate))
            .creationDate(creationDate)
            .numberOfShards(1).numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        long after = randomNonNegativeLong();
        long now = creationDate + after + randomIntBetween(0, 2);
        PhaseAfterStep step = new PhaseAfterStep(() -> now, TimeValue.timeValueMillis(after), null, null);
        assertTrue(step.isConditionMet(index, clusterState));
    }

    public void testConditionNotMet() {
        long creationDate = randomNonNegativeLong();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, creationDate))
            .creationDate(creationDate)
            .numberOfShards(1).numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        long after = randomNonNegativeLong();
        long now = creationDate + after - randomIntBetween(1, 1000);
        PhaseAfterStep step = new PhaseAfterStep(() -> now, TimeValue.timeValueMillis(after), null, null);
        assertFalse(step.isConditionMet(index, clusterState));
    }
}
