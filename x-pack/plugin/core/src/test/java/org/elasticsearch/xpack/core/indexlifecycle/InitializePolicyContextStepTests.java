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
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class InitializePolicyContextStepTests extends ESTestCase {

    public void testAddCreationDate() {
        long creationDate = randomNonNegativeLong();
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .creationDate(creationDate)
            .numberOfShards(1).numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        InitializePolicyContextStep step = new InitializePolicyContextStep(null, null);
        ClusterState newState = step.performAction(index, clusterState);
        assertThat(getIndexLifecycleDate(index, newState), equalTo(creationDate));
    }

    public void testDoNothing() {
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
        InitializePolicyContextStep step = new InitializePolicyContextStep(null, null);
        ClusterState newState = step.performAction(index, clusterState);
        assertTrue(newState == clusterState);
    }

    private long getIndexLifecycleDate(Index index, ClusterState clusterState) {
        return clusterState.metaData().index(index).getSettings()
            .getAsLong(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, -1L);
    }
}
