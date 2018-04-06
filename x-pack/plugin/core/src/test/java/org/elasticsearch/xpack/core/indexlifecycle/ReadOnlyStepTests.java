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
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ReadOnlyStepTests extends ESTestCase {

    public void testPerformAction() {
        Settings.Builder indexSettingsBuilder = settings(Version.CURRENT);
        if (randomBoolean()) {
            indexSettingsBuilder.put(IndexMetaData.SETTING_BLOCKS_WRITE, randomBoolean());
        }
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();
        ReadOnlyStep step = new ReadOnlyStep(null, null);
        ClusterState newState = step.performAction(index, clusterState);
        assertThat(newState.metaData().index(index).getSettings().get(IndexMetaData.SETTING_BLOCKS_WRITE), equalTo("true"));
    }
}
