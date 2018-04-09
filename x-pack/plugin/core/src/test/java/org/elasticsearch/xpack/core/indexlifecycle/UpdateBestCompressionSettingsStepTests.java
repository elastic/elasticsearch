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
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public class UpdateBestCompressionSettingsStepTests extends ESTestCase {

    public UpdateBestCompressionSettingsStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));

        return new UpdateBestCompressionSettingsStep(stepKey, nextStepKey);
    }


    public void testHashcodeAndEquals() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomInstance(),
                instance -> new UpdateBestCompressionSettingsStep(instance.getKey(), instance.getNextStepKey()));
    }

    public void testPerformAction() {
        IndexMetaData indexMetadata = IndexMetaData.builder(randomAlphaOfLength(5))
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0).build();
        MetaData metaData = MetaData.builder()
            .persistentSettings(settings(Version.CURRENT).build())
            .put(IndexMetaData.builder(indexMetadata))
            .build();
        Index index = indexMetadata.getIndex();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();

        UpdateBestCompressionSettingsStep step = createRandomInstance();
        ClusterState newState = step.performAction(index, clusterState);
        assertNotSame(clusterState, newState);
        IndexMetaData newIndexMetadata = newState.metaData().index(index);
        assertNotNull(newIndexMetadata);
        assertNotSame(indexMetadata, newIndexMetadata);
        assertTrue(EngineConfig.INDEX_CODEC_SETTING.exists(newIndexMetadata.getSettings()));
        assertTrue(CodecService.BEST_COMPRESSION_CODEC.equals(
            newIndexMetadata.getSettings().get(EngineConfig.INDEX_CODEC_SETTING.getKey())));
    }

    public void testPerformActionNoIndex() {
        MetaData metaData = MetaData.builder().persistentSettings(settings(Version.CURRENT).build()).build();
        Index index = new Index("invalid_index", "invalid_index_id");
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();

        UpdateBestCompressionSettingsStep step = createRandomInstance();
        ClusterState newState = step.performAction(index, clusterState);
        assertSame(clusterState, newState);
    }
}
