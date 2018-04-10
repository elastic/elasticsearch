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
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public class UpdateReplicaSettingsStepTests extends ESTestCase {

    public UpdateReplicaSettingsStep createRandomInstance() {
        StepKey stepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));

        return new UpdateReplicaSettingsStep(stepKey, nextStepKey, randomIntBetween(0, 100));
    }

    public UpdateReplicaSettingsStep mutateInstance(UpdateReplicaSettingsStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        int replicas = instance.getNumberOfReplicas();

        switch (between(0, 2)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            replicas += 1;
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new UpdateReplicaSettingsStep(key, nextKey, replicas);
    }

    public void testHashcodeAndEquals() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomInstance(),
                instance -> new UpdateReplicaSettingsStep(instance.getKey(), instance.getNextStepKey(), instance.getNumberOfReplicas()),
                this::mutateInstance);
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

        UpdateReplicaSettingsStep step = createRandomInstance();
        ClusterState newState = step.performAction(index, clusterState);
        assertNotSame(clusterState, newState);
        IndexMetaData newIndexMetadata = newState.metaData().index(index);
        assertNotNull(newIndexMetadata);
        assertNotSame(indexMetadata, newIndexMetadata);
        assertTrue(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.exists(newIndexMetadata.getSettings()));
        assertEquals(step.getNumberOfReplicas(),
                (int) IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(newIndexMetadata.getSettings()));
    }

    public void testPerformActionNoIndex() {
        MetaData metaData = MetaData.builder().persistentSettings(settings(Version.CURRENT).build()).build();
        Index index = new Index("invalid_index", "invalid_index_id");
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).build();

        UpdateReplicaSettingsStep step = createRandomInstance();
        ClusterState newState = step.performAction(index, clusterState);
        assertSame(clusterState, newState);
    }
}
