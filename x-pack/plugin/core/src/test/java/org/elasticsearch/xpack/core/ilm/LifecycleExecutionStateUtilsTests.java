/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionStateUtils.newClusterStateWithLifecycleState;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class LifecycleExecutionStateUtilsTests extends ESTestCase {

    public void testNewClusterStateWithLifecycleState() {
        String indexName = "my-index";
        String indexUUID = randomAlphaOfLength(10);
        Index index = new Index(indexName, indexUUID);

        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(indexName)
                    .settings(settings(IndexVersion.current()).put(IndexMetadata.SETTING_INDEX_UUID, indexUUID))
                    .creationDate(randomNonNegativeLong())
                    .numberOfShards(1)
                    .numberOfReplicas(0)

            )
            .build();

        ClusterState clusterState1 = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        IndexMetadata indexMetadata1 = clusterState1.metadata().index(indexName);
        assertThat(indexMetadata1.getLifecycleExecutionState(), sameInstance(LifecycleExecutionState.EMPTY_STATE));

        LifecycleExecutionState state = LifecycleExecutionState.builder().setPhase("phase").setAction("action").setStep("step").build();

        // setting the state via newClusterStateWithLifecycleState will set the state
        ClusterState clusterState2 = newClusterStateWithLifecycleState(clusterState1, index, state);
        IndexMetadata indexMetadata2 = clusterState2.metadata().index(indexName);
        assertThat(indexMetadata2.getLifecycleExecutionState().asMap(), is(state.asMap()));

        // but if the lifecycle state doesn't actually change, then you get the same cluster state back
        LifecycleExecutionState copy = LifecycleExecutionState.fromCustomMetadata(state.asMap());
        ClusterState clusterState3 = newClusterStateWithLifecycleState(clusterState2, index, copy);
        assertThat(clusterState3, sameInstance(clusterState2));
    }

}
