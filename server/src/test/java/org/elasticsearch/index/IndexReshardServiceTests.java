/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;

public class IndexReshardServiceTests extends ESTestCase {
    public void testReshardingIndices() {
        var indexMetadata1 = IndexMetadata.builder(randomAlphaOfLength(5)).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        var indexMetadata2 = IndexMetadata.builder(indexMetadata1.getIndex().getName() + "2")
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(1, 2))
            .build();

        var projectId = randomProjectIdOrDefault();

        var projectMetadataBuilder = ProjectMetadata.builder(projectId);
        projectMetadataBuilder.put(indexMetadata1, false);
        projectMetadataBuilder.put(indexMetadata2, false);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("testReshardingIndices"))
            .putProjectMetadata(projectMetadataBuilder)
            .build();

        Set<Index> indicesToCheck = new HashSet<>();
        indicesToCheck.add(indexMetadata1.getIndex());
        indicesToCheck.add(indexMetadata2.getIndex());

        Set<Index> reshardingIndices = IndexReshardService.reshardingIndices(clusterState.projectState(projectId), indicesToCheck);

        assertEquals(1, reshardingIndices.size());
        assertTrue(reshardingIndices.contains(indexMetadata2.getIndex()));
    }

    public void testIsShardSnapshotImpactedByResharding() {
        String indexName = randomAlphaOfLength(5);

        var noReshardingMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        assertFalse(IndexReshardService.isShardSnapshotImpactedByResharding(noReshardingMetadata, 0));

        var withReshardingMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(1, 2))
            .build();
        assertTrue(IndexReshardService.isShardSnapshotImpactedByResharding(withReshardingMetadata, 0));
        // The shard doesn't matter if there is resharding metadata.
        assertTrue(IndexReshardService.isShardSnapshotImpactedByResharding(withReshardingMetadata, 50));

        var manyShards = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 4, 0)).build();
        // When shard 3 is present in the snapshot it means that index metadata is up-to-date with the snapshot
        // and therefore the result is false.
        assertFalse(IndexReshardService.isShardSnapshotImpactedByResharding(manyShards, 3));
        // But emulating a split 2 -> 4, a snapshot now only contains shard 1 which means the result is true.
        assertTrue(IndexReshardService.isShardSnapshotImpactedByResharding(manyShards, 1));
    }
}
