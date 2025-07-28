/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc;

import org.apache.lucene.util.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class ArchiveAllocationDeciderTests extends ESTestCase {

    public void testLicenseCheckIgnoredOnCurrentIndexVersion() {
        checkCanAllocate(IndexVersion.current(), false, Decision.Type.YES);
        checkCanAllocate(IndexVersion.current(), true, Decision.Type.YES);
    }

    public void testLicenseCheckIgnoredOnPreviousMinor() {
        checkCanAllocate(IndexVersions.MINIMUM_READONLY_COMPATIBLE, false, Decision.Type.YES);
        checkCanAllocate(IndexVersions.MINIMUM_READONLY_COMPATIBLE, true, Decision.Type.YES);
    }

    public void testLicenseCheckOnOldVersion() {
        final IndexVersion version = new IndexVersion(
            randomIntBetween(2_00_00_00, IndexVersions.MINIMUM_READONLY_COMPATIBLE.id() - 1),
            Version.fromBits(randomIntBetween(1, Version.MIN_SUPPORTED_MAJOR - 1), randomIntBetween(1, 10), randomIntBetween(1, 10))
        );
        checkCanAllocate(version, false, Decision.Type.NO);
        checkCanAllocate(version, true, Decision.Type.YES);
    }

    private static void checkCanAllocate(IndexVersion indexVersion, boolean validLicense, Decision.Type expectedType) {
        final String indexName = randomAlphaOfLengthBetween(3, 12);
        final String indexUuid = randomUUID();
        final String nodeId = randomIdentifier();

        final ShardRouting shard = TestShardRouting.newShardRouting(
            new ShardId(indexName, indexUuid, 0),
            nodeId,
            true,
            ShardRoutingState.STARTED
        );
        final ProjectMetadata.Builder projectbuilder = ProjectMetadata.builder(randomUniqueProjectId());
        addIndex(projectbuilder, indexName, indexUuid, indexVersion);
        addRandomIndices(projectbuilder, randomIntBetween(1, 5));

        final Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.put(projectbuilder);
        for (int p = randomIntBetween(0, 5); p > 0; p--) {
            metadataBuilder.put(addRandomIndices(ProjectMetadata.builder(randomUniqueProjectId()), randomIntBetween(0, 10)));
        }

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadataBuilder).build();

        final ArchiveAllocationDecider decider = new ArchiveAllocationDecider(() -> validLicense);
        final RoutingAllocation allocation = new RoutingAllocation(new AllocationDeciders(List.of(decider)), clusterState, null, null, 0L);
        assertThat(decider.canAllocate(shard, allocation).type(), is(expectedType));
    }

    private static ProjectMetadata.Builder addRandomIndices(ProjectMetadata.Builder projectbuilder, int count) {
        for (int i = 0; i < count; i++) {
            addIndex(projectbuilder, randomAlphaOfLengthBetween(5, 16), randomUUID(), IndexVersionUtils.randomVersion());
        }
        return projectbuilder;
    }

    private static void addIndex(ProjectMetadata.Builder projectbuilder, String indexName, String indexUuid, IndexVersion indexVersion) {
        projectbuilder.put(
            IndexMetadata.builder(indexName)
                .settings(indexSettings(indexVersion, 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, indexUuid))
                .build(),
            false
        );
    }

}
