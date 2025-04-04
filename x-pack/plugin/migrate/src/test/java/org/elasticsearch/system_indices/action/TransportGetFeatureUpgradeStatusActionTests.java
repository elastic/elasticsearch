/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.MIGRATION_NEEDED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TransportGetFeatureUpgradeStatusActionTests extends ESTestCase {

    public static String TEST_SYSTEM_INDEX_PATTERN = ".test*";
    // Version just before MINIMUM_COMPATIBLE in order to check that UpgradeStatus.MIGRATION_NEEDED is set correctly
    private static final IndexVersion TEST_OLD_VERSION = IndexVersion.fromId(IndexVersions.MINIMUM_COMPATIBLE.id() - 1);
    private static final ClusterState CLUSTER_STATE = getClusterState();
    private static final SystemIndices.Feature FEATURE = getFeature();

    public void testGetFeatureStatus() {
        GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus status = TransportGetFeatureUpgradeStatusAction.getFeatureUpgradeStatus(
            CLUSTER_STATE,
            FEATURE
        );

        assertThat(status.getUpgradeStatus(), equalTo(MIGRATION_NEEDED));
        assertThat(status.getFeatureName(), equalTo("test-feature"));
        assertThat(status.getMinimumIndexVersion(), equalTo(TEST_OLD_VERSION));
        assertThat(status.getIndexVersions(), hasSize(2)); // additional testing below
    }

    public void testGetIndexInfos() {
        List<GetFeatureUpgradeStatusResponse.IndexInfo> versions = TransportGetFeatureUpgradeStatusAction.getIndexInfos(
            CLUSTER_STATE,
            FEATURE
        );

        assertThat(versions, hasSize(2));

        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(0);
            assertThat(version.getVersion(), equalTo(IndexVersion.current()));
            assertThat(version.getIndexName(), equalTo(".test-index-1"));
        }
        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(1);
            assertThat(version.getVersion(), equalTo(TEST_OLD_VERSION));
            assertThat(version.getIndexName(), equalTo(".test-index-2"));
        }
    }

    private static SystemIndices.Feature getFeature() {
        SystemIndexDescriptor descriptor = SystemIndexDescriptorUtils.createUnmanaged(TEST_SYSTEM_INDEX_PATTERN, "descriptor for tests");

        List<SystemIndexDescriptor> descriptors = new ArrayList<>();
        descriptors.add(descriptor);

        // system indices feature object
        SystemIndices.Feature feature = new SystemIndices.Feature("test-feature", "feature for tests", descriptors);
        return feature;
    }

    private static ClusterState getClusterState() {
        IndexMetadata indexMetadata1 = IndexMetadata.builder(".test-index-1")
            .settings(Settings.builder().put("index.version.created", IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexMetadata indexMetadata2 = IndexMetadata.builder(".test-index-2")
            .settings(Settings.builder().put("index.version.created", TEST_OLD_VERSION).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = new ClusterState.Builder(ClusterState.EMPTY_STATE).metadata(
            new Metadata.Builder().indices(Map.of(".test-index-1", indexMetadata1, ".test-index-2", indexMetadata2)).build()
        ).build();
        return clusterState;
    }
}
