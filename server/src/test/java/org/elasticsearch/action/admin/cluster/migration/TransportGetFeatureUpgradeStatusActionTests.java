/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.migration;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.admin.cluster.migration.GetFeatureUpgradeStatusResponse.UpgradeStatus.MIGRATION_NEEDED;
import static org.hamcrest.Matchers.equalTo;

public class TransportGetFeatureUpgradeStatusActionTests extends ESTestCase {

    public static String TEST_SYSTEM_INDEX_PATTERN = ".test*";
    private static final ClusterState CLUSTER_STATE = getClusterState();
    private static final SystemIndices.Feature FEATURE = getFeature();

    public void testGetFeatureStatus() {
        GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus status = TransportGetFeatureUpgradeStatusAction.getFeatureUpgradeStatus(
            CLUSTER_STATE,
            FEATURE
        );

        assertThat(status.getUpgradeStatus(), equalTo(MIGRATION_NEEDED));
        assertThat(status.getFeatureName(), equalTo("test-feature"));
        assertThat(status.getMinimumIndexVersion(), equalTo(Version.V_7_0_0));
        assertThat(status.getIndexVersions().size(), equalTo(2)); // additional testing below
    }

    public void testGetIndexInfos() {
        List<GetFeatureUpgradeStatusResponse.IndexInfo> versions = TransportGetFeatureUpgradeStatusAction.getIndexInfos(
            CLUSTER_STATE,
            FEATURE
        );

        assertThat(versions.size(), equalTo(2));

        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(0);
            assertThat(version.getVersion(), equalTo(Version.CURRENT));
            assertThat(version.getIndexName(), equalTo(".test-index-1"));
        }
        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(1);
            assertThat(version.getVersion(), equalTo(Version.V_7_0_0));
            assertThat(version.getIndexName(), equalTo(".test-index-2"));
        }
    }

    private static SystemIndices.Feature getFeature() {
        SystemIndexDescriptor descriptor = new SystemIndexDescriptor(TEST_SYSTEM_INDEX_PATTERN, "descriptor for tests");

        List<SystemIndexDescriptor> descriptors = new ArrayList<>();
        descriptors.add(descriptor);

        // system indices feature object
        SystemIndices.Feature feature = new SystemIndices.Feature("test-feature", "feature for tests", descriptors);
        return feature;
    }

    private static ClusterState getClusterState() {
        IndexMetadata indexMetadata1 = IndexMetadata.builder(".test-index-1")
            .settings(Settings.builder().put("index.version.created", Version.CURRENT).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexMetadata indexMetadata2 = IndexMetadata.builder(".test-index-2")
            .settings(Settings.builder().put("index.version.created", Version.V_7_0_0).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        ClusterState clusterState = new ClusterState.Builder(ClusterState.EMPTY_STATE).metadata(
            new Metadata.Builder().indices(
                ImmutableOpenMap.<String, IndexMetadata>builder()
                    .fPut(".test-index-1", indexMetadata1)
                    .fPut(".test-index-2", indexMetadata2)
                    .build()
            ).build()
        ).build();
        return clusterState;
    }
}
