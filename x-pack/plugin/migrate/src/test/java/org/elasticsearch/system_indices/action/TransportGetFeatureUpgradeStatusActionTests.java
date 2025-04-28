/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.system_indices.action;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.system_indices.task.FeatureMigrationResults;
import org.elasticsearch.system_indices.task.SingleFeatureMigrationResult;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.system_indices.action.GetFeatureUpgradeStatusResponse.UpgradeStatus.MIGRATION_NEEDED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TransportGetFeatureUpgradeStatusActionTests extends ESTestCase {

    private static final String DATA_STREAM_NAME = ".test-ds";
    private static final String BACKING_INDEX_NAME = DataStream.BACKING_INDEX_PREFIX + DATA_STREAM_NAME + "-1";
    private static final String FEATURE_NAME = "test-feature";
    private static String TEST_SYSTEM_INDEX_PATTERN = ".test*";
    // Version just before MINIMUM_COMPATIBLE in order to check that UpgradeStatus.MIGRATION_NEEDED is set correctly
    private static final IndexVersion TEST_OLD_VERSION = IndexVersion.fromId(IndexVersions.MINIMUM_COMPATIBLE.id() - 1);
    private static final ClusterState CLUSTER_STATE = getClusterState();
    private static final String TEST_INDEX_1_NAME = ".test-index-1";

    private static final SystemIndices.Feature FEATURE = getFeature();

    public void testGetFeatureStatus() {
        GetFeatureUpgradeStatusResponse.FeatureUpgradeStatus status = TransportGetFeatureUpgradeStatusAction.getFeatureUpgradeStatus(
            CLUSTER_STATE,
            FEATURE
        );

        assertThat(status.getUpgradeStatus(), equalTo(MIGRATION_NEEDED));
        assertThat(status.getFeatureName(), equalTo(FEATURE_NAME));
        assertThat(status.getMinimumIndexVersion(), equalTo(TEST_OLD_VERSION));
        assertThat(status.getIndexVersions(), hasSize(3)); // additional testing below
    }

    public void testGetIndexInfos() {
        List<GetFeatureUpgradeStatusResponse.IndexInfo> versions = TransportGetFeatureUpgradeStatusAction.getIndexInfos(
            CLUSTER_STATE,
            FEATURE
        );

        assertThat(versions, hasSize(3));

        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(0);
            assertThat(version.getVersion(), equalTo(IndexVersion.current()));
            assertThat(version.getIndexName(), equalTo(TEST_INDEX_1_NAME));
        }
        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(1);
            assertThat(version.getVersion(), equalTo(TEST_OLD_VERSION));
            assertThat(version.getIndexName(), equalTo(".test-index-2"));
        }
        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(2);
            assertThat(version.getVersion(), equalTo(TEST_OLD_VERSION));
            assertThat(version.getIndexName(), equalTo(BACKING_INDEX_NAME));
        }
    }

    public void testGetIndexInfosWithErrors() {
        List<GetFeatureUpgradeStatusResponse.IndexInfo> versions = TransportGetFeatureUpgradeStatusAction.getIndexInfos(
            getClusterStateWithFailedMigration(TEST_INDEX_1_NAME),
            FEATURE
        );

        assertThat(versions, hasSize(3));

        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(0);
            assertThat(version.getVersion(), equalTo(IndexVersion.current()));
            assertThat(version.getIndexName(), equalTo(TEST_INDEX_1_NAME));
            assertNotNull(version.getException());
        }
        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(1);
            assertThat(version.getVersion(), equalTo(TEST_OLD_VERSION));
            assertThat(version.getIndexName(), equalTo(".test-index-2"));
            assertNull(version.getException());
        }
        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(2);
            assertThat(version.getVersion(), equalTo(TEST_OLD_VERSION));
            assertThat(version.getIndexName(), equalTo(BACKING_INDEX_NAME));
            assertNull(version.getException());
        }
    }

    public void testGetIndexInfosWithDataStreamErrors() {
        List<GetFeatureUpgradeStatusResponse.IndexInfo> versions = TransportGetFeatureUpgradeStatusAction.getIndexInfos(
            getClusterStateWithFailedMigration(DATA_STREAM_NAME),
            FEATURE
        );

        assertThat(versions, hasSize(3));

        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(0);
            assertThat(version.getVersion(), equalTo(IndexVersion.current()));
            assertThat(version.getIndexName(), equalTo(TEST_INDEX_1_NAME));
            assertNull(version.getException());
        }
        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(1);
            assertThat(version.getVersion(), equalTo(TEST_OLD_VERSION));
            assertThat(version.getIndexName(), equalTo(".test-index-2"));
            assertNull(version.getException());
        }
        {
            GetFeatureUpgradeStatusResponse.IndexInfo version = versions.get(2);
            assertThat(version.getVersion(), equalTo(TEST_OLD_VERSION));
            assertThat(version.getIndexName(), equalTo(BACKING_INDEX_NAME));
            assertNotNull(version.getException());
        }
    }

    private static SystemIndices.Feature getFeature() {
        SystemIndexDescriptor descriptor = SystemIndexDescriptorUtils.createUnmanaged(TEST_SYSTEM_INDEX_PATTERN, "descriptor for tests");
        SystemDataStreamDescriptor dataStreamDescriptor = new SystemDataStreamDescriptor(
            DATA_STREAM_NAME,
            "test data stream",
            SystemDataStreamDescriptor.Type.INTERNAL,
            ComposableIndexTemplate.builder().build(),
            Map.of(),
            Collections.singletonList("origin"),
            "origin",
            ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
        );

        List<SystemIndexDescriptor> descriptors = new ArrayList<>();
        descriptors.add(descriptor);

        // system indices feature object
        SystemIndices.Feature feature = new SystemIndices.Feature(
            FEATURE_NAME,
            "feature for tests",
            List.of(descriptor),
            List.of(dataStreamDescriptor)
        );
        return feature;
    }

    private static ClusterState getClusterState() {
        IndexMetadata indexMetadata1 = IndexMetadata.builder(TEST_INDEX_1_NAME)
            .settings(Settings.builder().put("index.version.created", IndexVersion.current()).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(true)
            .build();

        IndexMetadata indexMetadata2 = IndexMetadata.builder(".test-index-2")
            .settings(Settings.builder().put("index.version.created", TEST_OLD_VERSION).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(true)
            .build();
        IndexMetadata dsIndexMetadata = IndexMetadata.builder(BACKING_INDEX_NAME)
            .settings(Settings.builder().put("index.version.created", TEST_OLD_VERSION).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .system(true)
            .build();

        DataStream dataStream = DataStream.builder(DATA_STREAM_NAME, List.of(dsIndexMetadata.getIndex()))
            .setSystem(true)
            .setHidden(true)
            .build();

        ClusterState clusterState = new ClusterState.Builder(ClusterState.EMPTY_STATE).metadata(
            new Metadata.Builder().dataStreams(Map.of(DATA_STREAM_NAME, dataStream), Collections.emptyMap())
                .indices(Map.of(TEST_INDEX_1_NAME, indexMetadata1, ".test-index-2", indexMetadata2, BACKING_INDEX_NAME, dsIndexMetadata))
                .build()
        ).build();
        return clusterState;
    }

    private static ClusterState getClusterStateWithFailedMigration(String failedIndexName) {
        SingleFeatureMigrationResult migrationResult = SingleFeatureMigrationResult.failure(failedIndexName, new Exception());
        FeatureMigrationResults featureMigrationResults = new FeatureMigrationResults(Map.of(FEATURE_NAME, migrationResult));

        ClusterState initialState = getClusterState();
        return ClusterState.builder(initialState)
            .metadata(Metadata.builder(initialState.metadata()).putCustom(FeatureMigrationResults.TYPE, featureMigrationResults).build())
            .build();
    }
}
