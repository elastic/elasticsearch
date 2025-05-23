/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.frozen.FrozenEngine;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.transforms.DestConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class IndexDeprecationCheckerTests extends ESTestCase {

    private static final IndexVersion OLD_VERSION = IndexVersion.fromId(7170099);
    private final IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance();
    private final IndexDeprecationChecker checker = new IndexDeprecationChecker(indexNameExpressionResolver);
    private final TransportDeprecationInfoAction.PrecomputedData emptyPrecomputedData =
        new TransportDeprecationInfoAction.PrecomputedData();
    private final IndexMetadata.State indexMetdataState;

    public IndexDeprecationCheckerTests(@Name("indexMetadataState") IndexMetadata.State indexMetdataState) {
        this.indexMetdataState = indexMetdataState;
        emptyPrecomputedData.setOnceNodeSettingsIssues(List.of());
        emptyPrecomputedData.setOncePluginIssues(Map.of());
        emptyPrecomputedData.setOnceTransformConfigs(List.of());
    }

    @ParametersFactory
    public static List<Object[]> createParameters() {
        return List.of(new Object[] { IndexMetadata.State.OPEN }, new Object[] { IndexMetadata.State.CLOSE });
    }

    public void testOldIndicesCheck() {
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(OLD_VERSION))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Old index with a compatibility version < " + Version.CURRENT.major + ".0",
            "https://ela.st/es-deprecation-9-index-version",
            "This index has version: " + OLD_VERSION.toReleaseVersion(),
            false,
            singletonMap("reindex_required", true)
        );
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        List<DeprecationIssue> issues = issuesByIndex.get("test");
        assertEquals(singletonList(expected), issues);
    }

    public void testOldTransformIndicesCheck() {
        var checker = new IndexDeprecationChecker(indexNameExpressionResolver);
        var indexMetadata = indexMetadata("test", OLD_VERSION);
        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        var expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "One or more Transforms write to this index with a compatibility version < " + Version.CURRENT.major + ".0",
            "https://ela.st/es-deprecation-9-transform-destination-index",
            "This index was created in version ["
                + OLD_VERSION.toReleaseVersion()
                + "] and requires action before upgrading to "
                + Version.CURRENT.major
                + ".0. "
                + "The following transforms are configured to write to this index: [test-transform]. Refer to the "
                + "migration guide to learn more about how to prepare transforms destination indices for your upgrade.",
            false,
            Map.of("reindex_required", true, "transform_ids", List.of("test-transform"))
        );
        var issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            createContextWithTransformConfigs(Map.of("test", List.of("test-transform")))
        );
        assertEquals(singletonList(expected), issuesByIndex.get("test"));
    }

    public void testOldIndicesCheckWithMultipleTransforms() {
        var indexMetadata = indexMetadata("test", OLD_VERSION);
        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        var expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "One or more Transforms write to this index with a compatibility version < " + Version.CURRENT.major + ".0",
            "https://ela.st/es-deprecation-9-transform-destination-index",
            "This index was created in version ["
                + OLD_VERSION.toReleaseVersion()
                + "] and requires action before upgrading to "
                + Version.CURRENT.major
                + ".0. "
                + "The following transforms are configured to write to this index: [test-transform1, test-transform2]. Refer to the "
                + "migration guide to learn more about how to prepare transforms destination indices for your upgrade.",
            false,
            Map.of("reindex_required", true, "transform_ids", List.of("test-transform1", "test-transform2"))
        );
        var issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            createContextWithTransformConfigs(Map.of("test", List.of("test-transform1", "test-transform2")))
        );
        assertEquals(singletonList(expected), issuesByIndex.get("test"));
    }

    public void testMultipleOldIndicesCheckWithTransforms() {
        var indexMetadata1 = indexMetadata("test1", OLD_VERSION);
        var indexMetadata2 = indexMetadata("test2", OLD_VERSION);
        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata1, true).put(indexMetadata2, true))
            .blocks(clusterBlocksForIndices(indexMetadata1, indexMetadata2))
            .build();
        var expected = Map.of(
            "test1",
            List.of(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "One or more Transforms write to this index with a compatibility version < " + Version.CURRENT.major + ".0",
                    "https://ela.st/es-deprecation-9-transform-destination-index",
                    "This index was created in version ["
                        + OLD_VERSION.toReleaseVersion()
                        + "] and requires action before upgrading to "
                        + Version.CURRENT.major
                        + ".0. "
                        + "The following transforms are configured to write to this index: [test-transform1]. Refer to the "
                        + "migration guide to learn more about how to prepare transforms destination indices for your upgrade.",
                    false,
                    Map.of("reindex_required", true, "transform_ids", List.of("test-transform1"))
                )
            ),
            "test2",
            List.of(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "One or more Transforms write to this index with a compatibility version < " + Version.CURRENT.major + ".0",
                    "https://ela.st/es-deprecation-9-transform-destination-index",
                    "This index was created in version ["
                        + OLD_VERSION.toReleaseVersion()
                        + "] and requires action before upgrading to "
                        + Version.CURRENT.major
                        + ".0. "
                        + "The following transforms are configured to write to this index: [test-transform2]. Refer to the "
                        + "migration guide to learn more about how to prepare transforms destination indices for your upgrade.",
                    false,
                    Map.of("reindex_required", true, "transform_ids", List.of("test-transform2"))
                )
            )
        );
        var issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            createContextWithTransformConfigs(Map.of("test1", List.of("test-transform1"), "test2", List.of("test-transform2")))
        );
        assertEquals(expected, issuesByIndex);
    }

    private IndexMetadata indexMetadata(String indexName, IndexVersion indexVersion) {
        return IndexMetadata.builder(indexName)
            .settings(settings(indexVersion))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
    }

    public void testOldIndicesCheckDataStreamIndex() {
        IndexMetadata indexMetadata = IndexMetadata.builder(".ds-test")
            .settings(settings(OLD_VERSION).put("index.hidden", true))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        DataStream dataStream = new DataStream(
            randomAlphaOfLength(10),
            List.of(indexMetadata.getIndex()),
            randomNonNegativeLong(),
            Map.of(),
            randomBoolean(),
            false,
            false,
            randomBoolean(),
            randomFrom(IndexMode.values()),
            null,
            randomFrom(DataStreamOptions.EMPTY, DataStreamOptions.FAILURE_STORE_DISABLED, DataStreamOptions.FAILURE_STORE_ENABLED, null),
            List.of(),
            randomBoolean(),
            null
        );
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, true)
                    .customs(
                        Map.of(
                            DataStreamMetadata.TYPE,
                            new DataStreamMetadata(
                                ImmutableOpenMap.builder(Map.of("my-data-stream", dataStream)).build(),
                                ImmutableOpenMap.of()
                            )
                        )
                    )
            )
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        assertThat(issuesByIndex.size(), equalTo(0));
    }

    public void testOldIndicesCheckSnapshotIgnored() {
        Settings.Builder settings = settings(OLD_VERSION);
        settings.put(INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();

        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        assertThat(issuesByIndex.size(), equalTo(0));
    }

    public void testOldIndicesIgnoredWarningCheck() {
        IndexMetadata indexMetadata = readonlyIndexMetadata("test", OLD_VERSION);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Old index with a compatibility version < 8.0 has been ignored",
            "https://ela.st/es-deprecation-9-index-version",
            "This read-only index has version: "
                + OLD_VERSION.toReleaseVersion()
                + " and will be supported as read-only in "
                + Version.CURRENT.major
                + 1
                + ".0",
            false,
            singletonMap("reindex_required", true)
        );
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        assertTrue(issuesByIndex.containsKey("test"));
        assertEquals(List.of(expected), issuesByIndex.get("test"));
    }

    public void testOldSystemIndicesIgnored() {
        // We do not want system indices coming back in the deprecation info API
        Settings.Builder settings = settings(OLD_VERSION).put(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey(), true);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .system(true)
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        assertThat(issuesByIndex, equalTo(Map.of()));
    }

    private IndexMetadata readonlyIndexMetadata(String indexName, IndexVersion indexVersion) {
        Settings.Builder settings = settings(indexVersion).put(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey(), true);
        return IndexMetadata.builder(indexName).settings(settings).numberOfShards(1).numberOfReplicas(0).state(indexMetdataState).build();
    }

    public void testOldTransformIndicesIgnoredCheck() {
        var checker = new IndexDeprecationChecker(indexNameExpressionResolver);
        var indexMetadata = readonlyIndexMetadata("test", OLD_VERSION);
        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        var expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "One or more Transforms write to this old index with a compatibility version < " + Version.CURRENT.major + ".0",
            "https://ela.st/es-deprecation-9-transform-destination-index",
            "This index was created in version ["
                + OLD_VERSION.toReleaseVersion()
                + "] and will be supported as a read-only index in "
                + Version.CURRENT.major
                + ".0. "
                + "The following transforms are no longer able to write to this index: [test-transform]. Refer to the "
                + "migration guide to learn more about how to handle your transforms destination indices.",
            false,
            Map.of("reindex_required", true, "transform_ids", List.of("test-transform"))
        );
        var issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            createContextWithTransformConfigs(Map.of("test", List.of("test-transform")))
        );
        assertEquals(singletonList(expected), issuesByIndex.get("test"));
    }

    public void testOldIndicesIgnoredCheckWithMultipleTransforms() {
        var indexMetadata = readonlyIndexMetadata("test", OLD_VERSION);
        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        var expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "One or more Transforms write to this old index with a compatibility version < " + Version.CURRENT.major + ".0",
            "https://ela.st/es-deprecation-9-transform-destination-index",
            "This index was created in version ["
                + OLD_VERSION.toReleaseVersion()
                + "] and will be supported as a read-only index in "
                + Version.CURRENT.major
                + ".0. "
                + "The following transforms are no longer able to write to this index: [test-transform1, test-transform2]. Refer to the "
                + "migration guide to learn more about how to handle your transforms destination indices.",
            false,
            Map.of("reindex_required", true, "transform_ids", List.of("test-transform1", "test-transform2"))
        );
        var issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            createContextWithTransformConfigs(Map.of("test", List.of("test-transform1", "test-transform2")))
        );
        assertEquals(singletonList(expected), issuesByIndex.get("test"));
    }

    public void testMultipleOldIndicesIgnoredCheckWithTransforms() {
        var indexMetadata1 = readonlyIndexMetadata("test1", OLD_VERSION);
        var indexMetadata2 = readonlyIndexMetadata("test2", OLD_VERSION);
        var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata1, true).put(indexMetadata2, true))
            .blocks(clusterBlocksForIndices(indexMetadata1, indexMetadata2))
            .build();
        var expected = Map.of(
            "test1",
            List.of(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "One or more Transforms write to this old index with a compatibility version < " + Version.CURRENT.major + ".0",
                    "https://ela.st/es-deprecation-9-transform-destination-index",
                    "This index was created in version ["
                        + OLD_VERSION.toReleaseVersion()
                        + "] and will be supported as a read-only index in "
                        + Version.CURRENT.major
                        + ".0. "
                        + "The following transforms are no longer able to write to this index: [test-transform1]. Refer to the "
                        + "migration guide to learn more about how to handle your transforms destination indices.",
                    false,
                    Map.of("reindex_required", true, "transform_ids", List.of("test-transform1"))
                )
            ),
            "test2",
            List.of(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "One or more Transforms write to this old index with a compatibility version < " + Version.CURRENT.major + ".0",
                    "https://ela.st/es-deprecation-9-transform-destination-index",
                    "This index was created in version ["
                        + OLD_VERSION.toReleaseVersion()
                        + "] and will be supported as a read-only index in "
                        + Version.CURRENT.major
                        + ".0. "
                        + "The following transforms are no longer able to write to this index: [test-transform2]. Refer to the "
                        + "migration guide to learn more about how to handle your transforms destination indices.",
                    false,
                    Map.of("reindex_required", true, "transform_ids", List.of("test-transform2"))
                )
            )
        );
        var issuesByIndex = checker.check(
            clusterState,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            createContextWithTransformConfigs(Map.of("test1", List.of("test-transform1"), "test2", List.of("test-transform2")))
        );
        assertEquals(expected, issuesByIndex);
    }

    public void testTranslogRetentionSettings() {
        Settings.Builder settings = settings(IndexVersion.current());
        settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
        settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            state,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        List<DeprecationIssue> issues = issuesByIndex.get("test");
        assertThat(
            issues,
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "translog retention settings are ignored",
                    "https://ela.st/es-deprecation-7-translog-retention",
                    "translog retention settings [index.translog.retention.size] and [index.translog.retention.age] are ignored "
                        + "because translog is no longer used in peer recoveries with soft-deletes enabled (default in 7.0 or later)",
                    false,
                    DeprecationIssue.createMetaMapForRemovableSettings(
                        List.of(
                            IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(),
                            IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey()
                        )
                    )
                )
            )
        );
    }

    public void testDefaultTranslogRetentionSettings() {
        Settings.Builder settings = settings(IndexVersion.current());
        if (randomBoolean()) {
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING.getKey(), randomPositiveTimeValue());
            settings.put(IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING.getKey(), between(1, 1024) + "b");
            settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false);
        }
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            state,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        assertThat(issuesByIndex.size(), equalTo(0));
    }

    public void testIndexDataPathSetting() {
        Settings.Builder settings = settings(IndexVersion.current());
        settings.put(IndexMetadata.INDEX_DATA_PATH_SETTING.getKey(), createTempDir());
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            state,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        final String expectedUrl = "https://ela.st/es-deprecation-7-index-data-path";
        assertThat(
            issuesByIndex.get("test"),
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "setting [index.data_path] is deprecated and will be removed in a future version",
                    expectedUrl,
                    "Found index data path configured. Discontinue use of this setting.",
                    false,
                    null
                )
            )
        );
    }

    public void testSimpleFSSetting() {
        Settings.Builder settings = settings(IndexVersion.current());
        settings.put(INDEX_STORE_TYPE_SETTING.getKey(), "simplefs");
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            state,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        assertThat(
            issuesByIndex.get("test"),
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "[simplefs] is deprecated and will be removed in future versions",
                    "https://ela.st/es-deprecation-7-simplefs",
                    "[simplefs] is deprecated and will be removed in 8.0. Use [niofs] or other file systems instead. "
                        + "Elasticsearch 7.15 or later uses [niofs] for the [simplefs] store type "
                        + "as it offers superior or equivalent performance to [simplefs].",
                    false,
                    null
                )
            )
        );
    }

    public void testFrozenIndex() {
        Settings.Builder settings = settings(IndexVersion.current());
        settings.put(FrozenEngine.INDEX_FROZEN.getKey(), true);
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().put(indexMetadata, true)).build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            state,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        assertThat(
            issuesByIndex.get("test"),
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.CRITICAL,
                    "Index [test] is a frozen index. The frozen indices feature is deprecated and will be removed in version 9.0.",
                    "https://www.elastic.co/guide/en/elasticsearch/reference/master/frozen-indices.html",
                    "Frozen indices must be unfrozen before upgrading to version 9.0."
                        + " (The legacy frozen indices feature no longer offers any advantages."
                        + " You may consider cold or frozen tiers in place of frozen indices.)",
                    false,
                    null
                )
            )
        );
    }

    public void testCamelCaseDeprecation() {
        String simpleMapping = "{\n\"_doc\": {"
            + "\"properties\" : {\n"
            + "   \"date_time_field\" : {\n"
            + "       \"type\" : \"date\",\n"
            + "       \"format\" : \"strictDateOptionalTime\"\n"
            + "       }\n"
            + "   }"
            + "} }";

        String indexName = randomAlphaOfLengthBetween(5, 10);
        IndexMetadata simpleIndex = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putMapping(simpleMapping)
            .state(indexMetdataState)
            .build();
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(simpleIndex, true))
            .blocks(clusterBlocksForIndices(simpleIndex))
            .build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            state,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Date fields use deprecated camel case formats",
            "https://ela.st/es-deprecation-7-camel-case-format",
            "Convert [date_time_field] format [strictDateOptionalTime] "
                + "which contains deprecated camel case to snake case. [strictDateOptionalTime] to [strict_date_optional_time].",
            false,
            null
        );
        assertThat(issuesByIndex.get(indexName), hasItem(expected));
    }

    public void testLegacyTierIndex() {
        Settings.Builder settings = settings(IndexVersion.current());
        String filter = randomFrom("include", "exclude", "require");
        String tier = randomFrom("hot", "warm", "cold", "frozen");
        settings.put("index.routing.allocation." + filter + ".data", tier);
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .state(indexMetdataState)
            .build();
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put(indexMetadata, true))
            .blocks(clusterBlocksForIndices(indexMetadata))
            .build();
        Map<String, List<DeprecationIssue>> issuesByIndex = checker.check(
            state,
            new DeprecationInfoAction.Request(TimeValue.THIRTY_SECONDS),
            emptyPrecomputedData
        );
        assertThat(
            issuesByIndex.get("test"),
            contains(
                new DeprecationIssue(
                    DeprecationIssue.Level.WARNING,
                    "index [test] is configuring tiers via filtered allocation which is not recommended.",
                    "https://ela.st/migrate-to-tiers",
                    "One or more of your indices is configured with 'index.routing.allocation.*.data' settings."
                        + " This is typically used to create a hot/warm or tiered architecture, based on legacy guidelines."
                        + " Data tiers are a recommended replacement for tiered architecture clusters.",
                    false,
                    DeprecationIssue.createMetaMapForRemovableSettings(List.of("index.routing.allocation." + filter + ".data"))
                )
            )
        );
    }

    private ClusterBlocks clusterBlocksForIndices(IndexMetadata... indicesMetadatas) {
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        for (IndexMetadata indexMetadata : indicesMetadatas) {
            if (indexMetadata.getState() == IndexMetadata.State.CLOSE) {
                builder.addIndexBlock(indexMetadata.getIndex().getName(), MetadataIndexStateService.INDEX_CLOSED_BLOCK);
            }
        }
        return builder.build();
    }

    private TransportDeprecationInfoAction.PrecomputedData createContextWithTransformConfigs(Map<String, List<String>> indexToTransform) {
        List<TransformConfig> transforms = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : indexToTransform.entrySet()) {
            String index = entry.getKey();
            for (String transform : entry.getValue()) {
                transforms.add(
                    TransformConfig.builder()
                        .setId(transform)
                        .setSource(new SourceConfig(randomAlphaOfLength(10)))
                        .setDest(new DestConfig(index, List.of(), null))
                        .build()
                );
            }
        }
        TransportDeprecationInfoAction.PrecomputedData precomputedData = new TransportDeprecationInfoAction.PrecomputedData();
        precomputedData.setOnceTransformConfigs(transforms);
        return precomputedData;
    }
}
