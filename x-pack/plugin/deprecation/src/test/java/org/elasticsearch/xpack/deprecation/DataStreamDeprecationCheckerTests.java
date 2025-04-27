/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static java.util.Map.ofEntries;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamDeprecationCheckerTests extends ESTestCase {

    private final DataStreamDeprecationChecker checker = new DataStreamDeprecationChecker(TestIndexNameExpressionResolver.newInstance());

    public void testOldIndicesCheck() {
        int oldIndexCount = randomIntBetween(1, 100);
        int newIndexCount = randomIntBetween(1, 100);

        Map<String, IndexMetadata> nameToIndexMetadata = new HashMap<>();
        Set<String> expectedIndices = new HashSet<>();

        DataStream dataStream = createTestDataStream(oldIndexCount, 0, newIndexCount, 0, nameToIndexMetadata, expectedIndices);

        Metadata metadata = Metadata.builder()
            .indices(nameToIndexMetadata)
            .dataStreams(Map.of(dataStream.getName(), dataStream), Map.of())
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Old data stream with a compatibility version < " + Version.CURRENT.major + ".0",
            "https://ela.st/es-deprecation-ds-reindex",
            "This data stream has backing indices that were created before Elasticsearch " + Version.CURRENT.major + ".0",
            false,
            ofEntries(
                entry("reindex_required", true),
                entry("total_backing_indices", oldIndexCount + newIndexCount),
                entry("indices_requiring_upgrade_count", expectedIndices.size()),
                entry("indices_requiring_upgrade", expectedIndices)
            )
        );

        // We know that the data stream checks ignore the request.
        Map<String, List<DeprecationIssue>> issuesByDataStream = checker.check(clusterState);
        assertThat(issuesByDataStream.size(), equalTo(1));
        assertThat(issuesByDataStream.containsKey(dataStream.getName()), equalTo(true));
        assertThat(issuesByDataStream.get(dataStream.getName()), equalTo(List.of(expected)));
    }

    public void testOldIndicesCheckWithOnlyNewIndices() {
        // This tests what happens when any old indices that we have are closed. We expect no deprecation warning.
        int newOpenIndexCount = randomIntBetween(0, 100);
        int newClosedIndexCount = randomIntBetween(0, 100);

        Map<String, IndexMetadata> nameToIndexMetadata = new HashMap<>();
        Set<String> expectedIndices = new HashSet<>();

        DataStream dataStream = createTestDataStream(0, 0, newOpenIndexCount, newClosedIndexCount, nameToIndexMetadata, expectedIndices);

        Metadata metadata = Metadata.builder()
            .indices(nameToIndexMetadata)
            .dataStreams(Map.of(dataStream.getName(), dataStream), Map.of())
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        Map<String, List<DeprecationIssue>> issuesByDataStream = checker.check(clusterState);
        assertThat(issuesByDataStream.size(), equalTo(0));
    }

    public void testOldIndicesCheckWithClosedAndOpenIndices() {
        /*
         * This tests what happens when a data stream has old indices, and some are open and some are closed. We expect a deprecation
         * warning that includes information about the old ones only.
         */
        int oldOpenIndexCount = randomIntBetween(1, 100);
        int oldClosedIndexCount = randomIntBetween(1, 100);
        int newOpenIndexCount = randomIntBetween(0, 100);
        int newClosedIndexCount = randomIntBetween(0, 100);

        Map<String, IndexMetadata> nameToIndexMetadata = new HashMap<>();
        Set<String> expectedIndices = new HashSet<>();

        DataStream dataStream = createTestDataStream(
            oldOpenIndexCount,
            oldClosedIndexCount,
            newOpenIndexCount,
            newClosedIndexCount,
            nameToIndexMetadata,
            expectedIndices
        );

        Metadata metadata = Metadata.builder()
            .indices(nameToIndexMetadata)
            .dataStreams(Map.of(dataStream.getName(), dataStream), Map.of())
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Old data stream with a compatibility version < " + Version.CURRENT.major + ".0",
            "https://ela.st/es-deprecation-ds-reindex",
            "This data stream has backing indices that were created before Elasticsearch " + Version.CURRENT.major + ".0",
            false,
            ofEntries(
                entry("reindex_required", true),
                entry("total_backing_indices", oldOpenIndexCount + oldClosedIndexCount + newOpenIndexCount + newClosedIndexCount),
                entry("indices_requiring_upgrade_count", expectedIndices.size()),
                entry("indices_requiring_upgrade", expectedIndices)
            )
        );

        Map<String, List<DeprecationIssue>> issuesByDataStream = checker.check(clusterState);
        assertThat(issuesByDataStream.containsKey(dataStream.getName()), equalTo(true));
        assertThat(issuesByDataStream.get(dataStream.getName()), equalTo(List.of(expected)));
    }

    /*
     * This creates a test DataStream with the given counts. The nameToIndexMetadata Map and the expectedIndices Set are mutable collections
     * that will be populated by this method.
     */
    private DataStream createTestDataStream(
        int oldOpenIndexCount,
        int oldClosedIndexCount,
        int newOpenIndexCount,
        int newClosedIndexCount,
        Map<String, IndexMetadata> nameToIndexMetadata,
        Set<String> expectedIndices
    ) {
        List<Index> allIndices = new ArrayList<>();

        for (int i = 0; i < oldOpenIndexCount; i++) {
            allIndices.add(createOldIndex(i, false, nameToIndexMetadata, expectedIndices));
        }
        for (int i = 0; i < oldClosedIndexCount; i++) {
            allIndices.add(createOldIndex(i, true, nameToIndexMetadata, expectedIndices));
        }
        for (int i = 0; i < newOpenIndexCount; i++) {
            allIndices.add(createNewIndex(i, false, nameToIndexMetadata));
        }
        for (int i = 0; i < newClosedIndexCount; i++) {
            allIndices.add(createNewIndex(i, true, nameToIndexMetadata));
        }

        DataStream dataStream = new DataStream(
            randomAlphaOfLength(10),
            allIndices,
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
        return dataStream;
    }

    private Index createOldIndex(
        int suffix,
        boolean isClosed,
        Map<String, IndexMetadata> nameToIndexMetadata,
        Set<String> expectedIndices
    ) {
        return createIndex(true, suffix, isClosed, nameToIndexMetadata, expectedIndices);
    }

    private Index createNewIndex(int suffix, boolean isClosed, Map<String, IndexMetadata> nameToIndexMetadata) {
        return createIndex(false, suffix, isClosed, nameToIndexMetadata, null);
    }

    private Index createIndex(
        boolean isOld,
        int suffix,
        boolean isClosed,
        Map<String, IndexMetadata> nameToIndexMetadata,
        Set<String> expectedIndices
    ) {
        Settings.Builder settingsBuilder = isOld ? settings(IndexVersion.fromId(7170099)) : settings(IndexVersion.current());
        String indexName = (isOld ? "old-" : "new-") + (isClosed ? "closed-" : "") + "data-stream-index-" + suffix;
        if (isOld) {
            if (expectedIndices.isEmpty() == false && randomIntBetween(0, 2) == 0) {
                settingsBuilder.put(INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
            } else {
                expectedIndices.add(indexName);
            }
        }
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settingsBuilder)
            .numberOfShards(1)
            .numberOfReplicas(0);
        if (isClosed) {
            indexMetadataBuilder.state(IndexMetadata.State.CLOSE);
        }
        IndexMetadata indexMetadata = indexMetadataBuilder.build();
        nameToIndexMetadata.put(indexMetadata.getIndex().getName(), indexMetadata);
        return indexMetadata.getIndex();
    }

    public void testOldIndicesIgnoredWarningCheck() {
        int oldIndexCount = randomIntBetween(1, 100);
        int newIndexCount = randomIntBetween(1, 100);

        List<Index> allIndices = new ArrayList<>();
        Map<String, IndexMetadata> nameToIndexMetadata = new HashMap<>();
        Set<String> expectedIndices = new HashSet<>();

        for (int i = 0; i < oldIndexCount; i++) {
            Settings.Builder settings = settings(IndexVersion.fromId(7170099));

            String indexName = "old-data-stream-index-" + i;
            settings.put(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey(), true);
            expectedIndices.add(indexName);

            Settings.Builder settingsBuilder = settings;
            IndexMetadata oldIndexMetadata = IndexMetadata.builder(indexName)
                .settings(settingsBuilder)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            allIndices.add(oldIndexMetadata.getIndex());
            nameToIndexMetadata.put(oldIndexMetadata.getIndex().getName(), oldIndexMetadata);
        }

        for (int i = 0; i < newIndexCount; i++) {
            Index newIndex = createNewIndex(i, false, nameToIndexMetadata);
            allIndices.add(newIndex);
        }

        DataStream dataStream = new DataStream(
            randomAlphaOfLength(10),
            allIndices,
            randomLong() * -1L,
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

        Metadata metadata = Metadata.builder()
            .indices(nameToIndexMetadata)
            .dataStreams(Map.of(dataStream.getName(), dataStream), Map.of())
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.WARNING,
            "Old data stream with a compatibility version < " + Version.CURRENT.major + ".0 has Been Ignored",
            "https://ela.st/es-deprecation-ds-reindex",
            "This data stream has read only backing indices that were created before Elasticsearch "
                + Version.CURRENT.major
                + ".0 and have been marked as OK to remain read-only after upgrade",
            false,
            ofEntries(
                entry("reindex_required", false),
                entry("total_backing_indices", oldIndexCount + newIndexCount),
                entry("ignored_indices_requiring_upgrade_count", expectedIndices.size()),
                entry("ignored_indices_requiring_upgrade", expectedIndices)
            )
        );

        Map<String, List<DeprecationIssue>> issuesByDataStream = checker.check(clusterState);
        assertThat(issuesByDataStream.containsKey(dataStream.getName()), equalTo(true));
        assertThat(issuesByDataStream.get(dataStream.getName()), equalTo(List.of(expected)));
    }

    public void testOldSystemDataStreamIgnored() {
        // We do not want system data streams coming back in the deprecation info API
        int oldIndexCount = randomIntBetween(1, 100);
        int newIndexCount = randomIntBetween(1, 100);
        List<Index> allIndices = new ArrayList<>();
        Map<String, IndexMetadata> nameToIndexMetadata = new HashMap<>();
        for (int i = 0; i < oldIndexCount; i++) {
            Settings.Builder settings = settings(IndexVersion.fromId(7170099));

            String indexName = "old-data-stream-index-" + i;
            settings.put(MetadataIndexStateService.VERIFIED_READ_ONLY_SETTING.getKey(), true);

            IndexMetadata oldIndexMetadata = IndexMetadata.builder(indexName)
                .settings(settings)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            allIndices.add(oldIndexMetadata.getIndex());
            nameToIndexMetadata.put(oldIndexMetadata.getIndex().getName(), oldIndexMetadata);
        }
        for (int i = 0; i < newIndexCount; i++) {
            Index newIndex = createNewIndex(i, false, nameToIndexMetadata);
            allIndices.add(newIndex);
        }
        DataStream dataStream = new DataStream(
            randomAlphaOfLength(10),
            allIndices,
            randomNonNegativeLong(),
            Map.of(),
            true,
            false,
            true,
            randomBoolean(),
            randomFrom(IndexMode.values()),
            null,
            randomFrom(DataStreamOptions.EMPTY, DataStreamOptions.FAILURE_STORE_DISABLED, DataStreamOptions.FAILURE_STORE_ENABLED, null),
            List.of(),
            randomBoolean(),
            null
        );
        Metadata metadata = Metadata.builder()
            .indices(nameToIndexMetadata)
            .dataStreams(Map.of(dataStream.getName(), dataStream), Map.of())
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        assertThat(checker.check(clusterState), equalTo(Map.of()));
    }

}
