/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static java.util.Map.entry;
import static java.util.Map.ofEntries;
import static org.elasticsearch.index.IndexModule.INDEX_STORE_TYPE_SETTING;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.DATA_STREAM_CHECKS;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamDeprecationChecksTests extends ESTestCase {

    public void testOldIndicesCheck() {
        int oldIndexCount = randomIntBetween(1, 100);
        int newIndexCount = randomIntBetween(1, 100);

        List<Index> allIndices = new ArrayList<>();
        Map<String, IndexMetadata> nameToIndexMetadata = new HashMap<>();
        Set<String> expectedIndices = new HashSet<>();

        for (int i = 0; i < oldIndexCount; i++) {
            Settings.Builder settings = settings(IndexVersion.fromId(7170099));

            String indexName = "old-data-stream-index-" + i;
            if (expectedIndices.isEmpty() == false && randomIntBetween(0, 2) == 0) {
                settings.put(INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
            } else {
                expectedIndices.add(indexName);
            }

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
            Settings.Builder settingsBuilder = settings(IndexVersion.current());
            IndexMetadata newIndexMetadata = IndexMetadata.builder("new-data-stream-index-" + i)
                .settings(settingsBuilder)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            allIndices.add(newIndexMetadata.getIndex());
            nameToIndexMetadata.put(newIndexMetadata.getIndex().getName(), newIndexMetadata);
        }

        DataStream dataStream = new DataStream(
            randomAlphaOfLength(10),
            allIndices,
            randomNegativeLong(),
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

        Metadata metadata = Metadata.builder().indices(nameToIndexMetadata).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();

        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Old data stream with a compatibility version < 9.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-9.0.html",
            "This data stream has backing indices that were created before Elasticsearch 9.0.0",
            false,
            ofEntries(
                entry("reindex_required", true),
                entry("total_backing_indices", oldIndexCount + newIndexCount),
                entry("indices_requiring_upgrade_count", expectedIndices.size()),
                entry("indices_requiring_upgrade", expectedIndices)
            )
        );

        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(DATA_STREAM_CHECKS, c -> c.apply(dataStream, clusterState));

        assertThat(issues, equalTo(singletonList(expected)));
    }

}
