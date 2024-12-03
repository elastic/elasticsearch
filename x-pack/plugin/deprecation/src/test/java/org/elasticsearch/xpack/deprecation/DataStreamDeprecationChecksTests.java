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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.deprecation.DeprecationChecks.DATA_STREAM_CHECKS;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamDeprecationChecksTests extends ESTestCase {

    public void testOldIndicesCheck() {
        long oldIndexCount = randomIntBetween(1, 100);
        long newIndexCount = randomIntBetween(1, 100);
        long oldSearchableSnapshotCount = 0;
        long oldFullyManagedSearchableSnapshotCount = 0;
        long oldPartiallyManagedSearchableSnapshotCount = 0;
        List<Index> allIndices = new ArrayList<>();
        Map<String, IndexMetadata> nameToIndexMetadata = new HashMap<>();
        for (int i = 0; i < oldIndexCount; i++) {
            Settings.Builder settingsBuilder = settings(IndexVersion.fromId(7170099));
            if (randomBoolean()) {
                settingsBuilder.put("index.store.type", "snapshot");
                if (randomBoolean()) {
                    oldFullyManagedSearchableSnapshotCount++;
                } else {
                    settingsBuilder.put("index.store.snapshot.partial", true);
                    oldPartiallyManagedSearchableSnapshotCount++;
                }
                oldSearchableSnapshotCount++;
            }
            IndexMetadata oldIndexMetadata = IndexMetadata.builder("old-data-stream-index-" + i)
                .settings(settingsBuilder)
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            allIndices.add(oldIndexMetadata.getIndex());
            nameToIndexMetadata.put(oldIndexMetadata.getIndex().getName(), oldIndexMetadata);
        }
        for (int i = 0; i < newIndexCount; i++) {
            Settings.Builder settingsBuilder = settings(IndexVersion.current());
            if (randomBoolean()) {
                settingsBuilder.put("index.store.type", "snapshot");
            }
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
        Metadata metadata = Metadata.builder().indices(nameToIndexMetadata).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build();
        DeprecationIssue expected = new DeprecationIssue(
            DeprecationIssue.Level.CRITICAL,
            "Old data stream with a compatibility version < 8.0",
            "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-9.0.html",
            "This data stream has backing indices that were created before Elasticsearch 8.0.0",
            false,
            Map.of(
                "backing_indices",
                Map.of(
                    "count",
                    oldIndexCount + newIndexCount,
                    "need_upgrading",
                    Map.of(
                        "count",
                        oldIndexCount,
                        "searchable_snapshots",
                        Map.of(
                            "count",
                            oldSearchableSnapshotCount,
                            "fully_mounted",
                            Map.of("count", oldFullyManagedSearchableSnapshotCount),
                            "partially_mounted",
                            Map.of("count", oldPartiallyManagedSearchableSnapshotCount)
                        )
                    )
                )
            )
        );
        List<DeprecationIssue> issues = DeprecationChecks.filterChecks(DATA_STREAM_CHECKS, c -> c.apply(dataStream, clusterState));
        assertThat(issues, equalTo(singletonList(expected)));
    }
}
