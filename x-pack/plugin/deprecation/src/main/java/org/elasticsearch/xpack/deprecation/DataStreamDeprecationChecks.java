/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;

import static java.util.Map.entry;
import static java.util.Map.ofEntries;

public class DataStreamDeprecationChecks {
    static DeprecationIssue oldIndicesCheck(DataStream dataStream, ClusterState clusterState) {
        List<Index> backingIndices = dataStream.getIndices();
        boolean hasOldIndices = backingIndices.stream()
            .anyMatch(index -> clusterState.metadata().index(index).getCompatibilityVersion().before(IndexVersions.V_8_0_0));
        if (hasOldIndices) {
            long totalIndices = backingIndices.size();
            List<Index> oldIndices = backingIndices.stream()
                .filter(index -> clusterState.metadata().index(index).getCompatibilityVersion().before(IndexVersions.V_8_0_0))
                .toList();
            long totalOldIndices = oldIndices.size();
            long totalOldSearchableSnapshots = oldIndices.stream()
                .filter(index -> clusterState.metadata().index(index).isSearchableSnapshot())
                .count();
            long totalOldPartiallyMountedSearchableSnapshots = oldIndices.stream()
                .filter(index -> clusterState.metadata().index(index).isPartialSearchableSnapshot())
                .count();
            long totalOldFullyMountedSearchableSnapshots = totalOldSearchableSnapshots - totalOldPartiallyMountedSearchableSnapshots;
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Old data stream with a compatibility version < 8.0",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-9.0.html",
                "This data stream has backing indices that were created before Elasticsearch 8.0.0",
                false,
                ofEntries(
                    entry(
                        "backing_indices",
                        ofEntries(
                            entry("count", totalIndices),
                            entry(
                                "need_upgrading",
                                ofEntries(
                                    entry("count", totalOldIndices),
                                    entry(
                                        "searchable_snapshots",
                                        ofEntries(
                                            entry("count", totalOldSearchableSnapshots),
                                            entry("fully_mounted", ofEntries(entry("count", totalOldFullyMountedSearchableSnapshots))),
                                            entry(
                                                "partially_mounted",
                                                ofEntries(entry("count", totalOldPartiallyMountedSearchableSnapshots))
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            );
        }
        return null;
    }
}
