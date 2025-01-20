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
import org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static java.util.Map.ofEntries;

public class DataStreamDeprecationChecks {
    static DeprecationIssue oldIndicesCheck(DataStream dataStream, ClusterState clusterState) {
        List<Index> backingIndices = dataStream.getIndices();

        Set<String> indicesNeedingUpgrade = backingIndices.stream()
            .filter(DeprecatedIndexPredicate.getReindexRequiredPredicate(clusterState.metadata()))
            .map(Index::getName)
            .collect(Collectors.toUnmodifiableSet());

        if (indicesNeedingUpgrade.isEmpty() == false) {
            return new DeprecationIssue(
                DeprecationIssue.Level.CRITICAL,
                "Old data stream with a compatibility version < 9.0",
                "https://www.elastic.co/guide/en/elasticsearch/reference/master/breaking-changes-9.0.html",
                "This data stream has backing indices that were created before Elasticsearch 9.0.0",
                false,
                ofEntries(
                    entry("reindex_required", true),
                    entry("total_backing_indices", backingIndices.size()),
                    entry("indices_requiring_upgrade_count", indicesNeedingUpgrade.size()),
                    entry("indices_requiring_upgrade", indicesNeedingUpgrade)
                )
            );
        }

        return null;
    }
}
