/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Comparator;
import java.util.Locale;
import java.util.Set;

import static java.util.stream.Collectors.joining;

public class HealthIndicatorDisplayValues {

    public static String getNodeName(DiscoveryNode node) {
        if (node.getName() != null) {
            return String.format(Locale.ROOT, "[%s][%s]", node.getId(), node.getName());
        }
        return String.format(Locale.ROOT, "[%s]", node.getId());
    }

    public static String getTruncatedIndices(Set<String> indices, Metadata clusterMetadata) {
        final int maxIndices = 10;
        String truncatedIndicesString = indices.stream()
            .sorted(indicesComparatorByPriorityAndName(clusterMetadata))
            .limit(maxIndices)
            .collect(joining(", "));
        if (maxIndices < indices.size()) {
            truncatedIndicesString = truncatedIndicesString + ", ...";
        }
        return truncatedIndicesString;
    }

    /**
     * Sorts index names by their priority first, then alphabetically by name. If the priority cannot be determined for an index then
     * a priority of -1 is used to sort it behind other index names.
     * @param clusterMetadata Used to look up index priority.
     * @return Comparator instance
     */
    public static Comparator<String> indicesComparatorByPriorityAndName(Metadata clusterMetadata) {
        // We want to show indices with a numerically higher index.priority first (since lower priority ones might get truncated):
        return Comparator.comparingInt((String indexName) -> {
            IndexMetadata indexMetadata = clusterMetadata.index(indexName);
            return indexMetadata == null ? -1 : indexMetadata.priority();
        }).reversed().thenComparing(Comparator.naturalOrder());
    }
}
