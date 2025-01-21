/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.core.deprecation.DeprecatedIndexPredicate;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static java.util.Map.ofEntries;
import static org.elasticsearch.xpack.deprecation.DeprecationInfoAction.filterChecks;

/**
 * Checks the data streams for deprecation warnings.
 */
public class DataStreamDeprecationChecks implements ResourceDeprecationChecker {

    public static final String DATA_STREAMS_CHECKER_NAME = "data_streams";
    private static final List<BiFunction<DataStream, ClusterState, DeprecationIssue>> DATA_STREAM_CHECKS = List.of(
        DataStreamDeprecationChecks::oldIndicesCheck
    );
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public DataStreamDeprecationChecks(IndexNameExpressionResolver indexNameExpressionResolver) {
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    /**
     * @param clusterState The cluster state provided for the checker
     * @return the name of the data streams that have violated the checks with their respective warnings.
     */
    @Override
    public Map<String, List<DeprecationIssue>> check(ClusterState clusterState) {
        List<String> dataStreamNames = indexNameExpressionResolver.dataStreamNames(
            clusterState,
            IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN
        );
        if (dataStreamNames.isEmpty()) {
            return Map.of();
        }
        Map<String, List<DeprecationIssue>> dataStreamIssues = new HashMap<>();
        for (String dataStreamName : dataStreamNames) {
            DataStream dataStream = clusterState.metadata().dataStreams().get(dataStreamName);
            List<DeprecationIssue> issuesForSingleDataStream = filterChecks(DATA_STREAM_CHECKS, c -> c.apply(dataStream, clusterState));
            if (issuesForSingleDataStream.isEmpty() == false) {
                dataStreamIssues.put(dataStreamName, issuesForSingleDataStream);
            }
        }
        return dataStreamIssues.isEmpty() ? Map.of() : dataStreamIssues;
    }

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

    @Override
    public boolean enabled(Settings settings) {
        return true;
    }

    @Override
    public String getName() {
        return DATA_STREAMS_CHECKER_NAME;
    }
}
