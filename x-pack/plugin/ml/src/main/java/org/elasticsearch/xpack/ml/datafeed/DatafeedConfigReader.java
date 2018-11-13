/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.ExpandedIdsMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class abstracts away reading datafeed configuration from either
 * the cluster state or index documents.
 */
public class DatafeedConfigReader {

    private final DatafeedConfigProvider datafeedConfigProvider;

    public DatafeedConfigReader(Client client, NamedXContentRegistry xContentRegistry) {
        this.datafeedConfigProvider = new DatafeedConfigProvider(client, xContentRegistry);
    }

    public DatafeedConfigReader(DatafeedConfigProvider datafeedConfigProvider) {
        this.datafeedConfigProvider = datafeedConfigProvider;
    }

    /**
     * Merges the results of {@link MlMetadata#expandDatafeedIds}
     * and {@link DatafeedConfigProvider#expandDatafeedIds(String, boolean, ActionListener)}
     */
    public void expandDatafeedIds(String expression, boolean allowNoDatafeeds, ClusterState clusterState,
                                  ActionListener<SortedSet<String>> listener) {

        Set<String> clusterStateDatafeedIds = MlMetadata.getMlMetadata(clusterState).expandDatafeedIds(expression);
        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(expression, allowNoDatafeeds);
        requiredMatches.filterMatchedIds(clusterStateDatafeedIds);

        datafeedConfigProvider.expandDatafeedIdsWithoutMissingCheck(expression, ActionListener.wrap(
                expandedDatafeedIds -> {
                    // Check for duplicate Ids
                    expandedDatafeedIds.forEach(id -> {
                        if (clusterStateDatafeedIds.contains(id)) {
                            listener.onFailure(new IllegalStateException("Datafeed [" + id + "] configuration " +
                                    "exists in both clusterstate and index"));
                            return;
                        }
                    });

                    requiredMatches.filterMatchedIds(expandedDatafeedIds);

                    if (requiredMatches.hasUnmatchedIds()) {
                        listener.onFailure(ExceptionsHelper.missingDatafeedException(requiredMatches.unmatchedIdsString()));
                    } else {
                        expandedDatafeedIds.addAll(clusterStateDatafeedIds);
                        listener.onResponse(expandedDatafeedIds);
                    }
                },
               listener::onFailure
        ));
    }

    /**
     * Merges the results of {@link MlMetadata#expandDatafeedIds}
     * and {@link DatafeedConfigProvider#expandDatafeedConfigs(String, boolean, ActionListener)}
     */
    public void expandDatafeedConfigs(String expression, boolean allowNoDatafeeds, ClusterState clusterState,
                                      ActionListener<List<DatafeedConfig>> listener) {

        Map<String, DatafeedConfig> clusterStateConfigs = expandClusterStateDatafeeds(expression, clusterState);

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(expression, allowNoDatafeeds);
        requiredMatches.filterMatchedIds(clusterStateConfigs.keySet());

        datafeedConfigProvider.expandDatafeedConfigsWithoutMissingCheck(expression, ActionListener.wrap(
                datafeedBuilders -> {
                    // Check for duplicate Ids
                    datafeedBuilders.forEach(datafeedBuilder -> {
                        if (clusterStateConfigs.containsKey(datafeedBuilder.getId())) {
                            listener.onFailure(new IllegalStateException("Datafeed [" + datafeedBuilder.getId() + "] configuration " +
                                    "exists in both clusterstate and index"));
                            return;
                        }
                    });

                    List<DatafeedConfig> datafeedConfigs = new ArrayList<>();
                    for (DatafeedConfig.Builder builder : datafeedBuilders) {
                        datafeedConfigs.add(builder.build());
                    }

                    requiredMatches.filterMatchedIds(datafeedConfigs.stream().map(DatafeedConfig::getId).collect(Collectors.toList()));

                    if (requiredMatches.hasUnmatchedIds()) {
                        listener.onFailure(ExceptionsHelper.missingDatafeedException(requiredMatches.unmatchedIdsString()));
                    } else {
                        datafeedConfigs.addAll(clusterStateConfigs.values());
                        Collections.sort(datafeedConfigs, Comparator.comparing(DatafeedConfig::getId));
                        listener.onResponse(datafeedConfigs);
                    }
                },
                listener::onFailure
        ));
    }

    private Map<String, DatafeedConfig> expandClusterStateDatafeeds(String datafeedExpression, ClusterState clusterState) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(clusterState);
        Set<String> expandedDatafeedIds = mlMetadata.expandDatafeedIds(datafeedExpression);
        return expandedDatafeedIds.stream().collect(Collectors.toMap(Function.identity(), mlMetadata::getDatafeed));
    }
}
