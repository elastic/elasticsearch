/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Class containing all the cluster, node, and index deprecation checks that will be served
 * by the {@link DeprecationInfoAction}.
 */
public class DeprecationChecks {

    private DeprecationChecks() {
    }

    static List<Function<ClusterState, DeprecationIssue>> CLUSTER_SETTINGS_CHECKS =
        Collections.emptyList();

    static List<BiFunction<Settings, PluginsAndModules, DeprecationIssue>> NODE_SETTINGS_CHECKS = List.of(
        NodeDeprecationChecks::checkSharedDataPathSetting,
        NodeDeprecationChecks::checkReservedPrefixedRealmNames,
        NodeDeprecationChecks::checkSingleDataNodeWatermarkSetting
    );

    static List<Function<IndexMetadata, DeprecationIssue>> INDEX_SETTINGS_CHECKS = List.of(
        IndexDeprecationChecks::oldIndicesCheck,
        IndexDeprecationChecks::translogRetentionSettingCheck,
        IndexDeprecationChecks::checkIndexDataPath,
        IndexDeprecationChecks::storeTypeSettingCheck
    );

    /**
     * helper utility function to reduce repeat of running a specific {@link List} of checks.
     *
     * @param checks The functional checks to execute using the mapper function
     * @param mapper The function that executes the lambda check with the appropriate arguments
     * @param <T> The signature of the check (BiFunction, Function, including the appropriate arguments)
     * @return The list of {@link DeprecationIssue} that were found in the cluster
     */
    static <T> List<DeprecationIssue> filterChecks(List<T> checks, Function<T, DeprecationIssue> mapper) {
        return checks.stream().map(mapper).filter(Objects::nonNull).collect(Collectors.toList());
    }

}
