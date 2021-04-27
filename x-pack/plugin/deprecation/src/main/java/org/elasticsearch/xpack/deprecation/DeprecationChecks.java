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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class containing all the cluster, node, and index deprecation checks that will be served
 * by the {@link DeprecationInfoAction}.
 */
public class DeprecationChecks {

    private DeprecationChecks() {
    }

    static List<Function<ClusterState, DeprecationIssue>> CLUSTER_SETTINGS_CHECKS =
        Collections.unmodifiableList(Arrays.asList(
            ClusterDeprecationChecks::checkUserAgentPipelines,
            ClusterDeprecationChecks::checkTemplatesWithTooManyFields,
            ClusterDeprecationChecks::checkPollIntervalTooLow,
            ClusterDeprecationChecks::checkTemplatesWithFieldNamesDisabled
        ));


    static final List<TriFunction<Settings, PluginsAndModules, XPackLicenseState, DeprecationIssue>> NODE_SETTINGS_CHECKS;

        static {
            final Stream<TriFunction<Settings, PluginsAndModules, XPackLicenseState, DeprecationIssue>> legacyRoleSettings =
                DiscoveryNode.getPossibleRoles()
                .stream()
                .filter(r -> r.legacySetting() != null)
                .map(r -> (s, p, t) -> NodeDeprecationChecks.checkLegacyRoleSettings(r.legacySetting(), s, p));
            NODE_SETTINGS_CHECKS = Stream.concat(
                legacyRoleSettings,
                Stream.of(
                    NodeDeprecationChecks::javaVersionCheck,
                    NodeDeprecationChecks::checkPidfile,
                    NodeDeprecationChecks::checkProcessors,
                    NodeDeprecationChecks::checkMissingRealmOrders,
                    NodeDeprecationChecks::checkUniqueRealmOrders,
                    NodeDeprecationChecks::checkImplicitlyDisabledBasicRealms,
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkThreadPoolListenerQueueSize(settings),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkThreadPoolListenerSize(settings),
                    NodeDeprecationChecks::checkClusterRemoteConnectSetting,
                    NodeDeprecationChecks::checkNodeLocalStorageSetting,
                    NodeDeprecationChecks::checkGeneralScriptSizeSetting,
                    NodeDeprecationChecks::checkGeneralScriptExpireSetting,
                    NodeDeprecationChecks::checkGeneralScriptCompileSettings,
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.ENRICH_ENABLED_SETTING),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.FLATTENED_ENABLED),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.INDEX_LIFECYCLE_ENABLED),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.MONITORING_ENABLED),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.ROLLUP_ENABLED),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.SNAPSHOT_LIFECYCLE_ENABLED),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.SQL_ENABLED),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.TRANSFORM_ENABLED),
                    (settings, pluginsAndModules, state) -> NodeDeprecationChecks.checkNodeBasicLicenseFeatureEnabledSetting(settings,
                        XPackSettings.VECTORS_ENABLED),
                    NodeDeprecationChecks::checkMultipleDataPaths,
                    NodeDeprecationChecks::checkDataPathsList,
                    NodeDeprecationChecks::checkImplicitlyDisabledSecurityOnBasicAndTrial
                )
            ).collect(Collectors.toList());
        }

    static List<Function<IndexMetadata, DeprecationIssue>> INDEX_SETTINGS_CHECKS =
        Collections.unmodifiableList(Arrays.asList(
            IndexDeprecationChecks::oldIndicesCheck,
            IndexDeprecationChecks::tooManyFieldsCheck,
            IndexDeprecationChecks::chainedMultiFieldsCheck,
            IndexDeprecationChecks::deprecatedDateTimeFormat,
            IndexDeprecationChecks::translogRetentionSettingCheck,
            IndexDeprecationChecks::fieldNamesDisabledCheck
        ));


    /**
     * helper utility function to reduce repeat of running a specific {@link List} of checks.
     *
     * @param checks The functional checks to execute using the mapper function
     * @param mapper The function that executes the lambda check with the appropriate arguments
     * @param <T> The signature of the check (TriFunction, BiFunction, Function, including the appropriate arguments)
     * @return The list of {@link DeprecationIssue} that were found in the cluster
     */
    static <T> List<DeprecationIssue> filterChecks(List<T> checks, Function<T, DeprecationIssue> mapper) {
        return checks.stream().map(mapper).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @FunctionalInterface
    public interface TriFunction<F, S, T, R> {
        R apply(F first, S second, T third);
    }
}
