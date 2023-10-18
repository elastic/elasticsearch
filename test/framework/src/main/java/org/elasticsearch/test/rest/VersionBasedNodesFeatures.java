/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO[lor]: this needs to be replaced with (historical) feature checks once we have figured out a way of exposing them
// to rest tests (e.g. through an internal API or similar)
class VersionBasedNodesFeatures {
    static class VersionBasedNodesFeaturesBuilder {
        private final List<ESRestTestNodesFeatures> nodesFeaturesList = new ArrayList<>();

        void addInfoFromNode(Map<?, ?> nodeInfo) {
            Version version = Version.fromString(nodeInfo.get("version").toString().replace("-SNAPSHOT", ""));
            nodesFeaturesList.add(new ESRestTestNodesFeatures() {
                @Override
                public boolean supportsSearchableSnapshotsIndices() {
                    return version.onOrAfter(Version.V_7_8_0);
                }

                @Override
                public boolean supportsComposableIndexTemplates() {
                    return version.onOrAfter(Version.V_7_7_0);
                }

                @Override
                public boolean supportsBulkDeleteOnTemplates() {
                    return version.onOrAfter(Version.V_7_13_0);
                }

                @Override
                public boolean deprecatesSoftDeleteDisabled() {
                    return version.onOrAfter(Version.V_7_6_0);
                }

                @Override
                public boolean enforcesSoftDeleteEnabled() {
                    return version.onOrAfter(Version.V_8_0_0);
                }

                @Override
                public boolean deprecatesSystemIndicesAccess() {
                    return version.onOrAfter(Version.V_8_0_0);
                }

                @Override
                public boolean supportsReplicationOfClosedIndices() {
                    return version.onOrAfter(Version.V_7_2_0);
                }

                @Override
                public boolean supportsFeatureStateReset() {
                    return version.onOrAfter(Version.V_7_13_0);
                }

                @Override
                public boolean enforcesMlResetEnabled() {
                    return version.onOrAfter(Version.V_8_7_0);
                }

                @Override
                public boolean supportsNodeShutdownApi() {
                    return version.onOrAfter(Version.V_7_15_0);
                }

                @Override
                public boolean supportsOperationsOnHiddenIndices() {
                    return version.onOrAfter(Version.V_7_7_0);
                }

                @Override
                public boolean enforcesPeerRecoveryRetentionLeases() {
                    return version.onOrAfter(Version.V_7_6_0);
                }

                @Override
                public boolean enforcesAllocationFilteringRulesOnIndicesAutoExpand() {
                    return version.onOrAfter(Version.V_7_6_0);
                }
            });
        }

        ESRestTestNodesFeatures build() {
            boolean supportsSearchableSnapshotsIndices = nodesFeaturesList.stream()
                .allMatch(ESRestTestNodesFeatures::supportsSearchableSnapshotsIndices);
            boolean supportsComposableIndexTemplates = nodesFeaturesList.stream()
                .allMatch(ESRestTestNodesFeatures::supportsComposableIndexTemplates);
            boolean supportsBulkDeleteOnTemplates = nodesFeaturesList.stream()
                .allMatch(ESRestTestNodesFeatures::supportsBulkDeleteOnTemplates);
            boolean isSoftDeleteDisabledDeprecated = nodesFeaturesList.stream()
                .allMatch(ESRestTestNodesFeatures::deprecatesSoftDeleteDisabled);
            boolean isSoftDeleteDefault = nodesFeaturesList.stream().allMatch(ESRestTestNodesFeatures::enforcesSoftDeleteEnabled);
            boolean areSystemIndexesEnforced = nodesFeaturesList.stream().allMatch(ESRestTestNodesFeatures::deprecatesSystemIndicesAccess);
            boolean supportsReplicationOfClosedIndices = nodesFeaturesList.stream()
                .allMatch(ESRestTestNodesFeatures::supportsReplicationOfClosedIndices);
            boolean supportsFeatureStateReset = nodesFeaturesList.stream().allMatch(ESRestTestNodesFeatures::supportsFeatureStateReset);
            boolean isMlResetAlwaysEnabled = nodesFeaturesList.stream().allMatch(ESRestTestNodesFeatures::enforcesMlResetEnabled);
            boolean supportsNodeShutdownApi = nodesFeaturesList.stream().allMatch(ESRestTestNodesFeatures::supportsNodeShutdownApi);
            boolean supportsOperationsOnHiddenIndices = nodesFeaturesList.stream()
                .allMatch(ESRestTestNodesFeatures::supportsOperationsOnHiddenIndices);
            boolean enforcesPeerRecoveryRetentionLeases = nodesFeaturesList.stream()
                .allMatch(ESRestTestNodesFeatures::enforcesPeerRecoveryRetentionLeases);
            boolean enforcesAllocationFilteringRulesOnIndicesAutoExpand = nodesFeaturesList.stream()
                .allMatch(ESRestTestNodesFeatures::enforcesAllocationFilteringRulesOnIndicesAutoExpand);

            return new ESRestTestNodesFeatures() {
                @Override
                public boolean supportsSearchableSnapshotsIndices() {
                    return supportsSearchableSnapshotsIndices;
                }

                @Override
                public boolean supportsComposableIndexTemplates() {
                    return supportsComposableIndexTemplates;
                }

                @Override
                public boolean supportsBulkDeleteOnTemplates() {
                    return supportsBulkDeleteOnTemplates;
                }

                @Override
                public boolean deprecatesSoftDeleteDisabled() {
                    return isSoftDeleteDisabledDeprecated;
                }

                @Override
                public boolean enforcesSoftDeleteEnabled() {
                    return isSoftDeleteDefault;
                }

                @Override
                public boolean deprecatesSystemIndicesAccess() {
                    return areSystemIndexesEnforced;
                }

                @Override
                public boolean supportsReplicationOfClosedIndices() {
                    return supportsReplicationOfClosedIndices;
                }

                @Override
                public boolean supportsFeatureStateReset() {
                    return supportsFeatureStateReset;
                }

                @Override
                public boolean enforcesMlResetEnabled() {
                    return isMlResetAlwaysEnabled;
                }

                @Override
                public boolean supportsNodeShutdownApi() {
                    return supportsNodeShutdownApi;
                }

                @Override
                public boolean supportsOperationsOnHiddenIndices() {
                    return supportsOperationsOnHiddenIndices;
                }

                @Override
                public boolean enforcesPeerRecoveryRetentionLeases() {
                    return enforcesPeerRecoveryRetentionLeases;
                }

                @Override
                public boolean enforcesAllocationFilteringRulesOnIndicesAutoExpand() {
                    return enforcesAllocationFilteringRulesOnIndicesAutoExpand;
                }
            };
        }
    }

    public static VersionBasedNodesFeaturesBuilder builder() {
        return new VersionBasedNodesFeaturesBuilder();
    }
}
