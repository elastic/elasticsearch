/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO[lor]: this needs to be replaced with (historical) feature checks once we have figured out a way of exposing them
// to rest tests (e.g. through an internal API or similar)
class VersionBasedNodesFeatures {
    static class VersionBasedNodesFeaturesBuilder {
        private final List<List<Boolean>> nodesFeaturesBitmap = new ArrayList<>();

        // TODO: replace with FeatureService historical features when they will be available
        private final List<Tuple<Version, String>> historicalFeatures = List.of(
            Tuple.tuple(Version.V_7_8_0, "searchable_snapshots_indices"),
            Tuple.tuple(Version.V_7_7_0, "composable_index_templates"),
            Tuple.tuple(Version.V_7_13_0, "bulk_template_operations"),
            Tuple.tuple(Version.V_7_6_0, "soft_delete_disabled_deprecated"),
            Tuple.tuple(Version.V_8_0_0, "soft_delete_enforced"),
            Tuple.tuple(Version.V_8_0_0, "system_indices_access_deprecated"),
            Tuple.tuple(Version.V_7_2_0, "replication_closed_indices"),
            Tuple.tuple(Version.V_7_13_0, "ml.feature_state_reset"),
            Tuple.tuple(Version.V_8_7_0, "ml.reset_enforced"),
            Tuple.tuple(Version.V_7_15_0, "node_shutdown_api"),
            Tuple.tuple(Version.V_7_7_0, "hidden_indices_operations"),
            Tuple.tuple(Version.V_7_6_0, "peer_recovery_retention_leases_enforced"),
            Tuple.tuple(Version.V_7_6_0, "indices_auto_expand_allocation_filtering_rules_enforced")
        );

        void addInfoFromNode(Map<?, ?> nodeInfo) {
            Version version = Version.fromString(nodeInfo.get("version").toString());
            var nodeFeatures = historicalFeatures.stream().map(t -> version.onOrAfter(t.v1())).toList();
            nodesFeaturesBitmap.add(nodeFeatures);
        }

        ESRestTestNodesFeatures build() {
            var commonFeatures = new HashMap<String, Boolean>();
            for (int i = 0; i < historicalFeatures.size(); ++i) {
                final int featureIndex = i;
                commonFeatures.put(
                    historicalFeatures.get(i).v2(),
                    nodesFeaturesBitmap.stream().allMatch(nodeFeatures -> nodeFeatures.get(featureIndex))
                );
            }

            return featureName -> {
                var hasFeature = commonFeatures.get(featureName);
                if (hasFeature == null) {
                    throw new IllegalArgumentException(featureName + " is not a feature available to ESRestTests");
                }
                return hasFeature;
            };
        }
    }

    public static VersionBasedNodesFeaturesBuilder builder() {
        return new VersionBasedNodesFeaturesBuilder();
    }
}
