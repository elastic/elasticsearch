/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class FeatureService {

    public static final Version MAX_HISTORICAL_VERSION_EXCLUSIVE = Version.V_8_12_0;

    private final NavigableMap<Version, Set<String>> historicalFeatures;
    private final Set<String> nodeFeatures;

    public FeatureService(List<FeatureSpecification> specs) {
        Map<String, FeatureSpecification> allFeatures = new HashMap<>();

        NavigableMap<Version, Set<String>> historicalFeatures = new TreeMap<>();
        Set<String> nodeFeatures = new HashSet<>();
        for (var spec : specs) {
            for (var hfe : spec.getHistoricalFeatures().entrySet()) {
                var existing = allFeatures.putIfAbsent(hfe.getKey().id(), spec);
                if (existing != null) {
                    throw new IllegalArgumentException(
                        Strings.format("Duplicate feature - [%s] is declared by both [%s] and [%s]", hfe.getKey().id(), existing, spec)
                    );
                }

                if (hfe.getValue().onOrAfter(MAX_HISTORICAL_VERSION_EXCLUSIVE)) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "Historical feature [%s] declared by [%s] for version [%s] is not a historical version",
                            hfe.getKey().id(),
                            spec,
                            hfe.getValue()
                        )
                    );
                }

                historicalFeatures.computeIfAbsent(hfe.getValue(), k -> new HashSet<>()).add(hfe.getKey().id());
            }

            for (NodeFeature f : spec.getFeatures()) {
                var existing = allFeatures.putIfAbsent(f.id(), spec);
                if (existing != null) {
                    throw new IllegalArgumentException(
                        Strings.format("Duplicate feature - [%s] is declared by both [%s] and [%s]", f.id(), existing, spec)
                    );
                }

                nodeFeatures.add(f.id());
            }
        }

        this.historicalFeatures = consolidateHistoricalFeatures(historicalFeatures);
        this.nodeFeatures = Set.copyOf(nodeFeatures);
    }

    private static NavigableMap<Version, Set<String>> consolidateHistoricalFeatures(
        NavigableMap<Version, Set<String>> declaredHistoricalFeatures
    ) {
        // update each version by adding in all features from previous versions
        Set<String> featureAggregator = new HashSet<>();
        for (Map.Entry<Version, Set<String>> versions : declaredHistoricalFeatures.entrySet()) {
            featureAggregator.addAll(versions.getValue());
            versions.setValue(Set.copyOf(featureAggregator));
        }

        return Collections.unmodifiableNavigableMap(declaredHistoricalFeatures);
    }

    /**
     * The non-historical features present on this node
     */
    public Set<String> getNodeFeatures() {
        return nodeFeatures;
    }

    /**
     * Returns {@code true} if all nodes in {@code state} have feature {@code feature}.
     */
    @SuppressForbidden(reason = "We need basic feature information from cluster state")
    public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
        if (state.clusterFeatures().clusterHasFeature(feature)) {
            return true;
        }

        var features = historicalFeatures.floorEntry(state.getNodes().getMinNodeVersion());
        return features != null && features.getValue().contains(feature.id());
    }
}
