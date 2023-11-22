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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * Manages information on the features supported by nodes in the cluster
 */
public class FeatureService {

    /**
     * A feature indicating that node features are supported.
     */
    public static final NodeFeature FEATURES_SUPPORTED = new NodeFeature("features_supported");

    private static final Logger logger = LogManager.getLogger(FeatureService.class);

    public static final Version CLUSTER_FEATURES_ADDED_VERSION = Version.V_8_12_0;

    private final NavigableMap<Version, Set<String>> historicalFeatures;
    private final Map<String, NodeFeature> nodeFeatures;

    public FeatureService(List<? extends FeatureSpecification> specs) {
        Map<String, FeatureSpecification> allFeatures = new HashMap<>();

        NavigableMap<Version, Set<String>> historicalFeatures = new TreeMap<>();
        Map<String, NodeFeature> nodeFeatures = new HashMap<>();
        for (FeatureSpecification spec : specs) {
            for (var hfe : spec.getHistoricalFeatures().entrySet()) {
                FeatureSpecification existing = allFeatures.putIfAbsent(hfe.getKey().id(), spec);
                // the same SPI class can be loaded multiple times if it's in the base classloader
                if (existing != null && existing.getClass() != spec.getClass()) {
                    throw new IllegalArgumentException(
                        Strings.format("Duplicate feature - [%s] is declared by both [%s] and [%s]", hfe.getKey().id(), existing, spec)
                    );
                }

                if (hfe.getValue().after(CLUSTER_FEATURES_ADDED_VERSION)) {
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
                FeatureSpecification existing = allFeatures.putIfAbsent(f.id(), spec);
                if (existing != null && existing.getClass() != spec.getClass()) {
                    throw new IllegalArgumentException(
                        Strings.format("Duplicate feature - [%s] is declared by both [%s] and [%s]", f.id(), existing, spec)
                    );
                }

                nodeFeatures.put(f.id(), f);
            }
        }

        this.historicalFeatures = consolidateHistoricalFeatures(historicalFeatures);
        this.nodeFeatures = Map.copyOf(nodeFeatures);

        logger.info("Registered local node features {}", nodeFeatures.keySet().stream().sorted().toList());
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
     * The non-historical features supported by this node.
     */
    public Map<String, NodeFeature> getNodeFeatures() {
        return nodeFeatures;
    }

    /**
     * Returns {@code true} if all nodes in {@code state} support feature {@code feature}.
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
