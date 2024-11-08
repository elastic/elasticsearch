/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
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

import static org.elasticsearch.features.FeatureService.CLUSTER_FEATURES_ADDED_VERSION;

/**
 * Reads and consolidate features exposed by a list {@link FeatureSpecification}, grouping them into historical features and node
 * features for the consumption of {@link FeatureService}
 */
public class FeatureData {

    private static final Logger Log = LogManager.getLogger(FeatureData.class);
    private static final boolean INCLUDE_TEST_FEATURES = System.getProperty("tests.testfeatures.enabled", "").equals("true");

    static {
        if (INCLUDE_TEST_FEATURES) {
            Log.warn("WARNING: Test features are enabled. This should ONLY be used in automated tests.");
        }
    }

    private final NavigableMap<Version, Set<String>> historicalFeatures;
    private final Map<String, NodeFeature> nodeFeatures;

    private FeatureData(NavigableMap<Version, Set<String>> historicalFeatures, Map<String, NodeFeature> nodeFeatures) {
        this.historicalFeatures = historicalFeatures;
        this.nodeFeatures = nodeFeatures;
    }

    public static FeatureData createFromSpecifications(List<? extends FeatureSpecification> specs) {
        Map<String, FeatureSpecification> allFeatures = new HashMap<>();

        // Initialize historicalFeatures with empty version to guarantee there's a floor entry for every version
        NavigableMap<Version, Set<String>> historicalFeatures = new TreeMap<>(Map.of(Version.V_EMPTY, Set.of()));
        Map<String, NodeFeature> nodeFeatures = new HashMap<>();
        for (FeatureSpecification spec : specs) {
            Set<NodeFeature> specFeatures = spec.getFeatures();
            if (INCLUDE_TEST_FEATURES) {
                specFeatures = new HashSet<>(specFeatures);
                specFeatures.addAll(spec.getTestFeatures());
            }

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

                if (specFeatures.contains(hfe.getKey())) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "Feature [%s] cannot be declared as both a regular and historical feature by [%s]",
                            hfe.getKey().id(),
                            spec
                        )
                    );
                }

                historicalFeatures.computeIfAbsent(hfe.getValue(), k -> new HashSet<>()).add(hfe.getKey().id());
            }

            for (NodeFeature f : specFeatures) {
                FeatureSpecification existing = allFeatures.putIfAbsent(f.id(), spec);
                if (existing != null && existing.getClass() != spec.getClass()) {
                    throw new IllegalArgumentException(
                        Strings.format("Duplicate feature - [%s] is declared by both [%s] and [%s]", f.id(), existing, spec)
                    );
                }

                nodeFeatures.put(f.id(), f);
            }
        }

        return new FeatureData(consolidateHistoricalFeatures(historicalFeatures), Map.copyOf(nodeFeatures));
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

    public NavigableMap<Version, Set<String>> getHistoricalFeatures() {
        return historicalFeatures;
    }

    public Map<String, NodeFeature> getNodeFeatures() {
        return nodeFeatures;
    }
}
