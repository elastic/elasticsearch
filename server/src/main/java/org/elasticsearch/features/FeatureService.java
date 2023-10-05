/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.plugins.PluginsService;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Service responsible for registering features this node should publish to other nodes
 */
public class FeatureService {

    private static Set<FeatureSpecification> FEATURE_SPECS;

    private static volatile Set<String> NODE_FEATURES;
    private static volatile NavigableMap<Version, Set<String>> HISTORICAL_FEATURES;

    static {
        // load all the ones accessible on the immediate classpath here
        // in unit tests, this will pick everything up. In a real ES node, this will miss instances from plugins
        FEATURE_SPECS = ServiceLoader.load(FeatureSpecification.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .collect(Collectors.toUnmodifiableSet());

        if (Assertions.ENABLED) {
            checkSpecsForDuplicates();
        }
    }

    /**
     * Loads additional historical features from {@link FeatureSpecification} instances in plugins and modules
     */
    public static void loadAdditionalFeatures(PluginsService plugins) {
        NODE_FEATURES = null;
        HISTORICAL_FEATURES = null;

        var existingSpecs = new HashSet<>(FEATURE_SPECS);
        existingSpecs.addAll(plugins.loadServiceProviders(FeatureSpecification.class));
        FEATURE_SPECS = Collections.unmodifiableSet(existingSpecs);

        if (Assertions.ENABLED) {
            checkSpecsForDuplicates();
        }
    }

    private static void checkSpecsForDuplicates() {
        Map<String, FeatureSpecification> allFeatures = new HashMap<>();
        for (var spec : FEATURE_SPECS) {
            for (var f : spec.getFeatures()) {
                var existing = allFeatures.putIfAbsent(f.id(), spec);
                if (existing != null) {
                    throw new IllegalStateException(
                        Strings.format("Duplicate feature - %s is declared by both %s and %s", f.id(), existing, spec)
                    );
                }
            }
            for (var hf : spec.getHistoricalFeatures().keySet()) {
                var existing = allFeatures.putIfAbsent(hf.id(), spec);
                if (existing != null) {
                    throw new IllegalStateException(
                        Strings.format("Duplicate feature - %s is declared by both %s and %s", hf.id(), existing, spec)
                    );
                }
            }
        }
    }

    private static NavigableMap<Version, Set<String>> calculateHistoricalFeaturesFromSpecs() {
        NavigableMap<Version, Set<String>> declaredHistoricalFeatures = new TreeMap<>();
        for (var spec : FEATURE_SPECS) {
            for (var f : spec.getHistoricalFeatures().entrySet()) {
                if (f.getValue().after(Version.V_8_11_0)) {
                    throw new IllegalArgumentException(
                        Strings.format("Feature declared for version %s is not a historical feature", f.getValue())
                    );
                }
                if (f.getKey().era().era() != f.getValue().major) {
                    throw new IllegalArgumentException(
                        Strings.format("Incorrect feature era %s for version %s", f.getKey().era(), f.getValue().major)
                    );
                }

                declaredHistoricalFeatures.computeIfAbsent(f.getValue(), k -> new HashSet<>()).add(f.getKey().id());
            }
        }

        // add in all features from previous versions
        // make the sets sorted so they're easier to look through
        Set<String> featureAggregator = new TreeSet<>();
        for (Map.Entry<Version, Set<String>> versions : declaredHistoricalFeatures.entrySet()) {
            if (FeatureEra.isPublishable(versions.getKey().major)) continue;    // don't include, it's before the valid eras

            featureAggregator.addAll(versions.getValue());
            versions.setValue(Collections.unmodifiableNavigableSet(new TreeSet<>(featureAggregator)));
        }

        return Collections.unmodifiableNavigableMap(declaredHistoricalFeatures);
    }

    /**
     * Returns the features that are implied by a node with a specified {@code version}.
     */
    private static Set<String> readHistoricalFeatures(Version version) {
        var aggregates = HISTORICAL_FEATURES;
        if (aggregates == null) {
            aggregates = HISTORICAL_FEATURES = calculateHistoricalFeaturesFromSpecs();
        }
        var features = aggregates.floorEntry(version);
        return features != null ? features.getValue() : Set.of();
    }

    public static boolean versionHasHistoricalFeature(Version version, String feature) {
        return readHistoricalFeatures(version).contains(feature);
    }

    private static Set<String> calculateFeaturesFromSpecs() {
        Set<String> features = new TreeSet<>();
        for (var spec : FEATURE_SPECS) {
            for (var f : spec.getFeatures()) {
                if (f.era().isPublishable()) {
                    features.add(f.id());
                }
            }
        }
        return Collections.unmodifiableSet(features);
    }

    /**
     * Returns all the features registered by all loaded {@link FeatureSpecification} instances
     */
    public static Set<String> readFeatures() {
        var features = NODE_FEATURES;
        if (features == null) {
            features = NODE_FEATURES = calculateFeaturesFromSpecs();
        }
        return features;
    }

    private final Set<String> features;

    /**
     * Constructs a new {@code FeatureService} for the local node
     */
    public FeatureService() {
        features = readFeatures();
    }

    public FeatureService(Set<String> features) {
        this.features = Set.copyOf(features);
    }

    /**
     * Returns the set of features published by this node.
     * This prevents any further modifications to the feature set.
     */
    public Set<String> readPublishableFeatures() {
        return features;
    }
}
