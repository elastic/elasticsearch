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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Service responsible for registering features this node should publish to other nodes
 */
public class FeatureService {

    private static Map<Class<? extends FeatureSpecification>, FeatureSpecification> FEATURE_SPECS;

    private static volatile Set<String> NODE_FEATURES;
    private static volatile NavigableMap<Version, Set<String>> HISTORICAL_FEATURES;

    private static void readSpecs() {
        if (FEATURE_SPECS == null) {
            // load all the ones accessible on the immediate classpath here
            // in unit tests, this will pick everything up. In a real ES node, this will miss instances from plugins
            FEATURE_SPECS = ServiceLoader.load(FeatureSpecification.class)
                .stream()
                .collect(Collectors.toUnmodifiableMap(ServiceLoader.Provider::type, ServiceLoader.Provider::get));

            if (Assertions.ENABLED) {
                checkFeatureSpecs(FEATURE_SPECS.values());
            }
        }
    }

    public static void resetSpecs() {
        FEATURE_SPECS = null;
        NODE_FEATURES = null;
        HISTORICAL_FEATURES = null;
    }

    /**
     * Registers additional {@link FeatureSpecification} implementations from {@code plugins}
     */
    public static void registerSpecificationsFrom(PluginsService plugins) {
        registerSpecificationsFrom(plugins.loadServiceProviders(FeatureSpecification.class));
    }

    /**
     * Registers additional {@link FeatureSpecification} implementations
     */
    public static void registerSpecificationsFrom(Collection<? extends FeatureSpecification> specs) {
        NODE_FEATURES = null;
        HISTORICAL_FEATURES = null;
        readSpecs();

        // the class key is to ensure we only load one copy of each implementation
        // if we're registering the same things multiple times for tests etc
        var existingSpecs = new HashMap<>(FEATURE_SPECS);
        for (FeatureSpecification spec : specs) {
            existingSpecs.put(spec.getClass(), spec);
        }
        if (Assertions.ENABLED) {
            checkFeatureSpecs(existingSpecs.values());
        }
        FEATURE_SPECS = Collections.unmodifiableMap(existingSpecs);
    }

    private static void checkFeatureSpecs(Collection<FeatureSpecification> specs) {
        Map<String, FeatureSpecification> allFeatures = new HashMap<>();
        for (FeatureSpecification spec : specs) {
            for (NodeFeature f : spec.getFeatures()) {
                var existing = allFeatures.putIfAbsent(f.id(), spec);
                if (existing != null) {
                    throw new IllegalArgumentException(
                        Strings.format("Duplicate feature - [%s] is declared by both [%s] and [%s]", f.id(), existing, spec)
                    );
                }
            }

            for (var hfe : spec.getHistoricalFeatures().entrySet()) {
                var existing = allFeatures.putIfAbsent(hfe.getKey().id(), spec);
                if (existing != null) {
                    throw new IllegalArgumentException(
                        Strings.format("Duplicate feature - [%s] is declared by both [%s] and [%s]", hfe.getKey().id(), existing, spec)
                    );
                }

                if (hfe.getValue().onOrAfter(Version.V_8_12_0)) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "Historical feature [%s] declared by [%s] for version [%s] is not a historical version",
                            hfe.getKey().id(),
                            spec,
                            hfe.getValue()
                        )
                    );
                }
                if (hfe.getKey().era().era() != hfe.getValue().major) {
                    throw new IllegalArgumentException(
                        Strings.format(
                            "Historical feature [%s] declared by [%s] has incorrect feature era [%s] for version [%s]",
                            hfe.getKey().id(),
                            spec,
                            hfe.getKey().era(),
                            hfe.getValue(),
                            spec
                        )
                    );
                }
            }
        }
    }

    private static NavigableMap<Version, Set<String>> calculateHistoricalFeaturesFromSpecs() {
        readSpecs();

        // use sorted sets here so they're easier to look through in info & debug output
        NavigableMap<Version, NavigableSet<String>> declaredHistoricalFeatures = new TreeMap<>();
        for (FeatureSpecification spec : FEATURE_SPECS.values()) {
            for (var f : spec.getHistoricalFeatures().entrySet()) {
                if (FeatureEra.isPublishable(f.getValue().major) == false) continue;    // don't include, it's before the valid eras
                declaredHistoricalFeatures.computeIfAbsent(f.getValue(), k -> new TreeSet<>()).add(f.getKey().id());
            }
        }

        // add in all features from previous versions
        Set<String> featureAggregator = new TreeSet<>();
        for (Map.Entry<Version, NavigableSet<String>> versions : declaredHistoricalFeatures.entrySet()) {
            featureAggregator.addAll(versions.getValue());
            versions.setValue(Collections.unmodifiableNavigableSet(new TreeSet<>(featureAggregator)));
        }

        return Collections.unmodifiableNavigableMap(declaredHistoricalFeatures);
    }

    /**
     * Returns the features that are implied by a node of {@code version}.
     */
    static Set<String> readHistoricalFeatures(Version version) {
        var aggregates = HISTORICAL_FEATURES;
        if (aggregates == null) {
            aggregates = HISTORICAL_FEATURES = calculateHistoricalFeaturesFromSpecs();
        }
        var features = aggregates.floorEntry(version);
        return features != null ? features.getValue() : Set.of();
    }

    private static Set<String> calculateFeaturesFromSpecs() {
        readSpecs();

        Set<String> features = new TreeSet<>();
        for (FeatureSpecification spec : FEATURE_SPECS.values()) {
            for (NodeFeature f : spec.getFeatures()) {
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
     * Construct a new {@code FeatureService} for the local node, using all available feature specifications.
     */
    public FeatureService() {
        features = readFeatures();
    }

    /**
     * Construct a new {@code FeatureService} to expose {@code features}
     */
    public FeatureService(Set<String> features) {
        this.features = Set.copyOf(features);
    }

    /**
     * Returns the set of features published by this node
     */
    public Set<String> readPublishableFeatures() {
        return features;
    }

    /**
     * {@code true} if the node with version {@code nodeVersion} and published features {@code features} has feature {@code feature}
     */
    public static boolean nodeHasFeature(Version nodeVersion, Set<String> features, NodeFeature feature) {
        if (feature.era().isPublishable() == false) {
            // this feature can never appear in our lists - its true by default
            return true;
        } else {
            return features.contains(feature.id()) || readHistoricalFeatures(nodeVersion).contains(feature.id());
        }
    }
}
