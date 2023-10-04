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
import org.elasticsearch.plugins.PluginsService;

import java.util.Collections;
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

    private static volatile NavigableMap<Version, Set<String>> HISTORICAL_FEATURES;

    static {
        // load all the ones accessible on the immediate classpath here
        // in unit tests, this will pick everything up. In a real ES node, this will miss instances from plugins
        FEATURE_SPECS = ServiceLoader.load(FeatureSpecification.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Loads additional historical features from {@link FeatureSpecification} instances in plugins and modules
     */
    public static void loadAdditionalHistoricalFeatures(PluginsService plugins) {
        HISTORICAL_FEATURES = null;

        var existingSpecs = new HashSet<>(FEATURE_SPECS);
        existingSpecs.addAll(plugins.loadServiceProviders(FeatureSpecification.class));
        FEATURE_SPECS = Collections.unmodifiableSet(existingSpecs);
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
        Set<String> featureAggregator = new HashSet<>();
        for (Map.Entry<Version, Set<String>> versions : declaredHistoricalFeatures.entrySet()) {
            if (FeatureEra.isPublishable(versions.getKey().major)) continue;    // don't include, it's before the valid eras

            // check the sizes to ensure we haven't got dups
            int sizeBefore = featureAggregator.size();
            featureAggregator.addAll(versions.getValue());
            if (featureAggregator.size() < sizeBefore + versions.getValue().size()) {
                throw new IllegalStateException("Duplicated feature ids in historical features at version" + versions.getKey());
            }

            // make them sorted so they're easier to look through
            versions.setValue(Collections.unmodifiableNavigableSet(new TreeSet<>(featureAggregator)));
        }

        return Collections.unmodifiableNavigableMap(declaredHistoricalFeatures);
    }

    /**
     * Returns the features that are implied by a node with a specified {@code version}.
     */
    public static Set<String> readHistoricalFeatures(Version version) {
        var aggregates = HISTORICAL_FEATURES;
        if (aggregates == null) {
            aggregates = HISTORICAL_FEATURES = calculateHistoricalFeaturesFromSpecs();
        }
        var features = aggregates.floorEntry(version);
        return features != null ? features.getValue() : Set.of();
    }

    private final Set<String> features = new TreeSet<>();
    private volatile boolean locked;

    /**
     * Constructs a new {@code FeatureService} for the local node
     */
    public FeatureService() {
        // load all the pre-declared features
        for (var spec : FEATURE_SPECS) {
            try {
                spec.getFeatures().forEach(this::registerFeature);
            } catch (IllegalArgumentException e) {
                // try to capture where the feature is from to help debugging
                throw new IllegalArgumentException("Error registering features from " + spec, e);
            }
        }
    }

    /**
     * Register a new feature. This should only be called during node initialization.
     * Once {@link #readPublishableFeatures} is called, no more features can be registered.
     */
    public void registerFeature(NodeFeature feature) {
        // we don't need proper sync here, this is just a sanity check
        if (locked) throw new IllegalStateException("The node's feature set has already been read");

        if (feature.era().isPublishable() && features.add(feature.id()) == false) {
            throw new IllegalArgumentException("Feature " + feature.id() + " is already registered");
        }
    }

    /**
     * Returns the set of features published by this node.
     * This prevents any further modifications to the feature set.
     */
    public Set<String> readPublishableFeatures() {
        locked = true;
        return Collections.unmodifiableSet(features);
    }
}
