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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service responsible for registering features this node should publish to other nodes
 */
public class FeatureService {

    private static final int CURRENT_ERA = Version.CURRENT.major;

    /**
     * The set of features that are implied by historical node versions.
     */
    private static final Map<Version, Set<String>> HISTORICAL_FEATURES = new ConcurrentHashMap<>();

    /**
     * Adds a feature that should be inferred when a node is at or above the specified historical version.
     */
    public static void registerHistoricalFeature(NodeFeature feature, Version version) {
        if (version.after(Version.V_8_11_0)) {
            throw new IllegalArgumentException("This method should only be used for historical features");
        }
        if (feature.era() != version.major) {
            throw new IllegalArgumentException(Strings.format("Incorrect feature era %s for version %s", feature.era(), version.major));
        }
        if (AGGREGATED_FEATURES != null) {  // best-effort check
            throw new IllegalStateException("Aggregated historical features have already been calculated");
        }

        HISTORICAL_FEATURES.computeIfAbsent(version, k -> Collections.synchronizedSet(new HashSet<>())).add(feature.id());
    }

    private static volatile NavigableMap<Version, Set<String>> AGGREGATED_FEATURES;

    private static NavigableMap<Version, Set<String>> calculateAggregatedFeatures(Map<Version, Set<String>> versionFeatures) {
        NavigableMap<Version, Set<String>> consolidated = new TreeMap<>(versionFeatures);

        // add in all features from previous versions
        Set<String> aggregatedFeatures = new HashSet<>();
        for (Map.Entry<Version, Set<String>> versions : consolidated.entrySet()) {
            if (versions.getKey().major < CURRENT_ERA - 1) continue;    // don't include, it's before the valid eras

            // check the sizes to ensure we haven't got dups
            int sizeBefore = aggregatedFeatures.size();
            aggregatedFeatures.addAll(versions.getValue());
            if (aggregatedFeatures.size() < sizeBefore + versions.getValue().size()) {
                throw new IllegalStateException("Duplicated feature ids in historical versions list at version" + versions.getKey());
            }

            // make them sorted so they're easier to look through
            versions.setValue(Collections.unmodifiableNavigableSet(new TreeSet<>(aggregatedFeatures)));
        }

        return Collections.unmodifiableNavigableMap(consolidated);
    }

    /**
     * Returns the features that are implied by a node with a specified {@code version}.
     */
    public static Set<String> readHistoricalFeatures(Version version) {
        var aggregates = AGGREGATED_FEATURES;
        if (aggregates == null) {
            aggregates = AGGREGATED_FEATURES = calculateAggregatedFeatures(HISTORICAL_FEATURES);
        }
        var features = aggregates.floorEntry(version);
        return features != null ? features.getValue() : Set.of();
    }

    private final Set<String> features = new HashSet<>();
    private volatile boolean locked;

    private static boolean isPublishableEra(int era) {
        return era >= CURRENT_ERA - 1;
    }

    /**
     * Register a new feature. This should only be called during node initialization.
     * Once {@link #readPublishableFeatures} is called, no more features can be registered.
     */
    public void registerFeature(NodeFeature feature) {
        // we don't need proper sync here, this is just a sanity check
        if (locked) throw new IllegalStateException("The node's feature set has already been read");

        if (isPublishableEra(feature.era()) && features.add(feature.id()) == false) {
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
