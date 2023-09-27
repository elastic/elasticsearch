/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.Version;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service responsible for registering features this node should publish that it has.
 */
public class FeatureService {

    private static final Map<Version, Set<String>> HISTORICAL_FEATURES = Map.ofEntries();

    private static final NavigableMap<Version, Set<String>> AGGREGATED_FEATURES;

    static {
        NavigableMap<Version, Set<String>> consolidated = new TreeMap<>(HISTORICAL_FEATURES);
        int thisEra = Version.CURRENT.major;

        // add in all features from previous versions
        Set<String> aggregatedFeatures = new HashSet<>();
        for (Map.Entry<Version, Set<String>> versions : consolidated.entrySet()) {
            if (versions.getKey().major < thisEra - 1) continue;    // don't include, its past the valid eras

            // check the sizes to ensure we haven't got dups
            int sizeBefore = aggregatedFeatures.size();
            aggregatedFeatures.addAll(versions.getValue());
            if (aggregatedFeatures.size() < sizeBefore + versions.getValue().size()) {
                throw new IllegalStateException("Duplicated feature ids in historical versions list at version" + versions.getKey());
            }

            // make them sorted so they're easier to diagnose
            versions.setValue(Collections.unmodifiableNavigableSet(new TreeSet<>(aggregatedFeatures)));
        }

        AGGREGATED_FEATURES = Collections.unmodifiableNavigableMap(consolidated);
    }

    public static Set<String> getHistoricalFeatures(Version version) {
        var features = AGGREGATED_FEATURES.floorEntry(version);
        return features != null ? features.getValue() : Set.of();
    }

    private final Map<String, String> features = new ConcurrentHashMap<>();
    private volatile Set<String> finalFeatureSet;


    private record Feature(String id, int era) implements NodeFeature {}

    private static boolean isRelevantEra(int era) {
        return era >= Version.CURRENT.major - 1;
    }

    public NodeFeature registerFeature(String id, int era) {
        // we don't need proper sync here, this is just a sanity check
        if (finalFeatureSet != null) throw new IllegalStateException("The node's feature set has already been read");

        if (isRelevantEra(era) && features.putIfAbsent(id, id) != null) {
            throw new IllegalArgumentException("Feature " + id + " is already registered");
        }

        return new Feature(id, era);
    }

    public Set<String> readPublishedFeatures() {
        Set<String> finalFeatures = finalFeatureSet;
        if (finalFeatures == null) {
            finalFeatures = finalFeatureSet = Set.copyOf(features.values());
        }
        return finalFeatures;
    }
}
