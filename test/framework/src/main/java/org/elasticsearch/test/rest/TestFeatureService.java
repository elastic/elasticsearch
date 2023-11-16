/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureData;
import org.elasticsearch.features.FeatureSpecification;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Predicate;

class TestFeatureService {
    private final Predicate<String> historicalFeaturesPredicate;
    private final Set<String> clusterStateFeatures;

    TestFeatureService(List<? extends FeatureSpecification> specs, Collection<Version> nodeVersions, Set<String> clusterStateFeatures) {

        var minNodeVersion = nodeVersions.stream().min(Version::compareTo);
        this.historicalFeaturesPredicate = minNodeVersion.map(v -> {
            var featureData = FeatureData.createFromSpecifications(specs);
            var historicalFeatures = featureData.getHistoricalFeatures();
            return (Predicate<String>) featureId -> hasHistoricalFeature(historicalFeatures, v, featureId);
        }).orElse(f -> false);
        this.clusterStateFeatures = clusterStateFeatures;
    }

    private static boolean hasHistoricalFeature(NavigableMap<Version, Set<String>> historicalFeatures, Version version, String featureId) {
        var allHistoricalFeatures = historicalFeatures.lastEntry().getValue();
        assert allHistoricalFeatures != null && allHistoricalFeatures.contains(featureId) : "Unknown historical feature " + featureId;
        var features = historicalFeatures.floorEntry(version);
        return features != null && features.getValue().contains(featureId);
    }

    boolean clusterHasFeature(String featureId) {
        if (clusterStateFeatures.contains(featureId)) {
            return true;
        }
        return historicalFeaturesPredicate.test(featureId);
    }
}
