/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.features.FeatureData;
import org.elasticsearch.features.FeatureSpecification;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Predicate;

class ESRestTestFeatureService implements TestFeatureService {
    private final Predicate<String> historicalFeaturesPredicate;
    private final Set<String> clusterStateFeatures;
    private final Set<String> allSupportedFeatures;

    ESRestTestFeatureService(
        List<? extends FeatureSpecification> specs,
        Collection<Version> nodeVersions,
        Set<String> clusterStateFeatures
    ) {
        var minNodeVersion = nodeVersions.stream().min(Comparator.naturalOrder());
        var featureData = FeatureData.createFromSpecifications(specs);
        var historicalFeatures = featureData.getHistoricalFeatures();
        Set<String> allHistoricalFeatures = historicalFeatures.lastEntry() == null ? Set.of() : historicalFeatures.lastEntry().getValue();

        this.allSupportedFeatures = Sets.union(clusterStateFeatures, minNodeVersion.<Set<String>>map(v -> {
            var historicalFeaturesForVersion = historicalFeatures.floorEntry(v);
            return historicalFeaturesForVersion == null ? Set.of() : historicalFeaturesForVersion.getValue();
        }).orElse(allHistoricalFeatures));

        this.historicalFeaturesPredicate = minNodeVersion.<Predicate<String>>map(
            v -> featureId -> hasHistoricalFeature(historicalFeatures, v, featureId)
        ).orElse(featureId -> true); // We can safely assume that new non-semantic versions (serverless) support all historical features
        this.clusterStateFeatures = clusterStateFeatures;
    }

    private static boolean hasHistoricalFeature(NavigableMap<Version, Set<String>> historicalFeatures, Version version, String featureId) {
        var features = historicalFeatures.floorEntry(version);
        return features != null && features.getValue().contains(featureId);
    }

    @Override
    public boolean clusterHasFeature(String featureId) {
        if (clusterStateFeatures.contains(featureId)) {
            return true;
        }
        return historicalFeaturesPredicate.test(featureId);
    }

    @Override
    public Set<String> getAllSupportedFeatures() {
        return allSupportedFeatures;
    }
}
