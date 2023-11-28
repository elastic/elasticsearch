/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.core.Strings;
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

    TestFeatureService(
        boolean hasHistoricalFeaturesInformation,
        List<? extends FeatureSpecification> specs,
        Collection<Version> nodeVersions,
        Set<String> clusterStateFeatures
    ) {
        var minNodeVersion = nodeVersions.stream().min(Version::compareTo);
        var featureData = FeatureData.createFromSpecifications(specs);
        var historicalFeatures = featureData.getHistoricalFeatures();
        var allHistoricalFeatures = historicalFeatures.lastEntry() == null ? Set.of() : historicalFeatures.lastEntry().getValue();

        var errorMessage = hasHistoricalFeaturesInformation
            ? "Check the feature has been added to the correct FeatureSpecification in the relevant module or, if this is a "
                + "legacy feature used only in tests, to a test-only FeatureSpecification"
            : "This test is running on the legacy test framework; historical features from production code will not be available."
                + " You need to port the test to the new test plugins in order to use historical features from production code."
                + " If this is a legacy feature used only in tests, you can add it to a test-only FeatureSpecification";
        this.historicalFeaturesPredicate = minNodeVersion.<Predicate<String>>map(v -> featureId -> {
            assert allHistoricalFeatures.contains(featureId) : Strings.format("Unknown historical feature %s: %s", featureId, errorMessage);
            return hasHistoricalFeature(historicalFeatures, v, featureId);
        }).orElse(featureId -> {
            // We can safely assume that new non-semantic versions (serverless) support all historical features
            assert allHistoricalFeatures.contains(featureId) : Strings.format("Unknown historical feature %s: %s", featureId, errorMessage);
            return true;
        });
        this.clusterStateFeatures = clusterStateFeatures;
    }

    private static boolean hasHistoricalFeature(NavigableMap<Version, Set<String>> historicalFeatures, Version version, String featureId) {
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
