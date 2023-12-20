/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterFeatures;
import org.elasticsearch.core.Strings;
import org.elasticsearch.features.FeatureData;
import org.elasticsearch.features.FeatureSpecification;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Predicate;

public class TestFeatureService {
    private final Predicate<String> historicalFeaturesPredicate;
    private final Set<String> clusterStateFeatures;
    private final Map<String, Version> nodesVersions;
    private final Map<String, Set<String>> nodesFeatures;
    private final NavigableMap<Version, Set<String>> historicalFeatures;
    private final String errorMessage;

    TestFeatureService(
        boolean hasHistoricalFeaturesInformation,
        List<? extends FeatureSpecification> specs,
        Map<String, Version> nodesVersion,
        Map<String, Set<String>> nodesFeatures
    ) {
        this.nodesVersions = nodesVersion;
        this.nodesFeatures = nodesFeatures;

        var minNodeVersion = nodesVersion.values().stream().min(Version::compareTo);
        var featureData = FeatureData.createFromSpecifications(specs);
        this.historicalFeatures = featureData.getHistoricalFeatures();

        this.errorMessage = hasHistoricalFeaturesInformation
            ? "Check the feature has been added to the correct FeatureSpecification in the relevant module or, if this is a "
                + "legacy feature used only in tests, to a test-only FeatureSpecification"
            : "This test is running on the legacy test framework; historical features from production code will not be available."
                + " You need to port the test to the new test plugins in order to use historical features from production code."
                + " If this is a legacy feature used only in tests, you can add it to a test-only FeatureSpecification";
        this.historicalFeaturesPredicate = minNodeVersion.<Predicate<String>>map(
            v -> featureId -> hasHistoricalFeatureWithVersion(v, featureId)
        ).orElse(this::hasHistoricalFeatureWithoutVersion);
        this.clusterStateFeatures = ClusterFeatures.calculateAllNodeFeatures(nodesFeatures.values());
    }

    private boolean hasHistoricalFeatureWithVersion(Version v, String featureId) {
        assert getAllHistoricalFeatures().contains(featureId)
            : Strings.format("Unknown historical feature %s: %s", featureId, errorMessage);
        var features = historicalFeatures.floorEntry(v);
        return features != null && features.getValue().contains(featureId);
    }

    private boolean hasHistoricalFeatureWithoutVersion(String featureId) {
        // We can safely assume that new non-semantic versions (serverless) support all historical features
        assert getAllHistoricalFeatures().contains(featureId)
            : Strings.format("Unknown historical feature %s: %s", featureId, errorMessage);
        return true;
    }

    private Set<String> getAllHistoricalFeatures() {
        return historicalFeatures.lastEntry() == null ? Set.of() : historicalFeatures.lastEntry().getValue();
    }

    public boolean clusterHasFeature(String featureId) {
        if (clusterStateFeatures.contains(featureId)) {
            return true;
        }
        return historicalFeaturesPredicate.test(featureId);
    }

    public boolean nodeHasFeature(String nodeId, String featureId) {
        var nodeFeatures = nodesFeatures.get(nodeId);
        if (nodeFeatures != null && nodeFeatures.contains(featureId)) {
            return true;
        }

        if (nodesVersions.isEmpty()) {
            return hasHistoricalFeatureWithoutVersion(featureId);
        }

        var nodeVersion = nodesVersions.get(nodeId);
        return nodeVersion != null && hasHistoricalFeatureWithVersion(nodeVersion, featureId);
    }
}
