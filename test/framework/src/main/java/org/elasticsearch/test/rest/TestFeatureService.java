/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.Predicate;

public interface TestFeatureService {
    TestFeatureService ALL_FEATURES = ignored -> true;

    static Predicate<String> createHistoricalFeaturePredicate(
        NavigableMap<Version, Set<String>> historicalFeatures,
        Collection<Version> nodeVersions
    ) {
        var minNodeVersion = nodeVersions.stream().min(Version::compareTo);
        return minNodeVersion.<Predicate<String>>map(
            v -> featureId -> TestFeatureService.hasHistoricalFeature(historicalFeatures, v, featureId)
        ).orElse(featureId -> true); // We can safely assume that new non-semantic versions (serverless) support all historical features
    }

    static boolean hasHistoricalFeature(NavigableMap<Version, Set<String>> historicalFeatures, Version version, String featureId) {
        var features = historicalFeatures.floorEntry(version);
        return features != null && features.getValue().contains(featureId);
    }

    boolean clusterHasFeature(String featureId);
}
