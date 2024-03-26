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
import org.elasticsearch.core.Strings;
import org.elasticsearch.features.FeatureData;
import org.elasticsearch.features.FeatureSpecification;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

class ESRestTestFeatureService implements TestFeatureService {
    private final boolean validateFeatures;
    private final Set<String> clusterStateFeatures;
    private final Set<String> allSupportedFeatures;
    private final Set<String> allHistoricalFeatureNames;
    private final Set<String> allFeatureNames;

    ESRestTestFeatureService(
        boolean validateFeatures,
        Set<String> featureNames,
        List<? extends FeatureSpecification> specs,
        Collection<Version> nodeVersions,
        Set<String> clusterStateFeatures
    ) {
        this.validateFeatures = validateFeatures;
        this.allFeatureNames = featureNames;
        var featureData = FeatureData.createFromSpecifications(specs);
        var historicalFeatures = featureData.getHistoricalFeatures();
        this.allHistoricalFeatureNames = historicalFeatures.lastEntry() == null ? Set.of() : historicalFeatures.lastEntry().getValue();
        this.clusterStateFeatures = clusterStateFeatures;

        var minNodeVersion = nodeVersions.stream().min(Comparator.naturalOrder());

        // FIXME deduplicate
        this.allSupportedFeatures = Sets.union(clusterStateFeatures, minNodeVersion.<Set<String>>map(v -> {
            var historicalFeaturesForVersion = historicalFeatures.floorEntry(v);
            return historicalFeaturesForVersion == null ? Set.of() : historicalFeaturesForVersion.getValue();
        }).orElse(allHistoricalFeatureNames));
    }

    @Override
    public boolean clusterHasFeature(String featureId) {
        if (validateFeatures && allFeatureNames.contains(featureId) == false && allHistoricalFeatureNames.contains(featureId) == false) {
            throw new IllegalArgumentException(
                Strings.format(
                    "Unknown feature %s: check the feature has been added to the correct FeatureSpecification in the relevant module or, "
                        + "if this is a legacy feature used only in tests, to a test-only FeatureSpecification such as %s.",
                    featureId,
                    RestTestLegacyFeatures.class.getCanonicalName()
                )
            );
        }
        if (allFeatureNames.contains(featureId)) {
            return clusterStateFeatures.contains(featureId);
        }
        return allSupportedFeatures.contains(featureId);
    }

    @Override
    public Set<String> getAllSupportedFeatures() {
        return allSupportedFeatures;
    }
}
