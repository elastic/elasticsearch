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
import java.util.Set;
import java.util.function.Predicate;

class LegacyESRestTestFeatureService implements TestFeatureService {
    private final Predicate<String> historicalFeaturesPredicate;
    private final Set<String> clusterStateFeatures;

    LegacyESRestTestFeatureService(
        List<? extends FeatureSpecification> specs,
        Collection<Version> nodeVersions,
        Set<String> clusterStateFeatures
    ) {
        var featureData = FeatureData.createFromSpecifications(specs);
        var historicalFeatures = featureData.getHistoricalFeatures();
        this.historicalFeaturesPredicate = TestFeatureService.createHistoricalFeaturePredicate(historicalFeatures, nodeVersions);
        this.clusterStateFeatures = clusterStateFeatures;
    }

    public boolean clusterHasFeature(String featureId) {
        if (clusterStateFeatures.contains(featureId)) {
            return true;
        }
        return historicalFeaturesPredicate.test(featureId);
    }
}
