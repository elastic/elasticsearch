/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features.plugin;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestFeaturesForwarderSpecification implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        // ServiceLoader doesn't return this class...
        return Stream.concat(
            getTestFeatures().stream(),
            ServiceLoader.load(FeatureSpecification.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .flatMap(s -> s.getTestFeatures().stream())
        ).collect(Collectors.toSet());
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(TestFeaturesPlugin.TEST_FEATURES_ENABLED);
    }
}
