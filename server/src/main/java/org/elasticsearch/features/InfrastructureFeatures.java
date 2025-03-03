/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import java.util.Set;

/**
 * This class specifies features for Elasticsearch infrastructure.
 */
public class InfrastructureFeatures implements FeatureSpecification {

    public static final NodeFeature ELASTICSEARCH_V9 = new NodeFeature("es_v9");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(ELASTICSEARCH_V9);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(FeatureService.TEST_FEATURES_ENABLED);
    }
}
