/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xpack.application.rules.QueryRule;

import java.util.Set;

public class EnterpriseSearchFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(QueryRule.NUMERIC_VALIDATION);
    }
}
