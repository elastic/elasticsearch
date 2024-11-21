/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.constantkeyword;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class ConstantKeywordFeatures implements FeatureSpecification {
    public static final NodeFeature SYNTHETIC_SOURCE_WRITE_FIX = new NodeFeature("constant_keyword.synthetic_source_write_fix");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of();
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(SYNTHETIC_SOURCE_WRITE_FIX);
    }
}
