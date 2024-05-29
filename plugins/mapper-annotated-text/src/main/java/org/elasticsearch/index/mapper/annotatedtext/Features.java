/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.annotatedtext;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Provides features for annotated text mapper.
 */
public class Features implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            AnnotatedTextFieldMapper.SYNTHETIC_SOURCE_SUPPORT // Added in 8.15
        );
    }
}
