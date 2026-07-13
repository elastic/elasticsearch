/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.icu;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class AnalysisIcuFeatures implements FeatureSpecification {

    public static final NodeFeature ICU_TRANSFORM_CUSTOM_RULESET = new NodeFeature("analysis.icu_transform.custom_ruleset");
    public static final NodeFeature ICU_COLLATION_KEYWORD_BINARY_DV = new NodeFeature("mapper.icu_collation_keyword.supports_binary_dv");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(ICU_TRANSFORM_CUSTOM_RULESET, ICU_COLLATION_KEYWORD_BINARY_DV);
    }
}
