/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * Unified Completion feature flag. When the feature is complete, this flag will be removed.
 * Enable feature via JVM option: `-Des.inference_unified_feature_flag_enabled=true`.
 */
public class UnifiedCompletionFeature {
    public static final FeatureFlag UNIFIED_COMPLETION_FEATURE_FLAG = new FeatureFlag("inference_unified");

    private UnifiedCompletionFeature() {}
}
