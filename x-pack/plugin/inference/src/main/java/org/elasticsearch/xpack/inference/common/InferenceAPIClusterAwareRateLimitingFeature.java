/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.xpack.inference.InferencePlugin;

/**
 * Cluster aware rate limiting feature flag. When the feature is complete and fully rolled out, this flag will be removed.
 * Enable feature via JVM option: `-Des.inference_cluster_aware_rate_limiting_feature_flag_enabled=true`.
 *
 * This controls, whether {@link InferenceServiceNodeLocalRateLimitCalculator} gets instantiated and
 * added as injectable {@link InferencePlugin} component.
 */
public class InferenceAPIClusterAwareRateLimitingFeature {

    public static final boolean INFERENCE_API_CLUSTER_AWARE_RATE_LIMITING_FEATURE_FLAG = new FeatureFlag(
        "inference_cluster_aware_rate_limiting"
    ).isEnabled();

    private InferenceAPIClusterAwareRateLimitingFeature() {}

}
