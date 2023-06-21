/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * Inference rescorer feature flag. When the feature is complete, this flag will be removed.
 *
 * Upon removal, ensure transport serialization is all corrected for future BWC.
 *
 * See {@link InferenceRescorerBuilder}
 */
public class InferenceRescorerFeature {

    private InferenceRescorerFeature() {}

    private static final FeatureFlag INFERENCE_RESCORE_FEATURE_FLAG = new FeatureFlag("inference_rescorer");

    public static boolean isEnabled() {
        return INFERENCE_RESCORE_FEATURE_FLAG.isEnabled();
    }
}
