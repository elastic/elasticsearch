/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * Learning to rank feature flag. When the feature is complete, this flag will be removed.
 *
 * Upon removal, ensure transport serialization is all corrected for future BWC.
 *
 * See {@link LearningToRankRescorerBuilder}
 */
public class LearningToRankRescorerFeature {

    private LearningToRankRescorerFeature() {}

    private static final FeatureFlag LEARNING_TO_RANK = new FeatureFlag("learning_to_rank");

    public static boolean isEnabled() {
        return LEARNING_TO_RANK.isEnabled();
    }
}
