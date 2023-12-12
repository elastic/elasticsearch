/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.ltr;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

/**
 * Learning to rank feature flag. When the feature is complete, this flag will be removed.
 *
 * Upon removal, ensure transport serialization is all corrected for future BWC.
 *
 * See {@link LearningToRankRescorerBuilder}
 */
public class LearningToRankRescorerFeature {
    /**
     * This setting behave like a feature flag for the learning to rank feature but can be set by an operator on self-managed environment.
     * The feature is enabled by default only for snapshots builds.
     *
     * See {@link LearningToRankRescorerBuilder}
     */
    public static Setting<Boolean> LEARNING_TO_RANK_ENABLED = Setting.boolSetting(
        "xpack.ml.learning_to_rank.enabled",
        Build.current().isSnapshot(),
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    private LearningToRankRescorerFeature() {}

    public static boolean isEnabled(Settings settings) {
        return LEARNING_TO_RANK_ENABLED.get(settings);
    }
}
