/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * chunking_settings feature flag. When the feature is complete, this flag will be removed.
 */
public class ChunkingSettingsFeatureFlag {

    private ChunkingSettingsFeatureFlag() {}

    private static final FeatureFlag FEATURE_FLAG = new FeatureFlag("inference_chunking_settings");

    public static boolean isEnabled() {
        return FEATURE_FLAG.isEnabled();
    }
}
