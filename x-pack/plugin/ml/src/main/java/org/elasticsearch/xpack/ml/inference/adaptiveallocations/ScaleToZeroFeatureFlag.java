/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.adaptiveallocations;

import org.elasticsearch.common.util.FeatureFlag;

public class ScaleToZeroFeatureFlag {
    private ScaleToZeroFeatureFlag() {}

    private static final FeatureFlag FEATURE_FLAG = new FeatureFlag("inference_scale_to_zero");

    public static boolean isEnabled() {
        return FEATURE_FLAG.isEnabled();
    }
}
