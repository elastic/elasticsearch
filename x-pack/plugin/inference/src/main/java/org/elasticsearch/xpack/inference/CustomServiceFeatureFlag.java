/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.util.FeatureFlag;

public class CustomServiceFeatureFlag {
    /**
     * {@link org.elasticsearch.xpack.inference.services.custom.CustomService} feature flag. When the feature is complete,
     * this flag will be removed.
     * Enable feature via JVM option: `-Des.inference_custom_service_feature_flag_enabled=true`.
     */
    public static final FeatureFlag CUSTOM_SERVICE_FEATURE_FLAG = new FeatureFlag("inference_custom_service");

    private CustomServiceFeatureFlag() {}
}
