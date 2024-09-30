/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * Elastic Inference Service (EIS) feature flag. When the feature is complete, this flag will be removed.
 * Enable feature via JVM option: `-Des.eis_feature_flag_enabled=true`.
 */
public class ElasticInferenceServiceFeature {

    public static final FeatureFlag ELASTIC_INFERENCE_SERVICE_FEATURE_FLAG = new FeatureFlag("eis");

}
