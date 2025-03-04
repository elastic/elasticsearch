/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.core.Nullable;

/**
 * @param elasticInferenceServiceUrl the upstream Elastic Inference Server's URL
 */
public record ElasticInferenceServiceComponents(@Nullable String elasticInferenceServiceUrl) {
    public static final ElasticInferenceServiceComponents EMPTY_INSTANCE = ElasticInferenceServiceComponents.of(null);

    public static ElasticInferenceServiceComponents of(String elasticInferenceServiceUrl) {
        return new ElasticInferenceServiceComponents(elasticInferenceServiceUrl);
    }
}
