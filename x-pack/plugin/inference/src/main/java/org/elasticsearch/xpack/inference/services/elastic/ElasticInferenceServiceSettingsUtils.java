/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.ModelConfigurations;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveIntegerLessThanOrEqualToMax;

public final class ElasticInferenceServiceSettingsUtils {

    public static final int MAX_BATCH_SIZE_UPPER_BOUND = 1024;
    public static final String MAX_BATCH_SIZE = "max_batch_size";
    public static final TransportVersion INFERENCE_API_EIS_MAX_BATCH_SIZE = TransportVersion.fromName("inference_api_eis_max_batch_size");

    private ElasticInferenceServiceSettingsUtils() {}

    public static Integer parseMaxBatchSize(Map<String, Object> serviceSettings, ValidationException validationException) {
        return extractOptionalPositiveIntegerLessThanOrEqualToMax(
            serviceSettings,
            MAX_BATCH_SIZE,
            MAX_BATCH_SIZE_UPPER_BOUND,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
    }
}
