/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettingsTests;

import java.util.HashMap;
import java.util.Map;

public class VoyageAIServiceSettingsTests extends ESTestCase {

    public static VoyageAIServiceSettings createRandom() {
        var modelId = randomAlphaOfLength(15);
        return new VoyageAIServiceSettings(modelId, RateLimitSettingsTests.createRandom());
    }

    public static Map<String, Object> getServiceSettingsMap(String model) {
        var map = new HashMap<String, Object>();
        map.put(ServiceFields.MODEL_ID, model);
        map.put(
            RateLimitSettings.FIELD_NAME,
            new HashMap<>(Map.of(RateLimitSettings.REQUESTS_PER_MINUTE_FIELD, VoyageAIServiceSettings.DEFAULT_RATE_LIMIT_SETTINGS.requestsPerTimeUnit()))
        );
        return map;
    }
}
