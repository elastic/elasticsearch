/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.ServiceSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomElandInternalServiceSettings extends ElasticsearchInternalServiceSettings {

    public static final String NAME = "custom_eland_model_internal_service_settings";

    public CustomElandInternalServiceSettings(ElasticsearchInternalServiceSettings other) {
        super(other);
    }

    public CustomElandInternalServiceSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return CustomElandInternalServiceSettings.NAME;
    }

    @Override
    public ServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        serviceSettings = new HashMap<>(serviceSettings);
        ServiceSettings updated = super.updateServiceSettings(serviceSettings);
        if (updated instanceof ElasticsearchInternalServiceSettings esSettings) {
            return new CustomElandInternalServiceSettings(esSettings);
        } else {
            throw new IllegalStateException("Unexpected service settings type [" + updated.getClass().getName() + "]");
        }
    }
}
