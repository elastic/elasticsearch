/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

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
}
