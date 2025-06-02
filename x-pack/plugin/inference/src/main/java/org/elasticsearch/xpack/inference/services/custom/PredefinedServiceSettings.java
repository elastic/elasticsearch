/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Map;

// This class is a wrapper for CustomServiceSettings with the purpose of providing serialization that is backwards compatible with the
// existing format of serialized inference endpoints (for non-custom service endpoints)
public class PredefinedServiceSettings extends CustomServiceSettings {
    public static PredefinedServiceSettings fromMap(
        Map<String, Object> parsedMap,
        ConfigurationParseContext context,
        TaskType taskType,
        String inferenceId
    ) {
        return new PredefinedServiceSettings(CustomServiceSettings.fromMap(parsedMap, context, taskType));
    }

    public PredefinedServiceSettings(CustomServiceSettings customServiceSettings) {
        super(
            customServiceSettings.getTextEmbeddingSettings(),
            customServiceSettings.getUrl(),
            customServiceSettings.getHeaders(),
            customServiceSettings.getQueryParameters(),
            customServiceSettings.getRequestContentString(),
            customServiceSettings.getResponseJsonParser(),
            customServiceSettings.rateLimitSettings(),
            customServiceSettings.getParameters()
        );
    }

    // TODO: Check if there are any other values that need to be serialized here.
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(getParameters());
        return builder;
    }
}
