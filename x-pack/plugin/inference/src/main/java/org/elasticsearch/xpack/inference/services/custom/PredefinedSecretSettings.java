/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PredefinedSecretSettings extends CustomSecretSettings {
    public static PredefinedSecretSettings fromMap(PredefinedCustomServiceSchema schema, Map<String, Object> map) {
        return new PredefinedSecretSettings(CustomSecretSettings.fromMap(parseSecretSettings(schema, map)));
    }

    public PredefinedSecretSettings(CustomSecretSettings customSecretSettings) {
        super(customSecretSettings.getSecretParameters());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        var secretParameters = getSecretParameters();
        builder.startObject();
        if (secretParameters.isEmpty() == false) {
            for (var entry : secretParameters.entrySet()) {
                builder.field(entry.getKey(), entry.getValue().toString());
            }
        }
        builder.endObject();
        return builder;
    }

    private static Map<String, Object> parseSecretSettings(
        PredefinedCustomServiceSchema schema,
        Map<String, Object> unparsedSecretSettingsMap
    ) {
        Map<String, Object> secretParameters = new HashMap<>();
        for (String secretParameter : schema.getServiceSettingsSecretParameters()) {
            if (unparsedSecretSettingsMap.containsKey(secretParameter)) {
                secretParameters.put(secretParameter, unparsedSecretSettingsMap.remove(secretParameter));
            }
        }

        return new HashMap<>(Map.of(SECRET_PARAMETERS, secretParameters));
    }
}
