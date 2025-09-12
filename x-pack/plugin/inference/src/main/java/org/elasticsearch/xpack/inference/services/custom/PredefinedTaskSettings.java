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

public class PredefinedTaskSettings extends CustomTaskSettings {
    public static PredefinedTaskSettings fromMap(Map<String, Object> parsedMap) {
        return new PredefinedTaskSettings(CustomTaskSettings.fromMap(parsedMap));
    }

    public static PredefinedTaskSettings fromMap(PredefinedCustomServiceSchema schema, Map<String, Object> map) {
        return new PredefinedTaskSettings(CustomTaskSettings.fromMap(parseTaskSettingsMap(schema, map)));
    }

    public PredefinedTaskSettings(CustomTaskSettings customTaskSettings) {
        super(customTaskSettings.getParameters());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(getParameters());
        return builder;
    }

    private static Map<String, Object> parseTaskSettingsMap(
        PredefinedCustomServiceSchema schema,
        Map<String, Object> unparsedTaskSettingsMap
    ) {
        Map<String, Object> parsedTaskSettingsMap = new HashMap<>();

        Map<String, Object> parameters = new HashMap<>();
        for (String parameter : schema.getTaskSettingsParameters()) {
            if (unparsedTaskSettingsMap.containsKey(parameter)) {
                parameters.put(parameter, unparsedTaskSettingsMap.remove(parameter));
            }
        }
        parsedTaskSettingsMap.put("parameters", parameters);
        return parsedTaskSettingsMap;
    }
}
