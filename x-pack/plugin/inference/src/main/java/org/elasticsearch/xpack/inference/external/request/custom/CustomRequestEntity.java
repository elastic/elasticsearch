/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.custom;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings;
import org.elasticsearch.xpack.inference.services.custom.CustomTaskSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public record CustomRequestEntity(CustomServiceSettings serviceSettings, CustomTaskSettings taskSettings) implements ToXContentObject {

    public CustomRequestEntity {
        Objects.requireNonNull(serviceSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Map<String, Object> requestContent = serviceSettings.getRequestContent();
        Map<String, Object> taskSettingsContent = taskSettings.getParameters();
        builder.startObject();
        {
            for (String key : requestContent.keySet()) {
                builder.field(key, requestContent.get(key));
            }
            if (taskSettingsContent != null && taskSettingsContent.isEmpty() == false) {
                for (String key : taskSettingsContent.keySet()) {
                    builder.field(key, taskSettingsContent.get(key));
                }
            }
        }
        builder.endObject();
        return builder;
    }
}
