/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.generic.model;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class GenericInferenceTaskSettings implements TaskSettings {
    private final String NAME = "generic_service_task_settings";
    private final Map<String, Object> settings;

    public GenericInferenceTaskSettings(Map<String, Object> settings) {
        this.settings = settings;
    }

    public GenericInferenceTaskSettings(StreamInput in) throws IOException {
        this.settings = in.readGenericMap();
    }

    @Override
    public boolean isEmpty() {
        return settings.isEmpty();
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return new GenericInferenceTaskSettings(newSettings);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return null; // TODO: add minimal support version when launching generic inference service
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericMap(settings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.map(settings);
        return builder;
    }
}
