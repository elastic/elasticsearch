/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.inference.TaskSettings;

import java.io.IOException;
import java.util.Map;

public class DefaultTaskSettings extends SettingsMap implements TaskSettings {
    DefaultTaskSettings(Map<String, Object> headers, Map<String, Object> body) {
        super(headers, body);
    }

    DefaultTaskSettings(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return "inference_default_task_settings";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }
}
