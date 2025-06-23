/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ElserMlNodeTaskSettings implements TaskSettings {

    public static final String NAME = "elser_mlnode_task_settings";

    public static final ElserMlNodeTaskSettings DEFAULT = new ElserMlNodeTaskSettings();

    public ElserMlNodeTaskSettings() {}

    public ElserMlNodeTaskSettings(StreamInput in) {}

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_11_X;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        // TODO Class has no members all instances are equivalent
        // Return the hash of NAME to make the serialization tests pass
        return Objects.hash(NAME);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return DEFAULT;
    }
}
