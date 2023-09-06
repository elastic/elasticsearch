/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.ServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class ElserServiceSettings implements ServiceSettings {

    public static final String NAME = "elser";

    public static ElserServiceSettings fromMap(Map<String, Object> map) {
        return new ElserServiceSettings();
    }

    public ElserServiceSettings() {}

    public ElserServiceSettings(StreamInput in) {}

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
        return TransportVersion.V_8_500_070;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }

    @Override
    public int hashCode() {
        // TODO Class has no members all instances are equivalent
        // Return the hash of NAME to make the serialization tests poss
        return Objects.hashCode(NAME);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        return true;
    }
}
