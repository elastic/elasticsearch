/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * This class defines an empty secret settings object. This is useful for services that do not have any secret settings.
 */
public record EmptySecretSettings() implements SecretSettings {
    public static final String NAME = "empty_secret_settings";

    public static final EmptySecretSettings INSTANCE = new EmptySecretSettings();

    public EmptySecretSettings(StreamInput in) {
        this();
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
        return TransportVersions.ML_INFERENCE_EIS_INTEGRATION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {}
}
