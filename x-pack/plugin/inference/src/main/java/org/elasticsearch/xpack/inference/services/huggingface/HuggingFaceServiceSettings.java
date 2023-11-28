/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.createUri;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.extractRequiredString;

public record HuggingFaceServiceSettings(URI uri) implements ServiceSettings {
    public static final String NAME = "hugging_face_service_settings";

    static final String URL = "url";

    public static HuggingFaceServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String parsedUrl = extractRequiredString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        URI uri = convertToUri(parsedUrl, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new HuggingFaceServiceSettings(uri);
    }

    public HuggingFaceServiceSettings {
        Objects.requireNonNull(uri);
    }

    public HuggingFaceServiceSettings(String url) {
        this(createUri(url));
    }

    public HuggingFaceServiceSettings(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(URL, uri.toString());
        builder.endObject();

        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_HF_SERVICE_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri.toString());
    }
}
