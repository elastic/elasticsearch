/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;
import static org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings.extractUri;

public record HuggingFaceElserServiceSettings(URI uri, Integer maxInputTokens) implements ServiceSettings {

    public static final String NAME = "hugging_face_elser_service_settings";
    private static final Integer ELSER_TOKEN_LIMIT = 512;

    static final String URL = "url";

    public static HuggingFaceElserServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();
        var uri = extractUri(map, URL, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }
        return new HuggingFaceElserServiceSettings(uri, ELSER_TOKEN_LIMIT);
    }

    public HuggingFaceElserServiceSettings {
        Objects.requireNonNull(uri);
    }

    public HuggingFaceElserServiceSettings(String url) {
        this(createUri(url), ELSER_TOKEN_LIMIT);
    }

    public HuggingFaceElserServiceSettings(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(URL, uri.toString());
        builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        builder.endObject();

        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri.toString());
    }
}
