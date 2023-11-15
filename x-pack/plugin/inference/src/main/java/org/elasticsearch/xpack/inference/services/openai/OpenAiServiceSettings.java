/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.MapParsingUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.createUri;
import static org.elasticsearch.xpack.inference.services.MapParsingUtils.extractOptionalString;

/**
 * Defines the base settings for interacting with OpenAI.
 * @param uri an optional uri to override the openai url. This should only be used for testing.
 */
public record OpenAiServiceSettings(@Nullable URI uri, @Nullable String organizationId) implements ServiceSettings {

    public static final String NAME = "openai_service_settings";

    public static final String URL = "url";
    public static final String ORGANIZATION = "organization_id";

    public static OpenAiServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String organizationId = extractOptionalString(map, ORGANIZATION, ModelConfigurations.SERVICE_SETTINGS, validationException);

        // Throw if any of the settings were empty strings
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        // the url is optional and only for testing
        if (url == null) {
            return new OpenAiServiceSettings((URI) null, organizationId);
        }

        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiServiceSettings(uri, organizationId);
    }

    public OpenAiServiceSettings(@Nullable String url, @Nullable String organizationId) {
        this(createOptionalUri(url), organizationId);
    }

    private static URI createOptionalUri(String url) {
        if (url == null) {
            return null;
        }

        return createUri(url);
    }

    public OpenAiServiceSettings(StreamInput in) throws IOException {
        this(in.readOptionalString(), in.readOptionalString());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (uri != null) {
            builder.field(URL, uri.toString());
        }

        if (organizationId != null) {
            builder.field(ORGANIZATION, organizationId);
        }

        builder.endObject();
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_OPENAI_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalString(organizationId);
    }
}
