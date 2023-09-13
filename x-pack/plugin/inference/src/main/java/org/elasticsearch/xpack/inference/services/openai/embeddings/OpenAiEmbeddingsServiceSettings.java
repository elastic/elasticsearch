/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.ServiceSettings;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class OpenAiEmbeddingsServiceSettings implements ServiceSettings {

    public static final String NAME = "openai_service_settings";
    public static final String API_TOKEN = "api_token";

    private final SecureString apiToken;

    public static OpenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String apiToken = MapParsingUtils.removeAsType(map, API_TOKEN, String.class);

        if (apiToken == null) {
            validationException.addValidationError(MapParsingUtils.missingSettingErrorMsg(API_TOKEN, Model.SERVICE_SETTINGS));
        } else if (apiToken.isEmpty()) {
            validationException.addValidationError(MapParsingUtils.mustBeNonEmptyString(API_TOKEN));
        }

        SecureString secureApiToken = new SecureString(Objects.requireNonNull(apiToken).toCharArray());

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new OpenAiEmbeddingsServiceSettings(secureApiToken);
    }

    public OpenAiEmbeddingsServiceSettings(SecureString apiToken) {
        this.apiToken = apiToken;
    }

    public OpenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        // TODO should this be readString?
        // TODO do we need to decrypt here?
        apiToken = in.readSecureString();
    }

    public SecureString getApiToken() {
        return apiToken;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // TODO encrypt here
        builder.field(API_TOKEN, apiToken.toString());
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_500_072;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO do we need to encrypt here?
        out.writeSecureString(apiToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiToken);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OpenAiEmbeddingsServiceSettings that = (OpenAiEmbeddingsServiceSettings) o;
        return apiToken.equals(that.apiToken);
    }

}
