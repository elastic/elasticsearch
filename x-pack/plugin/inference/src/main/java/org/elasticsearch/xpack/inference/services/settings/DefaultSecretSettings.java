/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceConfiguration;
import org.elasticsearch.inference.configuration.ServiceConfigurationDisplayType;
import org.elasticsearch.inference.configuration.ServiceConfigurationFieldType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredSecureString;

/**
 * Contains secret settings that are common to all services.
 * @param apiKey the key used to authenticate with the 3rd party service
 */
public record DefaultSecretSettings(SecureString apiKey) implements SecretSettings, ApiKeySecrets {
    public static final String NAME = "default_secret_settings";

    static final String API_KEY = "api_key";

    public static DefaultSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        ValidationException validationException = new ValidationException();
        SecureString secureApiToken = extractRequiredSecureString(map, API_KEY, ModelSecrets.SECRET_SETTINGS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new DefaultSecretSettings(secureApiToken);
    }

    public static Map<String, ServiceConfiguration> toServiceConfigurationWithTooltip(String tooltip) {
        var configurationMap = new HashMap<String, ServiceConfiguration>();
        configurationMap.put(
            API_KEY,
            new ServiceConfiguration.Builder().setDisplay(ServiceConfigurationDisplayType.TEXTBOX)
                .setLabel("API Key")
                .setOrder(1)
                .setRequired(true)
                .setSensitive(true)
                .setTooltip(tooltip)
                .setType(ServiceConfigurationFieldType.STRING)
                .build()
        );
        return configurationMap;
    }

    public static Map<String, ServiceConfiguration> toServiceConfiguration() {
        return DefaultSecretSettings.toServiceConfigurationWithTooltip("API Key for the provider you're connecting to.");
    }

    public DefaultSecretSettings {
        Objects.requireNonNull(apiKey);
    }

    public DefaultSecretSettings(StreamInput in) throws IOException {
        this(in.readSecureString());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(API_KEY, apiKey.toString());
        builder.endObject();
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_12_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(apiKey);
    }

    @Override
    public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
        return fromMap(new HashMap<>(newSecrets));
    }
}
