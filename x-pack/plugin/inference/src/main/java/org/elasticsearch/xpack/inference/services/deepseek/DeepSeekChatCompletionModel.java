/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredSecureString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

/**
 * Design notes:
 * This provider tries to match the OpenAI, so we'll design around that as well.
 *
 * Task Type:
 * - Chat Completion
 *
 * Service Settings:
 * - api_key
 * - model
 * - url
 *
 * Task Settings:
 * - nothing?
 *
 * Rate Limiting:
 * - The website claims to want unlimited, so we're setting it as MAX_INT per minute?
 */
public class DeepSeekChatCompletionModel extends Model {
    // Per-node rate limit group and settings, limiting the outbound requests this node can make to INTEGER.MAX_VALUE per minute.
    private static final Object RATE_LIMIT_GROUP = new Object();
    private static final RateLimitSettings RATE_LIMIT_SETTINGS = new RateLimitSettings(Integer.MAX_VALUE);

    private static final URI DEFAULT_URI = URI.create("https://api.deepseek.com/chat/completions");
    private final DeepSeekServiceSettings serviceSettings;
    @Nullable
    private final DefaultSecretSettings secretSettings;

    public static List<NamedWriteableRegistry.Entry> namedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(ServiceSettings.class, DeepSeekServiceSettings.NAME, DeepSeekServiceSettings::new));
    }

    public static DeepSeekChatCompletionModel createFromNewInput(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettingsMap
    ) {
        var validationException = new ValidationException();

        var model = extractRequiredString(serviceSettingsMap, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = createOptionalUri(
            extractOptionalString(serviceSettingsMap, URL, ModelConfigurations.SERVICE_SETTINGS, validationException)
        );
        var secureApiToken = extractRequiredSecureString(
            serviceSettingsMap,
            "api_key",
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        var serviceSettings = new DeepSeekServiceSettings(model, uri);
        var taskSettings = new EmptyTaskSettings();
        var secretSettings = new DefaultSecretSettings(secureApiToken);
        var modelConfigurations = new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
        return new DeepSeekChatCompletionModel(serviceSettings, secretSettings, modelConfigurations, new ModelSecrets(secretSettings));
    }

    public static DeepSeekChatCompletionModel readFromStorage(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettingsMap,
        Map<String, Object> secrets
    ) {
        var validationException = new ValidationException();

        var model = extractRequiredString(serviceSettingsMap, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = createOptionalUri(
            extractOptionalString(serviceSettingsMap, "url", ModelConfigurations.SERVICE_SETTINGS, validationException)
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        var serviceSettings = new DeepSeekServiceSettings(model, uri);
        var taskSettings = new EmptyTaskSettings();
        var secretSettings = DefaultSecretSettings.fromMap(secrets);
        var modelConfigurations = new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
        return new DeepSeekChatCompletionModel(serviceSettings, secretSettings, modelConfigurations, new ModelSecrets(secretSettings));
    }

    private DeepSeekChatCompletionModel(
        DeepSeekServiceSettings serviceSettings,
        @Nullable DefaultSecretSettings secretSettings,
        ModelConfigurations configurations,
        ModelSecrets secrets
    ) {
        super(configurations, secrets);
        this.serviceSettings = serviceSettings;
        this.secretSettings = secretSettings;
    }

    public Optional<SecureString> apiKey() {
        return Optional.ofNullable(secretSettings).map(DefaultSecretSettings::apiKey);
    }

    public String model() {
        return serviceSettings.modelId();
    }

    public URI uri() {
        return serviceSettings.uri() != null ? serviceSettings.uri() : DEFAULT_URI;
    }

    public Object rateLimitGroup() {
        return RATE_LIMIT_GROUP;
    }

    public RateLimitSettings rateLimitSettings() {
        return RATE_LIMIT_SETTINGS;
    }

    private record DeepSeekServiceSettings(String modelId, URI uri) implements ServiceSettings {
        private static final String NAME = "deep_seek_service_settings";

        DeepSeekServiceSettings {
            Objects.requireNonNull(modelId);
        }

        DeepSeekServiceSettings(StreamInput in) throws IOException {
            this(in.readString(), in.readOptional(url -> URI.create(url.readString())));
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.ML_INFERENCE_DEEPSEEK;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(modelId);
            out.writeOptionalString(uri != null ? uri.toString() : null);
        }

        @Override
        public ToXContentObject getFilteredXContentObject() {
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_ID, modelId);
            if (uri != null) {
                builder.field(URL, uri.toString());
            }
            return builder.endObject();
        }
    }
}
