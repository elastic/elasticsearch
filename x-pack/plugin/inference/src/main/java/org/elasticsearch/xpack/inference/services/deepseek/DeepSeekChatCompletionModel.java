/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.deepseek;

import org.elasticsearch.TransportVersion;
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
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredSecureString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings.API_KEY;

/**
 * Model implementation for DeepSeek's chat completion service.
 * This class is responsible for holding the configuration and secrets necessary to make requests to DeepSeek's chat completion API,
 * as well as defining the rate limiting settings for those requests.
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

    /**
     * Creates a new {@link DeepSeekChatCompletionModel} from the given input parameters,
     * validating the presence and format of required fields in the service settings map.
     * @param inferenceEntityId the inference entity ID
     * @param taskType the task type
     * @param service the service name
     * @param serviceSettingsMap a map of service settings,
     *                           expected to contain the required fields for configuring a DeepSeek chat completion model,
     *                           which will be validated and extracted to build the model's configuration and secrets
     * @return the created {@link DeepSeekChatCompletionModel} if validation succeeds,
     * or an exception is thrown if validation fails due to missing or invalid fields in the service settings map
     */
    public static DeepSeekChatCompletionModel createFromNewInput(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettingsMap
    ) {
        var validationException = new ValidationException();

        var serviceSettings = buildAndValidateDeepSeekServiceSettings(serviceSettingsMap, validationException);
        var secureApiToken = extractRequiredSecureString(
            serviceSettingsMap,
            API_KEY,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        var taskSettings = new EmptyTaskSettings();
        var secretSettings = new DefaultSecretSettings(secureApiToken);
        var modelConfigurations = new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
        return new DeepSeekChatCompletionModel(serviceSettings, secretSettings, modelConfigurations, new ModelSecrets(secretSettings));
    }

    /**
     * Creates a {@link DeepSeekChatCompletionModel} from the given configuration and secrets maps,
     * validating the presence and format of required fields.
     * @param inferenceEntityId the inference entity ID
     * @param taskType the task type
     * @param service the service name
     * @param serviceSettingsMap the service settings map
     * @param secrets the secrets map
     * @return the created {@link DeepSeekChatCompletionModel}
     */
    public static DeepSeekChatCompletionModel readFromStorage(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> serviceSettingsMap,
        Map<String, Object> secrets
    ) {
        var validationException = new ValidationException();
        var serviceSettings = buildAndValidateDeepSeekServiceSettings(serviceSettingsMap, validationException);
        validationException.throwIfValidationErrorsExist();

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

    public DeepSeekChatCompletionModel(ModelConfigurations modelConfigurations, ModelSecrets modelSecrets) {
        this(
            (DeepSeekServiceSettings) modelConfigurations.getServiceSettings(),
            (DefaultSecretSettings) modelSecrets.getSecretSettings(),
            modelConfigurations,
            modelSecrets
        );
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

    /**
     * Builds a {@link DeepSeekServiceSettings} object from the given service settings map, validating required fields and formats.
     * @param serviceSettings the service settings map
     * @param validationException the validation exception to accumulate any validation errors
     * @return a {@link DeepSeekServiceSettings} object if validation succeeds,
     * or null if validation fails (with errors added to the validationException)
     */
    @Nullable
    private static DeepSeekServiceSettings buildAndValidateDeepSeekServiceSettings(
        Map<String, Object> serviceSettings,
        ValidationException validationException
    ) {
        int initialValidationErrorCount = validationException.validationErrors().size();
        var model = extractRequiredString(serviceSettings, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var uri = extractOptionalUri(serviceSettings, URL, validationException);
        if (validationException.validationErrors().size() > initialValidationErrorCount) {
            return null;
        }
        return new DeepSeekServiceSettings(model, uri);
    }

    /**
     * Service settings for DeepSeek's chat completion model, containing the model ID and an optional custom URI for the API endpoint.
     * @param modelId the DeepSeek model ID to use for chat completion requests
     * @param uri an optional custom URI for the DeepSeek chat completion API endpoint; if not provided, the default URI will be used
     */
    public record DeepSeekServiceSettings(String modelId, URI uri) implements ServiceSettings {
        private static final String NAME = "deep_seek_service_settings";
        private static final TransportVersion ML_INFERENCE_DEEPSEEK = TransportVersion.fromName("ml_inference_deepseek");

        public DeepSeekServiceSettings {
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
            assert false : "should never be called when supportsVersion is used";
            return ML_INFERENCE_DEEPSEEK;
        }

        @Override
        public boolean supportsVersion(TransportVersion version) {
            return version.supports(ML_INFERENCE_DEEPSEEK);
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

        /**
         * DeepSeek service settings are immutable, so this method simply returns the current instance without applying any updates.
         * @param serviceSettings a map with the new service settings
         * @return the current instance of {@link DeepSeekServiceSettings}, since the settings are immutable and cannot be updated
         */
        @Override
        public DeepSeekServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
            // DeepSeek service settings don't have any mutable fields
            return this;
        }
    }
}
