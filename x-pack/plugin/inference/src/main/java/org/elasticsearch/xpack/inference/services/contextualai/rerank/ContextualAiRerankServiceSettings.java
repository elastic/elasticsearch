/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.rerank;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

public class ContextualAiRerankServiceSettings extends FilteredXContentObject implements ServiceSettings {

    public static final String NAME = "contextualai_rerank_service_settings";

    // Default Contextual AI API endpoint
    private static final String DEFAULT_URL = "https://api.contextual.ai/v1/rerank";
    
    // Default rate limit settings - can be adjusted based on ContextualAI's actual limits
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(1000); // 1000 requests per minute

    private final URI uri;
    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public static ContextualAiRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        
        // Handle model_id differently based on context
        String modelId = switch (context) {
            case REQUEST -> {
                // For POST requests, model_id is optional - use from persisted settings if not provided
                yield extractOptionalString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
            }
            case PERSISTENT -> {
                // For PUT requests (registration), model_id is required
                yield extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
            }
        };
        
        RateLimitSettings rateLimitSettings = RateLimitSettings.of(map, null, validationException, "contextualai", context);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new ContextualAiRerankServiceSettings(
            ServiceUtils.createUri(url != null ? url : DEFAULT_URL), 
            modelId, // Can be null for REQUEST context
            rateLimitSettings
        );
    }

    public ContextualAiRerankServiceSettings(URI uri, @Nullable String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.uri = Objects.requireNonNull(uri);
        this.modelId = modelId; // Can be null for REQUEST context
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public ContextualAiRerankServiceSettings(StreamInput in) throws IOException {
        this.uri = ServiceUtils.createUri(in.readString());
        this.modelId = in.readString();
        this.rateLimitSettings = Objects.requireNonNullElse(in.readOptionalWriteable(RateLimitSettings::new), DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public URI uri() {
        return uri;
    }

    public String modelId() {
        return modelId;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(URL, uri.toString());
        builder.field(MODEL_ID, modelId);

        rateLimitSettings.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(URL, uri.toString());
        builder.field(MODEL_ID, modelId);

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri.toString());
        out.writeString(modelId);
        out.writeOptionalWriteable(rateLimitSettings);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ContextualAiRerankServiceSettings that = (ContextualAiRerankServiceSettings) object;
        return Objects.equals(uri, that.uri)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, modelId, rateLimitSettings);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }
}
