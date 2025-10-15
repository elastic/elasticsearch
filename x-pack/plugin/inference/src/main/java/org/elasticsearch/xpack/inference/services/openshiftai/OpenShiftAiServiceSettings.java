/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

/**
 * Represents the settings for an OpenShift AI service.
 * This class encapsulates the model ID, URI, and rate limit settings for the OpenShift AI service.
 */
public abstract class OpenShiftAiServiceSettings extends FilteredXContentObject implements ServiceSettings {
    // There is no default rate limit for OpenShift AI, so we set a reasonable default of 3000 requests per minute
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(3000);

    protected final String modelId;
    protected final URI uri;
    protected final RateLimitSettings rateLimitSettings;

    /**
     * Constructs a new OpenShiftAiServiceSettings from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs during reading
     */
    protected OpenShiftAiServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readOptionalString();
        this.uri = createUri(in.readString());
        this.rateLimitSettings = new RateLimitSettings(in);
    }

    /**
     * Constructs a new OpenShiftAiServiceSettings.
     *
     * @param modelId the ID of the modelId
     * @param uri the URI of the service
     * @param rateLimitSettings the rate limit settings for the service
     */
    protected OpenShiftAiServiceSettings(@Nullable String modelId, URI uri, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = modelId;
        this.uri = uri;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return OpenShiftAiUtils.ML_INFERENCE_OPENSHIFT_AI_ADDED;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return OpenShiftAiUtils.supportsOpenShiftAi(version);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(modelId);
        out.writeString(uri.toString());
        rateLimitSettings.writeTo(out);
    }

    @Override
    public String modelId() {
        return this.modelId;
    }

    /**
     * Returns the URI of the OpenShift AI service.
     *
     * @return the URI of the service
     */
    public URI uri() {
        return this.uri;
    }

    /**
     * Returns the rate limit settings for the OpenShift AI service.
     *
     * @return the rate limit settings
     */
    public RateLimitSettings rateLimitSettings() {
        return this.rateLimitSettings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }
        builder.field(URL, uri.toString());
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }
}
