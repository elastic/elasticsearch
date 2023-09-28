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
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class Model implements ToXContentObject, VersionedNamedWriteable {
    public static String documentId(String modelId) {
        return "model_" + modelId;
    }

    private static final String NAME = "inference_model";
    private final ModelConfigurations configurations;
    private final ModelSecrets secrets;

    public Model(ModelConfigurations configurations, ModelSecrets secrets) {
        this.configurations = Objects.requireNonNull(configurations);
        this.secrets = Objects.requireNonNull(secrets);
    }

    public Model(ModelConfigurations configurations) {
        this(configurations, new ModelSecrets());

    }

    public Model(StreamInput in) throws IOException {
        this(new ModelConfigurations(in), new ModelSecrets());
    }

    /**
     * Returns the model's non-sensitive configurations (e.g. service name).
     */
    public ModelConfigurations getConfigurations() {
        return configurations;
    }

    /**
     * Returns the model's sensitive configurations (e.g. api key).
     *
     * This returns an object that in json would look like:
     *
     * <pre>
     * {@code
     * {
     *     "secret_settings": { "api_key": "abc" }
     * }
     * }
     * </pre>
     */
    public ModelSecrets getSecrets() {
        return secrets;
    }

    public ServiceSettings getServiceSettings() {
        return configurations.getServiceSettings();
    }

    public TaskSettings getTaskSettings() {
        return configurations.getTaskSettings();
    }

    /**
     * Returns the inner sensitive data defined by a particular service.
     *
     * This returns an object that in json would look like:
     *
     * <pre>
     * {@code
     * {
     *     "api_key": "abc"
     * }
     * }
     * </pre>
     */
    public SecretSettings getSecretSettings() {
        return secrets.getSecretSettings();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.INFERENCE_MODEL_SECRETS_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO should we wrap this in an object? Probably not
        configurations.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return configurations.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Model model = (Model) o;
        return Objects.equals(configurations, model.configurations) && Objects.equals(secrets, model.secrets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configurations, secrets);
    }
}
