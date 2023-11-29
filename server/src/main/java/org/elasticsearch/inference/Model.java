/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import java.util.Objects;

public class Model {
    public static String documentId(String modelId) {
        return "model_" + modelId;
    }

    private final ModelConfigurations configurations;
    private final ModelSecrets secrets;

    public Model(ModelConfigurations configurations, ModelSecrets secrets) {
        this.configurations = Objects.requireNonNull(configurations);
        this.secrets = Objects.requireNonNull(secrets);
    }

    public Model(ModelConfigurations configurations) {
        this(configurations, new ModelSecrets());
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
