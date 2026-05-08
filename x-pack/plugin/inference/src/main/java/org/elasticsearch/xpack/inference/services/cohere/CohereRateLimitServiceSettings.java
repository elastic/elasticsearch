/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;

public interface CohereRateLimitServiceSettings {
    RateLimitSettings rateLimitSettings();

    CohereCommonServiceSettings.CohereApiVersion apiVersion();

    /**
     * Returns the URI override for the Cohere service endpoint, or {@code null} to use the default endpoint.
     * This is an internal field used to preserve custom URLs from pre-refactoring persisted models;
     * it is not exposed in the user-facing API.
     */
    default URI uri() {
        return null;
    }
}
