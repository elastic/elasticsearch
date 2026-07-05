/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.fireworksai;

import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;

/**
 * Defines the common settings interface for FireworksAI service implementations.
 * This interface is implemented by service settings classes that need to provide
 * rate limiting and URI configuration.
 */
public interface FireworksAiRateLimitServiceSettings {
    /**
     * @return the base URI for the FireworksAI API endpoint
     */
    URI uri();

    /**
     * @return the rate limit settings for this service
     */
    RateLimitSettings rateLimitSettings();
}
