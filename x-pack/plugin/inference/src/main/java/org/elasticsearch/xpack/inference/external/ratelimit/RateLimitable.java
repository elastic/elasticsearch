/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.ratelimit;

import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

/**
 * Defines the contract for the settings and grouping of requests for how they are rate limited.
 */
public interface RateLimitable {

    /**
     * Returns the settings defining how to initialize a {@link org.elasticsearch.xpack.inference.common.RateLimiter}
     */
    RateLimitSettings rateLimitSettings();

    /**
     * Returns an object responsible for containing the all the fields that uniquely identify how a request will be rate limited.
     * In practice the class should contain things like api key, url, model, or any headers that would impact rate limiting.
     * The class must implement hashcode such that these fields are taken into account.
     *
     * The returned object defines the bucket that a request should be placed when determine how it is rate limited.
     */
    Object rateLimitGrouping();
}
