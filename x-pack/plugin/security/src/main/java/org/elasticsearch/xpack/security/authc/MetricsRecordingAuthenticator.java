/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.telemetry.metric.LongCounter;

import java.util.Map;
import java.util.Objects;

/**
 * This class aims to simplify implementations of {@link Authenticator}s which support authentication metrics recording.
 */
abstract class MetricsRecordingAuthenticator implements Authenticator {

    private final LongCounter successCounter;
    private final LongCounter failuresCounter;

    MetricsRecordingAuthenticator(final LongCounter successCounter, final LongCounter failuresCounter) {
        this.successCounter = Objects.requireNonNull(successCounter);
        this.failuresCounter = Objects.requireNonNull(failuresCounter);
    }

    /**
     * Records successful authentication.
     *
     * @param attributes The additional info to associate with successful authentication.
     */
    protected void recordSuccessfulAuthentication(Map<String, Object> attributes) {
        this.successCounter.incrementBy(1L, attributes);
    }

    /**
     * Records failed authentication.
     *
     * @param attributes The additional info to associate with failed authentication.
     */
    public void recordFailedAuthentication(Map<String, Object> attributes) {
        this.failuresCounter.incrementBy(1L, attributes);
    }

}
