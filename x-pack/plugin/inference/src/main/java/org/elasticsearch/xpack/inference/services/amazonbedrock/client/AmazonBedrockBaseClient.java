/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.client;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockModel;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

public abstract class AmazonBedrockBaseClient implements AmazonBedrockClient {
    protected final Integer modelKeysAndRegionHashcode;
    protected Clock clock = Clock.systemUTC();
    protected volatile Instant expiryTimestamp;

    protected AmazonBedrockBaseClient(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        Objects.requireNonNull(model);
        this.modelKeysAndRegionHashcode = getModelKeysAndRegionHashcode(model, timeout);
    }

    public static Integer getModelKeysAndRegionHashcode(AmazonBedrockModel model, @Nullable TimeValue timeout) {
        var secretSettings = model.getSecretSettings();
        var serviceSettings = model.getServiceSettings();
        return Objects.hash(secretSettings.accessKey(), secretSettings.secretKey(), serviceSettings.region(), timeout);
    }

    public final void setClock(Clock clock) {
        this.clock = clock;
    }

    // used for testing
    Instant getExpiryTimestamp() {
        return this.expiryTimestamp;
    }

    abstract void close();
}
