/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;

/**
 * A builder for constructing an {@link ObjectParser} for parsing update requests for service settings.
 */
public class UpdateServiceSettingsOPBuilder<Value> extends AbstractSettingsOPBuilder<Value, UpdateServiceSettingsOPBuilder<Value>> {

    /**
     * Constructs an {@link UpdateServiceSettingsOPBuilder} that requires the rate limit settings and allows the
     * {@link org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings#API_KEY} field to be parsed but ignored in the
     * update request.
     * @param valueSupplier the supplier for the value
     * @param rateLimitSettingsSetter the setter for the rate limit settings
     * @return a new {@link UpdateServiceSettingsOPBuilder} instance
     */
    public static <V> UpdateServiceSettingsOPBuilder<V> of(
        Supplier<V> valueSupplier,
        BiConsumer<V, StatefulValue<RateLimitSettings>> rateLimitSettingsSetter
    ) {
        return new UpdateServiceSettingsOPBuilder<>(valueSupplier).enableRateLimitSettings(rateLimitSettingsSetter).allowApiKey();
    }

    private BiConsumer<Value, StatefulValue<RateLimitSettings>> rateLimitSettingsSetter;

    public UpdateServiceSettingsOPBuilder(Supplier<Value> valueSupplier) {
        super(SERVICE_SETTINGS, valueSupplier);
    }

    public UpdateServiceSettingsOPBuilder<Value> enableRateLimitSettings(
        BiConsumer<Value, StatefulValue<RateLimitSettings>> rateLimitSettingsSetter
    ) {
        this.rateLimitSettingsSetter = Objects.requireNonNull(rateLimitSettingsSetter);
        return this;
    }

    public ObjectParser<Value, Void> build() {
        var objectParser = new ObjectParser<Value, Void>(scope.toString(), valueSupplier);

        if (rateLimitSettingsSetter != null) {
            RateLimitSettings.declareUpdatableRateLimitSettings(objectParser, rateLimitSettingsSetter);
        }

        declareSecretFields(objectParser);

        return objectParser;
    }
}
