/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;

/**
 * A builder for constructing an {@link ObjectParser} for parsing update requests for service settings.
 */
public class UpdateServiceSettingsOPBuilder<Value> {

    /**
     * Constructs an {@link UpdateServiceSettingsOPBuilder} that requires the rate limit settings and allows the
     * {@link DefaultSecretSettings#API_KEY} field to be parsed but ignored in the update request.
     * @param valueSupplier the supplier for the value
     * @param rateLimitSettingsSetter the setter for the rate limit settings
     * @return a new {@link UpdateServiceSettingsOPBuilder} instance
     */
    public static <V> UpdateServiceSettingsOPBuilder<V> of(
        Supplier<V> valueSupplier,
        BiConsumer<V, StatefulValue<RateLimitSettings>> rateLimitSettingsSetter
    ) {
        return new UpdateServiceSettingsOPBuilder<>(valueSupplier).setRateLimitSettings(rateLimitSettingsSetter).allowApiKey();
    }

    private final Supplier<Value> valueSupplier;
    private BiConsumer<Value, StatefulValue<RateLimitSettings>> rateLimitSettingsSetter;
    private final Set<String> secretFields = new LinkedHashSet<>();

    public UpdateServiceSettingsOPBuilder(Supplier<Value> valueSupplier) {
        this.valueSupplier = Objects.requireNonNull(valueSupplier);
    }

    public UpdateServiceSettingsOPBuilder<Value> setRateLimitSettings(
        BiConsumer<Value, StatefulValue<RateLimitSettings>> rateLimitSettingsSetter
    ) {
        this.rateLimitSettingsSetter = rateLimitSettingsSetter;
        return this;
    }

    public UpdateServiceSettingsOPBuilder<Value> allowApiKey() {
        return allowSecretFields(DefaultSecretSettings.API_KEY);
    }

    /**
     * Declares the given field names as no-ops in the update parser. Secret fields (e.g. {@code api_key}, {@code access_key}) appear in
     * the same JSON block as service settings in update requests. The service's secret settings extract them separately; these
     * declarations prevent the strict parser from rejecting them as unknown fields.
     * <p>
     * Duplicate field names — whether from multiple calls or within a single varargs list — are ignored. It is therefore safe to call
     * this method with a field that was already declared by a previous call or by {@link #allowApiKey()}.
     */
    public UpdateServiceSettingsOPBuilder<Value> allowSecretFields(String... fieldNames) {
        secretFields.addAll(List.of(fieldNames));
        return this;
    }

    public ObjectParser<Value, Void> build() {
        var objectParser = new ObjectParser<Value, Void>(SERVICE_SETTINGS.toString(), valueSupplier);

        if (rateLimitSettingsSetter != null) {
            RateLimitSettings.declareUpdatableRateLimitSettings(objectParser, rateLimitSettingsSetter);
        }

        for (var field : secretFields) {
            objectParser.declareString((b, v) -> {}, new ParseField(field));
        }

        return objectParser;
    }
}
