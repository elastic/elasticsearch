/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
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
 * A builder for constructing an {@link ObjectParser} for parsing requests and persisted configuration for service settings.
 */
public class ServiceSettingsOPBuilder<Value> {

    /**
     * Constructs a {@link ServiceSettingsOPBuilder} that requires the rate limit settings and allows the
     * {@link DefaultSecretSettings#API_KEY} field to be parsed but ignored.
     * @param ignoreUnknownFields whether to ignore unknown fields
     * @param valueSupplier the supplier for the value
     * @param defaultRateLimitSettings the default rate limit settings
     * @param rateLimitSettingsSetter the setter for the rate limit settings
     * @return a new {@link ServiceSettingsOPBuilder} instance
     */
    public static <V> ServiceSettingsOPBuilder<V> of(
        boolean ignoreUnknownFields,
        Supplier<V> valueSupplier,
        RateLimitSettings defaultRateLimitSettings,
        BiConsumer<V, RateLimitSettings> rateLimitSettingsSetter
    ) {
        return new ServiceSettingsOPBuilder<>(ignoreUnknownFields, valueSupplier).enableRateLimitSettings(
            rateLimitSettingsSetter,
            defaultRateLimitSettings
        ).allowApiKey();
    }

    private final boolean ignoreUnknownFields;
    private final Supplier<Value> valueSupplier;
    private RateLimitSettings defaultRateLimitSettings;
    private BiConsumer<Value, RateLimitSettings> rateLimitSettingsSetter;
    private final Set<String> secretFields = new LinkedHashSet<>();

    public ServiceSettingsOPBuilder(boolean ignoreUnknownFields, Supplier<Value> valueSupplier) {
        this.ignoreUnknownFields = ignoreUnknownFields;
        this.valueSupplier = Objects.requireNonNull(valueSupplier);
    }

    public ServiceSettingsOPBuilder<Value> allowApiKey() {
        return allowSecretFields(DefaultSecretSettings.API_KEY);
    }

    /**
     * Declares the given field names as no-ops in the parser. Secret fields (e.g. {@code api_key}, {@code access_key}) appear in the
     * same JSON block as service settings in requests. The service's secret settings extract them separately; these declarations prevent
     * the strict parser from rejecting them as unknown fields.
     * <p>
     * Duplicate field names — whether from multiple calls or within a single varargs list — are ignored. It is therefore safe to call
     * this method with a field that was already declared by a previous call or by {@link #allowApiKey()}.
     */
    public ServiceSettingsOPBuilder<Value> allowSecretFields(String... fieldNames) {
        secretFields.addAll(List.of(fieldNames));
        return this;
    }

    public ServiceSettingsOPBuilder<Value> enableRateLimitSettings(
        BiConsumer<Value, RateLimitSettings> setter,
        RateLimitSettings defaultRateLimitSettings
    ) {
        this.rateLimitSettingsSetter = setter;
        this.defaultRateLimitSettings = defaultRateLimitSettings;
        return this;
    }

    public ObjectParser<Value, ConfigurationParseContext> build() {
        var objectParser = new ObjectParser<Value, ConfigurationParseContext>(
            SERVICE_SETTINGS.toString(),
            ignoreUnknownFields,
            valueSupplier
        );

        if (defaultRateLimitSettings != null) {
            RateLimitSettings.declareRateLimitSettings(objectParser, rateLimitSettingsSetter, defaultRateLimitSettings);
        }

        for (var field : secretFields) {
            objectParser.declareString((b, v) -> {}, new ParseField(field));
        }

        return objectParser;
    }
}
