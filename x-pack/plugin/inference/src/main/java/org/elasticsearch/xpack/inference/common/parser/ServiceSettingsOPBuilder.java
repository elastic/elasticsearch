/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.SettingsScope;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;

/**
 * A builder for constructing an {@link ObjectParser} for parsing requests and persisted configuration for service settings.
 */
public class ServiceSettingsOPBuilder<Value> extends AbstractSettingsOPBuilder<Value, ServiceSettingsOPBuilder<Value>> {

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
    private RateLimitSettings defaultRateLimitSettings;
    private BiConsumer<Value, RateLimitSettings> rateLimitSettingsSetter;

    public ServiceSettingsOPBuilder(boolean ignoreUnknownFields, Supplier<Value> valueSupplier) {
        this(SERVICE_SETTINGS, ignoreUnknownFields, valueSupplier);
    }

    public ServiceSettingsOPBuilder(SettingsScope scope, boolean ignoreUnknownFields, Supplier<Value> valueSupplier) {
        super(scope, valueSupplier);
        this.ignoreUnknownFields = ignoreUnknownFields;
    }

    public ServiceSettingsOPBuilder<Value> enableRateLimitSettings(
        BiConsumer<Value, RateLimitSettings> setter,
        RateLimitSettings defaultRateLimitSettings
    ) {
        this.rateLimitSettingsSetter = Objects.requireNonNull(setter);
        this.defaultRateLimitSettings = defaultRateLimitSettings;
        return this;
    }

    public ObjectParser<Value, ConfigurationParseContext> build() {
        var objectParser = new ObjectParser<Value, ConfigurationParseContext>(scope.toString(), ignoreUnknownFields, valueSupplier);

        if (rateLimitSettingsSetter != null) {
            RateLimitSettings.declareRateLimitSettings(objectParser, rateLimitSettingsSetter, defaultRateLimitSettings);
        }

        declareSecretFields(objectParser);

        return objectParser;
    }
}
