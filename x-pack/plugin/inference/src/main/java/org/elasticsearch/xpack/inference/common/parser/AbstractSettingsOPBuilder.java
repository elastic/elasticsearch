/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.inference.services.SettingsScope;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Shared base for {@link ServiceSettingsOPBuilder} and {@link UpdateServiceSettingsOPBuilder}. Holds the secret-field no-op
 * declarations that prevent strict parsers from rejecting credential fields that appear in the same JSON block as service settings.
 *
 * @param <Value> the type that the built {@link ObjectParser} produces
 * @param <Self>  the concrete builder subtype (for fluent chaining)
 */
public abstract class AbstractSettingsOPBuilder<Value, Self extends AbstractSettingsOPBuilder<Value, Self>> {

    protected final SettingsScope scope;
    protected final Supplier<Value> valueSupplier;
    private final Set<String> secretFields = new LinkedHashSet<>();

    protected AbstractSettingsOPBuilder(SettingsScope scope, Supplier<Value> valueSupplier) {
        this.scope = Objects.requireNonNull(scope);
        this.valueSupplier = Objects.requireNonNull(valueSupplier);
    }

    /**
     * Declares the {@link DefaultSecretSettings#API_KEY} field as a no-op on the built parser.
     */
    public final Self allowApiKey() {
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
    @SuppressWarnings("unchecked")
    public final Self allowSecretFields(String... fieldNames) {
        secretFields.addAll(List.of(fieldNames));
        return (Self) this;
    }

    /**
     * Registers the accumulated secret-field no-ops onto {@code parser}.
     */
    protected final void declareSecretFields(ObjectParser<Value, ?> parser) {
        for (var field : secretFields) {
            parser.declareString((b, v) -> {}, new ParseField(field));
        }
    }
}
