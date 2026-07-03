/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Metadata describing a datasource configuration field — its name, sensitivity, and
 * value semantics. Used by {@link DataSourceConfiguration} subclasses to declare their
 * settings schema and drive validation. This is internal plugin machinery — it is never
 * exposed through the CRUD API or stored in cluster state. The validated output that
 * enters cluster state is {@link DataSourceSetting}.
 *
 * @param name the field's API name (e.g. "access_key", "region")
 * @param secret whether this field holds a credential or other sensitive value
 * @param caseInsensitive whether this field's values are case-insensitive (e.g. enum-like
 *                        fields like "auth"). Case-insensitive fields are normalized to
 *                        lowercase on input for consistent storage and comparison.
 * @param federatedAuth whether this field contributes to federated authentication (e.g. workload
 *                    identity federation settings that replace explicit credentials). A field
 *                    cannot be both {@code secret} and {@code federatedAuth}: the two represent
 *                    mutually exclusive authentication kinds, and combining them on a single
 *                    field would make it self-conflict during validation.
 */
public record DataSourceConfigDefinition(String name, boolean secret, boolean caseInsensitive, boolean federatedAuth) {

    public DataSourceConfigDefinition {
        if (secret && federatedAuth) {
            throw new IllegalArgumentException("field [" + name + "] cannot be both secret and federated auth");
        }
    }

    /** A field that holds a credential. */
    public static DataSourceConfigDefinition secret(String name) {
        return new DataSourceConfigDefinition(name, true, false, false);
    }

    /** A regular (non-secret) field. */
    public static DataSourceConfigDefinition plaintext(String name) {
        return new DataSourceConfigDefinition(name, false, false, false);
    }

    /** Returns a copy whose values are treated as case-insensitive (normalized to lowercase on input). */
    public DataSourceConfigDefinition asCaseInsensitive() {
        return new DataSourceConfigDefinition(name, secret, true, federatedAuth);
    }

    /** Returns a copy that marks this field as contributing to federated authentication. */
    public DataSourceConfigDefinition asFederatedAuth() {
        return new DataSourceConfigDefinition(name, secret, caseInsensitive, true);
    }

    /** Builds a definition map keyed by field name. Each name is typed once. */
    public static Map<String, DataSourceConfigDefinition> mapOf(DataSourceConfigDefinition... definitions) {
        return Arrays.stream(definitions).collect(Collectors.toUnmodifiableMap(DataSourceConfigDefinition::name, d -> d));
    }
}
