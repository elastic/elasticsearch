/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Describes a datasource configuration setting. Used both as a definition (no value)
 * and as a validated result (with value).
 *
 * @param name the setting's API name
 * @param isSecret whether this setting contains a credential
 * @param value the setting value, or null for definitions
 */
public record ConfigSetting(String name, boolean isSecret, String value) {

    /** Definition-time constructor — no value yet. */
    public ConfigSetting(String name, boolean isSecret) {
        this(name, isSecret, null);
    }

    /** Returns a copy with the given value. */
    public ConfigSetting withValue(String value) {
        return new ConfigSetting(name, isSecret, value);
    }

    /** Builds a definition map keyed by setting name. Each name is typed once. */
    public static Map<String, ConfigSetting> mapOf(ConfigSetting... settings) {
        return Arrays.stream(settings).collect(Collectors.toUnmodifiableMap(ConfigSetting::name, s -> s));
    }
}
