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
 * Describes a datasource configuration setting — its name and sensitivity.
 *
 * @param name the setting's API name (e.g. "access_key", "region")
 * @param sensitivity whether this setting is secret or plaintext
 */
public record ConfigSetting(String name, ConfigSettingSensitivity sensitivity) {

    public boolean isSecret() {
        return sensitivity == ConfigSettingSensitivity.SECRET;
    }

    /** A setting that holds a credential. */
    public static ConfigSetting secret(String name) {
        return new ConfigSetting(name, ConfigSettingSensitivity.SECRET);
    }

    /** A regular (non-secret) setting. */
    public static ConfigSetting plaintext(String name) {
        return new ConfigSetting(name, ConfigSettingSensitivity.PLAINTEXT);
    }

    /** Builds a definition map keyed by setting name. Each name is typed once. */
    public static Map<String, ConfigSetting> mapOf(ConfigSetting... settings) {
        return Arrays.stream(settings).collect(Collectors.toUnmodifiableMap(ConfigSetting::name, s -> s));
    }
}
