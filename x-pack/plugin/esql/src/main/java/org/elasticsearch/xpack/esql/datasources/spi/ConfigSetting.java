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
 * Describes a datasource configuration setting — its name and whether it holds a credential.
 *
 * @param name the setting's API name (e.g. "access_key", "region")
 * @param isSecret whether this setting contains a credential that should be masked/encrypted
 */
public record ConfigSetting(String name, boolean isSecret) {

    /** Builds a definition map keyed by setting name. Each name is typed once. */
    public static Map<String, ConfigSetting> mapOf(ConfigSetting... settings) {
        return Arrays.stream(settings).collect(Collectors.toUnmodifiableMap(ConfigSetting::name, s -> s));
    }
}
