/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

import java.util.Optional;
import java.util.function.Function;

public class OptionalStringSetting {

    private OptionalStringSetting() {}

    public static Setting<Optional<String>> create(String key, Property... properties) {
        return create(key, s -> null, properties);
    }

    public static Setting<Optional<String>> create(String key, Function<Settings, String> defaultValue, Property... properties) {
        return new Setting<>(key, defaultValue, Optional::ofNullable, properties);
    }

    public static Setting<Optional<String>> create(String key, Setting<Optional<String>> fallback, Property... properties) {
        return new Setting<>(key, fallback, Optional::ofNullable, properties);
    }
}
