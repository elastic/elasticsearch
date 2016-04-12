/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Optional;
import java.util.function.Function;

public class OptionalSettings {

    private OptionalSettings() {}

    public static Setting<Optional<Integer>> createInt(String key, Property... properties) {
        return new Setting<>(key, s -> null, s -> {
            if (s != null) {
                return Optional.of(Integer.parseInt(s));
            } else {
                return Optional.<Integer>ofNullable(null);
            }
        }, properties);
    }

    public static Setting<Optional<String>> createString(String key, Property... properties) {
        return createString(key, s -> null, properties);
    }

    public static Setting<Optional<String>> createString(String key, Function<Settings, String> defaultValue, Property... properties) {
        return new Setting<>(key, defaultValue, Optional::ofNullable, properties);
    }

    public static Setting<Optional<String>> createString(String key, Setting<Optional<String>> fallback, Property... properties) {
        return new Setting<>(key, fallback, Optional::ofNullable, properties);
    }

    public static Setting<Optional<TimeValue>> createTimeValue(String key, Property... properties) {
        return new Setting<>(key, s-> null, s -> {
            if (s != null) {
                return Optional.of(TimeValue.parseTimeValue(s, key));
            } else {
                return Optional.<TimeValue>ofNullable(null);
            }
        }, properties);
    }
}
