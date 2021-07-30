/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.breaker;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

/**
 * Settings for a {@link CircuitBreaker}
 */
public final class BreakerSettings {

    private static final String BREAKER_SETTING_PREFIX = "breaker.";
    private static final String BREAKER_LIMIT_SUFFIX = "limit";
    private static final String BREAKER_OVERHEAD_SUFFIX = "overhead";
    private static final String BREAKER_TYPE_SUFFIX = "type";

    public static final Setting.AffixSetting<ByteSizeValue> CIRCUIT_BREAKER_LIMIT_SETTING =
        Setting.affixKeySetting(BREAKER_SETTING_PREFIX,
            BREAKER_LIMIT_SUFFIX,
            name -> Setting.memorySizeSetting(name, "100%", Setting.Property.Dynamic, Setting.Property.NodeScope));
    static String breakerLimitSettingKey(String breakerName) {
        return BREAKER_SETTING_PREFIX + breakerName + "." + BREAKER_LIMIT_SUFFIX;
    }

    public static final Setting.AffixSetting<Double> CIRCUIT_BREAKER_OVERHEAD_SETTING =
        Setting.affixKeySetting(BREAKER_SETTING_PREFIX,
            BREAKER_OVERHEAD_SUFFIX,
            name -> Setting.doubleSetting(name, 2.0d, 0.0d, Setting.Property.Dynamic, Setting.Property.NodeScope));
    static String breakerOverheadSettingKey(String breakerName) {
        return BREAKER_SETTING_PREFIX + breakerName + "." + BREAKER_OVERHEAD_SUFFIX;
    }

    public static final Setting.AffixSetting<CircuitBreaker.Type> CIRCUIT_BREAKER_TYPE =
        Setting.affixKeySetting(BREAKER_SETTING_PREFIX,
            BREAKER_TYPE_SUFFIX,
            name -> new Setting<>(name,
                "noop",
                CircuitBreaker.Type::parseValue,
                (type) -> {
                    if (CircuitBreaker.Type.PARENT.equals(type)) {
                        throw new IllegalArgumentException(
                            "Invalid circuit breaker type [parent]. Only [memory] or [noop] are configurable"
                        );
                    }
                },
                Setting.Property.NodeScope));
    static String breakerTypeSettingKey(String breakerName) {
        return BREAKER_SETTING_PREFIX + breakerName + "." + BREAKER_TYPE_SUFFIX;
    }

    private final String name;
    private final long limitBytes;
    private final double overhead;
    private final CircuitBreaker.Type type;
    private final CircuitBreaker.Durability durability;

    public static BreakerSettings updateFromSettings(BreakerSettings defaultSettings, Settings currentSettings) {
        final String breakerName = defaultSettings.name;
        return new BreakerSettings(breakerName,
            getOrDefault(CIRCUIT_BREAKER_LIMIT_SETTING.getConcreteSetting(breakerLimitSettingKey(breakerName)),
                new ByteSizeValue(defaultSettings.limitBytes),
                currentSettings).getBytes(),
            getOrDefault(CIRCUIT_BREAKER_OVERHEAD_SETTING.getConcreteSetting(breakerOverheadSettingKey(breakerName)),
                defaultSettings.overhead,
                currentSettings),
            getOrDefault(CIRCUIT_BREAKER_TYPE.getConcreteSetting(breakerTypeSettingKey(breakerName)),
                defaultSettings.type,
                currentSettings),
            defaultSettings.durability);
    }

    private static <T> T getOrDefault(Setting<T> concreteSetting, T defaultValue, Settings settings) {
        return concreteSetting.exists(settings) ? concreteSetting.get(settings) : defaultValue;
    }

    public BreakerSettings(String name, long limitBytes, double overhead) {
        this(name, limitBytes, overhead, CircuitBreaker.Type.MEMORY, CircuitBreaker.Durability.PERMANENT);
    }

    public BreakerSettings(String name, long limitBytes, double overhead, CircuitBreaker.Type type, CircuitBreaker.Durability durability) {
        this.name = name;
        this.limitBytes = limitBytes;
        this.overhead = overhead;
        this.type = type;
        this.durability = durability;
    }

    public String getName() {
        return this.name;
    }

    public long getLimit() {
        return this.limitBytes;
    }

    public double getOverhead() {
        return this.overhead;
    }

    public CircuitBreaker.Type getType() {
        return this.type;
    }

    public CircuitBreaker.Durability getDurability() {
        return durability;
    }

    @Override
    public String toString() {
        return "[" + this.name +
                ",type=" + this.type.toString() +
                ",durability=" + (this.durability == null ? "null" : this.durability.toString()) +
                ",limit=" + this.limitBytes + "/" + new ByteSizeValue(this.limitBytes) +
                ",overhead=" + this.overhead + "]";
    }
}
