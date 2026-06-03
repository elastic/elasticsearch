/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * BoostConfiguration is essentially a set of BoostWindow instances that collectively define the complete boost
 * configuration for a certain type of data, e.g. "logs".
 */
public class BoostConfiguration {

    // Identifier for the boost configuration, e.g. "logs".
    private final String name;
    private final Map<String, BoostWindow> boostWindows;
    private final NavigableMap<Long, BoostWindow> boostWindowLookup;

    public BoostConfiguration(String name, Map<String, BoostWindow> boostWindows) {
        this.name = name;
        this.boostWindows = Collections.unmodifiableMap(boostWindows);

        // We build a map keyed by the maxAge for faster lookup. It also deduplicates if multiple windows have the same maxAge.
        final TreeMap<Long, BoostWindow> lookup = new TreeMap<>();
        for (BoostWindow window : boostWindows.values()) {
            long millis = window.effectiveMaxAge().getMillis();
            BoostWindow existing = lookup.get(millis);
            // TODO: the cachedRatio may vary in a single window if we support both cachePowerMin and cachePowerMax
            // One option is to still order them based on cachePowerMin
            if (existing == null || window.cachedRatio() > existing.cachedRatio()) {
                lookup.put(millis, window);
            }
        }
        this.boostWindowLookup = Collections.unmodifiableNavigableMap(lookup);
    }

    public String getName() {
        return name;
    }

    /**
     *
     * @param timeInMillisRelativeToNow The time of "now() - timestamp". Positive if in the past, negative if in the future.
     * @return The BoostWindow that should be applied to the timestamp.
     */
    public BoostWindow getBoostWindow(long timeInMillisRelativeToNow) {
        if (boostWindowLookup.isEmpty()) {
            return null;
        }
        // Find the smallest effectiveMaxAge that covers timeInMillisRelativeToNow (ceiling).
        // ceilingEntry naturally clamps to the first entry when input is below the smallest key.
        // When input exceeds the largest key, clamp to the last entry.
        var entry = boostWindowLookup.ceilingEntry(timeInMillisRelativeToNow);
        return entry != null ? entry.getValue() : boostWindowLookup.lastEntry().getValue();
    }

    /**
     * Create a new BoostConfiguration by applying the overrides to the existing one. The passed-in
     * overrides are meant to be finer grained, e.g. index level overrides, while the existing one is coarser grained,
     * e.g. project wide or a single configuration wide (e.g. `logs`).
     */
    public BoostConfiguration withOverrides(Map<String, BoostWindow.Overrides> overridesPerWindow) {
        if (overridesPerWindow == null) {
            return this;
        }

        return new BoostConfiguration(
            name,
            Maps.transformValues(boostWindows, boostWindow -> boostWindow.withOverrides(overridesPerWindow.get(boostWindow.name())))
        );
    }

    @Override
    public String toString() {
        return "BoostConfiguration{"
            + "name='"
            + name
            + '\''
            + ", boostWindows="
            + boostWindows
            + ", boostWindowLookup="
            + boostWindowLookup
            + '}';
    }

    private static final Setting<TimeValue> MAX_AGE_SETTING = Setting.timeSetting("max_age", TimeValue.ZERO, TimeValue.MINUS_ONE);
    private static final Setting<Integer> CACHE_POWER_MIN_SETTING = Setting.intSetting("cache_power_min", 0, 0);
    private static final Setting<TimeValue> OVERRIDE_MAX_AGE_SETTING = Setting.timeSetting(
        "overrides.max_age",
        TimeValue.ZERO,
        TimeValue.ZERO
    );
    private static final Setting<Double> BOOST_FACTOR_MIN_SETTING = Setting.doubleSetting("overrides.boost_factor_min", 1.0, 0.0);

    private static Set<String> ALLOWED_WINDOW_SETTING_KEYS = Set.of("max_age", "cache_power_min", "overrides");
    private static Set<String> ALLOWED_OVERRIDES_SETTING_KEYS = Set.of("max_age", "boost_factor_min");

    public static BoostConfiguration fromSettings(String name, Settings settings) {
        final Map<String, BoostWindow> boostWindows = settings.getAsGroups()
            .entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> {
                final String windowName = entry.getKey();
                final var windowSettings = entry.getValue();

                if (ALLOWED_WINDOW_SETTING_KEYS.containsAll(getFirstComponentOfSettingKeys(windowSettings)) == false) {
                    throw new IllegalArgumentException("Invalid settings for boost window [" + windowName + "]: " + windowSettings);
                }

                final TimeValue maxAge = MAX_AGE_SETTING.get(windowSettings);
                final int cachePowerMin = CACHE_POWER_MIN_SETTING.get(windowSettings);

                BoostWindow.Overrides overrides = null;
                final Settings overridesSettings = windowSettings.getByPrefix("overrides.");
                if (overridesSettings.isEmpty() == false) {
                    if (ALLOWED_OVERRIDES_SETTING_KEYS.containsAll(getFirstComponentOfSettingKeys(overridesSettings)) == false) {
                        throw new IllegalArgumentException("Invalid settings for boost window [" + windowName + "]: " + overridesSettings);
                    }
                    overrides = new BoostWindow.Overrides(
                        OVERRIDE_MAX_AGE_SETTING.exists(windowSettings) ? OVERRIDE_MAX_AGE_SETTING.get(windowSettings) : null,
                        BOOST_FACTOR_MIN_SETTING.get(windowSettings)
                    );
                }
                return new BoostWindow(windowName, maxAge, cachePowerMin, overrides);
            }));
        return new BoostConfiguration(name, boostWindows);
    }

    private static Set<String> getFirstComponentOfSettingKeys(Settings settings) {
        return settings.keySet().stream().map(key -> {
            final var idx = key.indexOf('.');
            return idx < 0 ? key : key.substring(0, idx);
        }).collect(Collectors.toUnmodifiableSet());
    }
}
