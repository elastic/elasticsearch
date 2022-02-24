/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.Level;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class LogSettings {

    private LogSettings() {}

    public static final Setting<Level> LOG_DEFAULT_LEVEL_SETTING = new Setting<>(
        "logger.level",
        Level.INFO.name(),
        Level::valueOf,
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<Level> LOG_LEVEL_SETTING = Setting.prefixKeySetting(
        "logger.",
        (key) -> new Setting<>(key, Level.INFO.name(), Level::valueOf, Setting.Property.Dynamic, Setting.Property.NodeScope)
    );

    public static Optional<Level> defaultLogLevel(Settings settings) {
        if (LogSettings.LOG_DEFAULT_LEVEL_SETTING.exists(settings)) {
            return Optional.of(LogSettings.LOG_DEFAULT_LEVEL_SETTING.get(settings));
        }
        return Optional.empty();
    }

    public static Map<String, Level> logLevelSettingsMap(Settings settings) {
        // do not set a log level for a logger named level (from the default log setting)
        return LogSettings.LOG_LEVEL_SETTING.getAllConcreteSettings(settings)
            .filter(s -> s.getKey().equals(LogSettings.LOG_DEFAULT_LEVEL_SETTING.getKey()) == false)
            .collect(Collectors.toUnmodifiableMap(s -> s.getKey().substring("logger.".length()), s -> s.get(settings)));
    }
}
