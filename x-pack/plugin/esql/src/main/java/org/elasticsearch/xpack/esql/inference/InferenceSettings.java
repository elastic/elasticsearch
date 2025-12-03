/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.plan.logical.inference.Completion;
import org.elasticsearch.xpack.esql.plan.logical.inference.Rerank;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class InferenceSettings {

    public static final Setting<Boolean> COMPLETION_ENABLED_SETTING = CommandSettings.commandEnabledSetting("completion");
    public static final Setting<Integer> COMPLETION_ROW_LIMIT_SETTING = CommandSettings.rowLimitSetting(
        "completion",
        Completion.DEFAULT_MAX_ROW_LIMIT
    );

    public static final Setting<Boolean> RERANK_ENABLED_SETTING = CommandSettings.commandEnabledSetting("rerank");
    public static final Setting<Integer> RERANK_ROW_LIMIT_SETTING = CommandSettings.rowLimitSetting("rerank", Rerank.DEFAULT_MAX_ROW_LIMIT);

    public static CommandSettings completionCommandConfig(Settings settings) {
        return new CommandSettings(COMPLETION_ENABLED_SETTING.get(settings), COMPLETION_ROW_LIMIT_SETTING.get(settings));
    }

    public static CommandSettings rerankCommandConfig(Settings settings) {
        return new CommandSettings(RERANK_ENABLED_SETTING.get(settings), RERANK_ROW_LIMIT_SETTING.get(settings));
    }

    private final Map<String, CommandSettings> commandSettings;

    public static InferenceSettings fromSettings(Settings settings) {
        return new InferenceSettings(CommandSettings.fromSettings(settings));
    }

    private InferenceSettings(Map<String, CommandSettings> commandSettings) {
        this.commandSettings = commandSettings;
    }

    public CommandSettings commandSettings(String commandName) {
        return commandSettings.get(commandName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceSettings that = (InferenceSettings) o;
        return Objects.equals(commandSettings, that.commandSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commandSettings);
    }

    public record CommandSettings(boolean enabled, int rowLimit) {

        private static final String ENABLED_SETTING_PATTERN = "inference.command.%s.enabled";
        private static final String ROW_LIMIT_PATTERN = "inference.command.%s.row_limit";

        public static Map<String, CommandSettings> fromSettings(Settings settings) {
            return Map.ofEntries(
                Map.entry("completion", completionCommandConfig(settings)),
                Map.entry("rerank", rerankCommandConfig(settings))
            );
        }

        private static Setting<Boolean> commandEnabledSetting(String commandName) {
            return Setting.boolSetting(
                String.format(Locale.ROOT, ENABLED_SETTING_PATTERN, commandName),
                true,
                Setting.Property.NodeScope,
                Setting.Property.Dynamic
            );
        }

        private static Setting<Integer> rowLimitSetting(String commandName, int defaultValue) {
            return Setting.intSetting(
                String.format(Locale.ROOT, ROW_LIMIT_PATTERN, commandName),
                defaultValue,
                -1,
                Setting.Property.NodeScope,
                Setting.Property.Dynamic
            );
        }
    }
}
