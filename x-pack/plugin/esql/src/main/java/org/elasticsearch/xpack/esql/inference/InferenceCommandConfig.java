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

public record InferenceCommandConfig(boolean enabled, int rowLimit) {

    public static final Setting<Boolean> COMPLETION_ENABLED_SETTING = commandEnabledSetting("completion");
    public static final Setting<Integer> COMPLETION_ROW_LIMIT_SETTING = rowLimitSetting("completion", Completion.DEFAULT_MAX_ROW_LIMIT);
    public static final Setting<Boolean> RERANK_ENABLED_SETTING = commandEnabledSetting("rerank");
    public static final Setting<Integer> RERANK_ROW_LIMIT_SETTING = rowLimitSetting("rerank", Rerank.DEFAULT_MAX_ROW_LIMIT);

    public static InferenceCommandConfig completionCommandConfig(Settings settings) {
        return new InferenceCommandConfig(COMPLETION_ENABLED_SETTING.get(settings), COMPLETION_ROW_LIMIT_SETTING.get(settings));
    }

    public static InferenceCommandConfig rerankCommandConfig(Settings settings) {
        return new InferenceCommandConfig(RERANK_ENABLED_SETTING.get(settings), RERANK_ROW_LIMIT_SETTING.get(settings));
    }

    public static Map<String, InferenceCommandConfig> fromSettings(Settings settings) {
        return Map.of("completion", completionCommandConfig(settings), "rerank", rerankCommandConfig(settings));
    }

    private static final String ENABLED_SETTING_PATTERN = "inference.command.%s.enabled";
    private static final String ROW_LIMIT_PATTERN = "inference.command.%s.row_limit";

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
