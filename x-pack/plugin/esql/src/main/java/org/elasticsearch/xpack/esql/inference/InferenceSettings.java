/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 * Settings for inference features such as completion and rerank.
 */
public record InferenceSettings(boolean completionEnabled, int completionRowLimit, boolean rerankEnabled, int rerankRowLimit) {

    public static final Setting<Boolean> COMPLETION_ENABLED_SETTING = Setting.boolSetting(
        "esql.command.completion.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> COMPLETION_ROW_LIMIT_SETTING = Setting.intSetting(
        "esql.command.completion.limit",
        100,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Boolean> RERANK_ENABLED_SETTING = Setting.boolSetting(
        "esql.command.rerank.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> RERANK_ROW_LIMIT_SETTING = Setting.intSetting(
        "esql.command.rerank.limit",
        1000,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> getSettings() {
        return List.of(COMPLETION_ENABLED_SETTING, COMPLETION_ROW_LIMIT_SETTING, RERANK_ENABLED_SETTING, RERANK_ROW_LIMIT_SETTING);
    }

    public InferenceSettings(Settings settings) {
        this(
            COMPLETION_ENABLED_SETTING.get(settings),
            COMPLETION_ROW_LIMIT_SETTING.get(settings),
            RERANK_ENABLED_SETTING.get(settings),
            RERANK_ROW_LIMIT_SETTING.get(settings)
        );
    }
}
