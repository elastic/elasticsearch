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
 * Settings for inference features such as completion, rerank and dense_vector.
 */
public record InferenceSettings(
    boolean completionEnabled,
    int completionRowLimit,
    boolean rerankEnabled,
    int rerankRowLimit,
    boolean denseVectorEnabled,
    int denseVectorRowLimit,
    String denseVectorDefaultInferenceId
) {

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

    public static final Setting<Boolean> DENSE_VECTOR_ENABLED_SETTING = Setting.boolSetting(
        "esql.command.dense_vector.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> DENSE_VECTOR_ROW_LIMIT_SETTING = Setting.intSetting(
        "esql.command.dense_vector.limit",
        1000,
        1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The default inference endpoint id used by the {@code DENSE_VECTOR} command when the query does not provide one via
     * {@code WITH { "inference_id": ... }}. An empty value means "not set": resolution falls through to the built-in default
     * ({@code DenseVector.DEFAULT_INFERENCE_ID}).
     */
    public static final Setting<String> DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING = Setting.simpleString(
        "esql.command.dense_vector.default_inference_id",
        "",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static List<Setting<?>> getSettings() {
        return List.of(
            COMPLETION_ENABLED_SETTING,
            COMPLETION_ROW_LIMIT_SETTING,
            RERANK_ENABLED_SETTING,
            RERANK_ROW_LIMIT_SETTING,
            DENSE_VECTOR_ENABLED_SETTING,
            DENSE_VECTOR_ROW_LIMIT_SETTING,
            DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING
        );
    }

    public InferenceSettings(Settings settings) {
        this(
            COMPLETION_ENABLED_SETTING.get(settings),
            COMPLETION_ROW_LIMIT_SETTING.get(settings),
            RERANK_ENABLED_SETTING.get(settings),
            RERANK_ROW_LIMIT_SETTING.get(settings),
            DENSE_VECTOR_ENABLED_SETTING.get(settings),
            DENSE_VECTOR_ROW_LIMIT_SETTING.get(settings),
            DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING.get(settings)
        );
    }
}
