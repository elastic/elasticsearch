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
    int denseVectorBatchSize,
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
     * Maximum number of input texts coalesced into a single embedding inference request. Sized as
     * {@link #DENSE_VECTOR_ROW_LIMIT_SETTING} / {@link InferenceOperator#DEFAULT_MAX_OUTSTANDING_REQUESTS} so that a
     * full page of rows keeps the operator's concurrency window saturated in a single wave.
     */
    public static final int DENSE_VECTOR_DEFAULT_BATCH_SIZE = 20;

    /**
     * Upper bound for {@link #DENSE_VECTOR_BATCH_SIZE_SETTING}. A batch is sent as a single embedding request, so an unbounded
     * value lets a single request carry enough inputs to overwhelm the inference endpoint. This cap keeps any one request within a
     * size the endpoint can handle while still leaving ample room above the default.
     */
    public static final int DENSE_VECTOR_MAX_BATCH_SIZE = 1000;

    /**
     * Maximum number of input texts coalesced into a single embedding inference request by the {@code DENSE_VECTOR} command.
     * A larger value sends fewer, larger requests; a smaller value sends more, smaller ones. Accepts values from {@code 1} to
     * {@link #DENSE_VECTOR_MAX_BATCH_SIZE}, defaults to {@link #DENSE_VECTOR_DEFAULT_BATCH_SIZE}, and is dynamic so it can be tuned
     * on a running cluster.
     */
    public static final Setting<Integer> DENSE_VECTOR_BATCH_SIZE_SETTING = Setting.intSetting(
        "esql.command.dense_vector.batch_size",
        DENSE_VECTOR_DEFAULT_BATCH_SIZE,
        1,
        DENSE_VECTOR_MAX_BATCH_SIZE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * The default inference endpoint id used by the {@code DENSE_VECTOR} command when the query does not provide one via
     * {@code WITH { "inference_id": ... }}. An empty value means "not set": resolution falls through to the built-in default
     * ({@code DenseVector.DEFAULT_INFERENCE_ID}).
     * <p>
     * A blank but non-empty value is rejected here rather than at query time, where it would otherwise be taken for an endpoint
     * id and surface as a confusing "unknown inference endpoint" failure. Empty stays valid because it is the "not set" marker.
     */
    public static final Setting<String> DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING = Setting.simpleString(
        "esql.command.dense_vector.default_inference_id",
        "",
        value -> {
            if (value.isEmpty() == false && value.isBlank()) {
                throw new IllegalArgumentException("[esql.command.dense_vector.default_inference_id] must not be blank");
            }
        },
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
            DENSE_VECTOR_BATCH_SIZE_SETTING,
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
            DENSE_VECTOR_BATCH_SIZE_SETTING.get(settings),
            DENSE_VECTOR_DEFAULT_INFERENCE_ID_SETTING.get(settings)
        );
    }
}
