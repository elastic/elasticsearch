/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.completion;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiUtils;

import java.io.IOException;
import java.util.Map;

/**
 * The OCI Generative AI {@code completion} and {@code chat_completion} tasks do not support any task settings today. This class is
 * kept as an empty implementation with its own {@link #NAME} so that task settings can be added later without a backwards
 * compatibility migration.
 */
public class OciGenAiChatCompletionTaskSettings implements TaskSettings {

    public static final String NAME = "oci_genai_chat_completion_task_settings";
    public static final OciGenAiChatCompletionTaskSettings EMPTY_SETTINGS = new OciGenAiChatCompletionTaskSettings();

    public static OciGenAiChatCompletionTaskSettings fromMap(@Nullable Map<String, Object> map) {
        return EMPTY_SETTINGS;
    }

    public OciGenAiChatCompletionTaskSettings() {}

    public OciGenAiChatCompletionTaskSettings(StreamInput in) throws IOException {
        // no fields to read
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return OciGenAiUtils.INFERENCE_OCI_GENAI_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // no fields to write
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        return this == o || (o != null && getClass() == o.getClass());
    }

    @Override
    public int hashCode() {
        return NAME.hashCode();
    }
}
