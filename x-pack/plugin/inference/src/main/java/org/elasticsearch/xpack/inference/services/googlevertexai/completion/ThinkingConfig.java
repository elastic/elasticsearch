/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.completion;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ServiceUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;

/**
 * This class encapsulates the ThinkingConfig object contained within GenerationConfig. Only the thinkingBudget field is currently
 * supported, but the includeThoughts field may be added in the future
 *
 * @see <a href="https://ai.google.dev/gemini-api/docs/thinking"> Gemini Thinking documentation</a>
 */
public class ThinkingConfig implements Writeable, ToXContentFragment {
    public static final String THINKING_CONFIG_FIELD = "thinking_config";
    public static final String THINKING_BUDGET_FIELD = "thinking_budget";

    private final Integer thinkingBudget;

    /**
     * Constructor for an empty {@code ThinkingConfig}
     */
    public ThinkingConfig() {
        this.thinkingBudget = null;
    }

    public ThinkingConfig(Integer thinkingBudget) {
        this.thinkingBudget = thinkingBudget;
    }

    public ThinkingConfig(StreamInput in) throws IOException {
        thinkingBudget = in.readOptionalVInt();
    }

    public static ThinkingConfig fromMap(Map<String, Object> map, ValidationException validationException) {
        Map<String, Object> thinkingConfigSettings = removeFromMapOrDefaultEmpty(map, THINKING_CONFIG_FIELD);
        Integer thinkingBudget = ServiceUtils.extractOptionalInteger(
            thinkingConfigSettings,
            THINKING_BUDGET_FIELD,
            ModelConfigurations.TASK_SETTINGS,
            validationException
        );

        return new ThinkingConfig(thinkingBudget);
    }

    public boolean isEmpty() {
        return thinkingBudget == null;
    }

    public Integer getThinkingBudget() {
        return thinkingBudget;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(thinkingBudget);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (thinkingBudget != null) {
            builder.startObject(THINKING_CONFIG_FIELD);
            builder.field(THINKING_BUDGET_FIELD, thinkingBudget);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ThinkingConfig that = (ThinkingConfig) o;
        return Objects.equals(thinkingBudget, that.thinkingBudget);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(thinkingBudget);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
