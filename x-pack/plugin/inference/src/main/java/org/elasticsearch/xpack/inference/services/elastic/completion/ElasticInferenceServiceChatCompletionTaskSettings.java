/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.services.elastic.compatibility.CompletionCompatibilityService.REASONING_FIELD_UNSUPPORTED_MESSAGE;

/**
 * Task settings for the Elastic Inference Service {@code chat_completion} task type, holding
 * an optional {@code reasoning} block.
 * <p>
 * At persistence time ({@link #updatedTaskSettings}), updates follow tri-state semantics:
 * <ul>
 *   <li>field omitted → keep the stored value;</li>
 *   <li>explicit {@code null} → clear to {@code null};</li>
 *   <li>field present with a value → replace the stored value.</li>
 * </ul>
 * At request time ({@link #mergeReasoning}), body-wins precedence applies: reasoning supplied in
 * the request body overrides whatever is stored here.
 */
public class ElasticInferenceServiceChatCompletionTaskSettings implements TaskSettings {

    public static final String NAME = "elastic_chat_completion_task_settings";

    public static final TransportVersion EIS_REASONING_TASK_SETTINGS_ADDED = TransportVersion.fromName(
        "inference_api_eis_reasoning_task_settings"
    );

    public static final ElasticInferenceServiceChatCompletionTaskSettings EMPTY = new ElasticInferenceServiceChatCompletionTaskSettings(
        (Reasoning) null
    );

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        var parser = new ObjectParser<Builder, ConfigurationParseContext>(
            ModelConfigurations.TASK_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );
        parser.declareObject(Builder::setReasoning, (p, c) -> Reasoning.PARSER.apply(p, null), new ParseField(REASONING_FIELD));
        return parser;
    }

    /**
     * Parses task settings from a raw config map.
     * <p>
     * An empty or null map produces an empty instance ({@code reasoning == null}).
     *
     * @param map     the raw task settings map (may be null or empty)
     * @param taskType the task type for the endpoint being created
     * @param context  whether the settings come from a request or persisted storage
     * @throws IllegalArgumentException    if {@code reasoning} is present but {@code taskType} is not
     *                                   {@link TaskType#CHAT_COMPLETION}
     * @throws ElasticsearchParseException if XContent parsing fails
     */
    public static ElasticInferenceServiceChatCompletionTaskSettings fromMap(
        Map<String, Object> map,
        TaskType taskType,
        ConfigurationParseContext context
    ) {
        if (map == null || map.isEmpty()) {
            return EMPTY;
        }

        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build(taskType);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.TASK_SETTINGS);
        }
    }

    /**
     * Merges body and stored reasoning values; the body wins if present.
     *
     * @param body   reasoning from the current request body (may be null)
     * @param stored reasoning from stored task settings (may be null)
     * @return {@code body} if non-null, otherwise {@code stored}
     */
    public static Reasoning mergeReasoning(@Nullable Reasoning body, @Nullable Reasoning stored) {
        return body != null ? body : stored;
    }

    private final Reasoning reasoning;

    public ElasticInferenceServiceChatCompletionTaskSettings(@Nullable Reasoning reasoning) {
        this.reasoning = reasoning;
    }

    public ElasticInferenceServiceChatCompletionTaskSettings(StreamInput in) throws IOException {
        this.reasoning = in.readOptionalWriteable(Reasoning::new);
    }

    /**
     * @return the stored reasoning configuration, or {@code null} if none was set
     */
    public Reasoning reasoning() {
        return reasoning;
    }

    @Override
    public boolean isEmpty() {
        return reasoning == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (reasoning != null) {
            builder.field(REASONING_FIELD, reasoning);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Produces updated task settings following tri-state semantics for each field:
     * <ul>
     *   <li>field omitted from {@code newSettings} → keep the stored value;</li>
     *   <li>field present with an explicit {@code null} → clear to {@code null};</li>
     *   <li>field present with a value → replace the stored value.</li>
     * </ul>
     * A {@code null} or empty map is treated as "no fields set" and returns settings equal to this instance.
     */
    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        if (newSettings == null || newSettings.isEmpty()) {
            return this;
        }

        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, newSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}] update", e, ModelConfigurations.TASK_SETTINGS);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return EIS_REASONING_TASK_SETTINGS_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(EIS_REASONING_TASK_SETTINGS_ADDED) == false) {
            throw new ElasticsearchStatusException(REASONING_FIELD_UNSUPPORTED_MESSAGE, RestStatus.BAD_REQUEST);
        }

        out.writeOptionalWriteable(reasoning);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var other = (ElasticInferenceServiceChatCompletionTaskSettings) o;
        return Objects.equals(reasoning, other.reasoning);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(reasoning);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Parses a task-settings update following tri-state semantics for {@code reasoning}: an omitted
     * field keeps the stored value, an explicit {@code null} clears it, and a present value replaces it.
     * An empty {@code reasoning} object is rejected by {@link Reasoning#PARSER}'s required-field rule.
     */
    private static class Update {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.TASK_SETTINGS, Update::new);

        static {
            StatefulValue.declareNullable(
                PARSER,
                (update, value) -> update.reasoning = value,
                p -> Reasoning.PARSER.apply(p, null),
                new ParseField(REASONING_FIELD),
                ObjectParser.ValueType.OBJECT_OR_NULL
            );
        }

        private StatefulValue<Reasoning> reasoning = StatefulValue.undefined();

        private ElasticInferenceServiceChatCompletionTaskSettings mergeInto(ElasticInferenceServiceChatCompletionTaskSettings existing) {
            return new ElasticInferenceServiceChatCompletionTaskSettings(applyUpdate(this.reasoning, existing.reasoning()));
        }
    }

    static class Builder {
        private Reasoning reasoning;

        private void setReasoning(Reasoning reasoning) {
            this.reasoning = reasoning;
        }

        ElasticInferenceServiceChatCompletionTaskSettings build(TaskType taskType) {
            validateReasoning(taskType);
            return new ElasticInferenceServiceChatCompletionTaskSettings(reasoning);
        }

        private void validateReasoning(TaskType taskType) {
            if (reasoning != null && taskType != TaskType.CHAT_COMPLETION) {
                throw new IllegalArgumentException(
                    Strings.format("[%s] is only supported for the [%s] task type", REASONING_FIELD, TaskType.CHAT_COMPLETION)
                );
            }
        }
    }
}
