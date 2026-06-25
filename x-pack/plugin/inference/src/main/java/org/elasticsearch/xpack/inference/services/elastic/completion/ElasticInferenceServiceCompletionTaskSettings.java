/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.completion;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Task settings for the Elastic Inference Service {@code chat_completion} task type, holding
 * an optional {@code reasoning} block that is applied to each request unless overridden by the
 * request body (body wins at the object level).
 */
public class ElasticInferenceServiceCompletionTaskSettings implements TaskSettings {

    public static final String NAME = "elastic_completion_task_settings";

    static final TransportVersion EIS_REASONING_TASK_SETTINGS_ADDED = TransportVersion.fromName(
        "inference_api_eis_reasoning_task_settings"
    );

    private static final ConstructingObjectParser<ElasticInferenceServiceCompletionTaskSettings, ConfigurationParseContext> REQUEST_PARSER =
        createParser(false);
    private static final ConstructingObjectParser<
        ElasticInferenceServiceCompletionTaskSettings,
        ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    static ConstructingObjectParser<ElasticInferenceServiceCompletionTaskSettings, ConfigurationParseContext> createParser(
        boolean ignoreUnknownFields
    ) {
        ConstructingObjectParser<ElasticInferenceServiceCompletionTaskSettings, ConfigurationParseContext> parser =
            new ConstructingObjectParser<>(
                NAME,
                ignoreUnknownFields,
                args -> new ElasticInferenceServiceCompletionTaskSettings((Reasoning) args[0])
            );
        parser.declareObject(optionalConstructorArg(), (p, c) -> Reasoning.PARSER.apply(p, null), new ParseField(REASONING_FIELD));
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
     * @throws ValidationException       if {@code reasoning} is present but {@code taskType} is not
     *                                   {@link TaskType#CHAT_COMPLETION}
     * @throws ElasticsearchParseException if XContent parsing fails
     */
    public static ElasticInferenceServiceCompletionTaskSettings fromMap(
        @Nullable Map<String, Object> map,
        TaskType taskType,
        ConfigurationParseContext context
    ) {
        if (map == null || map.isEmpty()) {
            return new ElasticInferenceServiceCompletionTaskSettings((Reasoning) null);
        }

        ElasticInferenceServiceCompletionTaskSettings settings;
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            settings = parser.apply(xParser, null);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.TASK_SETTINGS);
        }

        if (settings.reasoning != null && taskType != TaskType.CHAT_COMPLETION) {
            var validationException = new ValidationException();
            validationException.addValidationError(
                "[" + REASONING_FIELD + "] is only supported for the [" + TaskType.CHAT_COMPLETION + "] task type"
            );
            throw validationException;
        }

        return settings;
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

    public ElasticInferenceServiceCompletionTaskSettings(@Nullable Reasoning reasoning) {
        this.reasoning = reasoning;
    }

    public ElasticInferenceServiceCompletionTaskSettings(StreamInput in) throws IOException {
        this.reasoning = in.getTransportVersion().supports(EIS_REASONING_TASK_SETTINGS_ADDED)
            ? in.readOptionalWriteable(Reasoning::new)
            : null;
    }

    /**
     * @return the stored reasoning configuration, or {@code null} if none was set
     */
    @Nullable
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
     * Produces updated task settings by re-parsing {@code newSettings} via the strict REQUEST
     * parser and applying body-wins precedence: if the new settings carry a {@code reasoning}
     * block it replaces the stored one; otherwise the stored reasoning is preserved.
     */
    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        if (newSettings == null || newSettings.isEmpty()) {
            return this;
        }
        var updated = fromMap(newSettings, TaskType.CHAT_COMPLETION, ConfigurationParseContext.REQUEST);
        return new ElasticInferenceServiceCompletionTaskSettings(mergeReasoning(updated.reasoning, this.reasoning));
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
        if (out.getTransportVersion().supports(EIS_REASONING_TASK_SETTINGS_ADDED)) {
            out.writeOptionalWriteable(reasoning);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var other = (ElasticInferenceServiceCompletionTaskSettings) o;
        return Objects.equals(reasoning, other.reasoning);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(reasoning);
    }

    @Override
    public String toString() {
        return "ElasticInferenceServiceCompletionTaskSettings{reasoning=" + reasoning + "}";
    }
}
