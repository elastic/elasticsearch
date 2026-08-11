/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.completion;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.openai.OpenAiTaskSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMapRemoveNulls;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateMapStringValues;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.HEADERS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiServiceFields.USER;

/**
 * Task settings for the OpenAI {@code chat_completion} and {@code completion} task types.
 * <p>
 * For {@link TaskType#CHAT_COMPLETION}, an optional {@code reasoning} block may be stored and used as
 * a default that is merged with (and overridden by) the unified chat completion request body.
 * OpenAI's Chat Completions API only accepts {@code reasoning_effort}, so only {@code reasoning.effort}
 * is forwarded when building the upstream request.
 * <p>
 * At persistence time ({@link #updatedTaskSettings}), {@code reasoning} follows tri-state semantics:
 * <ul>
 *   <li>field omitted → keep the stored value;</li>
 *   <li>explicit {@code null} → clear to {@code null};</li>
 *   <li>field present with a value → replace the stored value.</li>
 * </ul>
 * At request time ({@link #mergeReasoning}), body-wins precedence applies.
 */
public class OpenAiChatCompletionTaskSettings extends OpenAiTaskSettings<OpenAiChatCompletionTaskSettings> {

    public static final String NAME = "openai_completion_task_settings";

    public static final TransportVersion OPENAI_REASONING_TASK_SETTINGS = TransportVersion.fromName(
        "inference_api_openai_reasoning_task_settings"
    );

    public static final String REASONING_FIELD_UNSUPPORTED_MESSAGE = """
        The reasoning field in task_settings is not supported by all nodes in the cluster; \
        please finish upgrading before using the reasoning field""";

    private static final TransportVersion INFERENCE_API_OPENAI_HEADERS = TransportVersion.fromName("inference_api_openai_headers");

    private record Parsed(@Nullable String user, @Nullable Map<String, String> headers, @Nullable Reasoning reasoning) {}

    /**
     * Parses task settings from a raw config map.
     *
     * @param map      the raw task settings map (may be null or empty)
     * @param taskType the task type for the endpoint being created
     * @throws IllegalArgumentException if {@code reasoning} is present but {@code taskType} is not
     *                                  {@link TaskType#CHAT_COMPLETION}
     */
    public static OpenAiChatCompletionTaskSettings fromMap(@Nullable Map<String, Object> map, TaskType taskType) {
        return new OpenAiChatCompletionTaskSettings(parseMap(map, taskType));
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

    private static Parsed parseMap(@Nullable Map<String, Object> map, TaskType taskType) {
        if (map == null || map.isEmpty()) {
            return new Parsed(null, null, null);
        }

        // Mutate the caller's map so known keys are consumed before unknown-settings validation.
        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER, ModelConfigurations.TASK_SETTINGS, validationException);
        Map<String, Object> headers = extractOptionalMapRemoveNulls(map, HEADERS, validationException);
        var stringHeaders = validateMapStringValues(headers, HEADERS, validationException, false, null);

        Reasoning reasoning = null;
        Object reasoningValue = map.remove(REASONING_FIELD);
        if (reasoningValue != null) {
            if (reasoningValue instanceof Map<?, ?> == false) {
                validationException.addValidationError(
                    Strings.format("[%s] must be an object", ModelConfigurations.TASK_SETTINGS + "." + REASONING_FIELD)
                );
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> reasoningMap = (Map<String, Object>) reasoningValue;
                try (var parser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, reasoningMap)) {
                    reasoning = Reasoning.PARSER.apply(parser, null);
                } catch (IOException e) {
                    throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.TASK_SETTINGS);
                }
            }
        }

        validationException.throwIfValidationErrorsExist();
        validateReasoning(reasoning, taskType);

        return new Parsed(user, stringHeaders, reasoning);
    }

    private static void validateReasoning(@Nullable Reasoning reasoning, TaskType taskType) {
        if (reasoning != null && taskType != TaskType.CHAT_COMPLETION) {
            throw new IllegalArgumentException(
                Strings.format("[%s] is only supported for the [%s] task type", REASONING_FIELD, TaskType.CHAT_COMPLETION)
            );
        }
    }

    private final Reasoning reasoning;

    /**
     * Convenience constructor used by tests and callers that do not supply a task type.
     * Defaults to {@link TaskType#CHAT_COMPLETION} so {@code reasoning} is allowed.
     */
    public OpenAiChatCompletionTaskSettings(Map<String, Object> map) {
        this(parseMap(map, TaskType.CHAT_COMPLETION));
    }

    public OpenAiChatCompletionTaskSettings(@Nullable String user, @Nullable Map<String, String> headers) {
        this(user, headers, null);
    }

    public OpenAiChatCompletionTaskSettings(@Nullable String user, @Nullable Map<String, String> headers, @Nullable Reasoning reasoning) {
        super(user, headers);
        this.reasoning = reasoning;
    }

    private OpenAiChatCompletionTaskSettings(Parsed parsed) {
        this(parsed.user(), parsed.headers(), parsed.reasoning());
    }

    public OpenAiChatCompletionTaskSettings(StreamInput in) throws IOException {
        super(readUserAndHeaders(in));
        if (in.getTransportVersion().supports(OPENAI_REASONING_TASK_SETTINGS)) {
            this.reasoning = in.readOptionalWriteable(Reasoning::new);
        } else {
            this.reasoning = null;
        }
    }

    private static Settings readUserAndHeaders(StreamInput in) throws IOException {
        var user = in.readOptionalString();

        Map<String, String> headers;
        if (in.getTransportVersion().supports(INFERENCE_API_OPENAI_HEADERS)) {
            headers = in.readOptionalImmutableMap(StreamInput::readString, StreamInput::readString);
        } else {
            headers = null;
        }

        return createSettings(user, headers);
    }

    /**
     * @return the stored reasoning configuration, or {@code null} if none was set
     */
    public Reasoning reasoning() {
        return reasoning;
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty() && reasoning == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (user() != null) {
            builder.field(USER, user());
        }

        if (headers() != null && headers().isEmpty() == false) {
            builder.field(HEADERS, headers());
        }

        if (reasoning != null) {
            builder.field(REASONING_FIELD, reasoning);
        }

        builder.endObject();
        return builder;
    }

    /**
     * Produces updated task settings. {@code user} and {@code headers} keep prior values when omitted
     * from {@code newSettings}. {@code reasoning} follows tri-state semantics (omit / null / value).
     */
    @Override
    public OpenAiChatCompletionTaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        if (newSettings == null || newSettings.isEmpty()) {
            return this;
        }

        // Mutate newSettings so known keys are consumed before unknown-settings validation.
        ValidationException validationException = new ValidationException();

        String updatedUser = extractOptionalString(newSettings, USER, ModelConfigurations.TASK_SETTINGS, validationException);
        Map<String, Object> updatedHeadersMap = extractOptionalMapRemoveNulls(newSettings, HEADERS, validationException);
        var updatedHeaders = updatedHeadersMap == null
            ? null
            : validateMapStringValues(updatedHeadersMap, HEADERS, validationException, false, null);
        validationException.throwIfValidationErrorsExist();

        var userToUse = updatedUser == null ? user() : updatedUser;
        var headersToUse = updatedHeaders == null ? headers() : updatedHeaders;

        Reasoning reasoningToUse = reasoning;
        if (newSettings.containsKey(REASONING_FIELD)) {
            try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, newSettings)) {
                reasoningToUse = applyUpdate(Update.PARSER.apply(xParser, null).reasoning, reasoning);
            } catch (IOException e) {
                throw new ElasticsearchParseException("Failed to parse [{}] update", e, ModelConfigurations.TASK_SETTINGS);
            }
            validateReasoning(reasoningToUse, TaskType.CHAT_COMPLETION);
            newSettings.remove(REASONING_FIELD);
        }

        return new OpenAiChatCompletionTaskSettings(userToUse, headersToUse, reasoningToUse);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(user());
        if (out.getTransportVersion().supports(INFERENCE_API_OPENAI_HEADERS)) {
            out.writeOptionalMap(headers(), StreamOutput::writeString, StreamOutput::writeString);
        }
        if (out.getTransportVersion().supports(OPENAI_REASONING_TASK_SETTINGS)) {
            out.writeOptionalWriteable(reasoning);
        } else if (reasoning != null) {
            throw new ElasticsearchStatusException(REASONING_FIELD_UNSUPPORTED_MESSAGE, RestStatus.BAD_REQUEST);
        }
    }

    @Override
    protected OpenAiChatCompletionTaskSettings create(@Nullable String user, @Nullable Map<String, String> headers) {
        return new OpenAiChatCompletionTaskSettings(user, headers, reasoning);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        var that = (OpenAiChatCompletionTaskSettings) o;
        return Objects.equals(reasoning, that.reasoning);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), reasoning);
    }

    /**
     * Parses a task-settings update following tri-state semantics for {@code reasoning}.
     */
    private static class Update {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.TASK_SETTINGS, true, Update::new);

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
    }
}
