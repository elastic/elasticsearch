/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.jinaai.JinaAIService.JINA_AI_EMBEDDING_REFACTOR;

/**
 * Common service settings shared across all JinaAI inference tasks (model identity and rate limiting), together with the parsing,
 * serialization, and update machinery that would otherwise be duplicated. Task-specific settings classes embed this value object
 * as a member ({@code commonSettings}) rather than extending it.
 * <p>
 * This class is not a {@link org.elasticsearch.inference.ServiceSettings} and not a named writeable: it is only ever serialized as
 * an embedded component inside the task-specific settings classes.
 */
public class JinaAICommonServiceSettings extends FilteredXContentObject
    implements
        JinaAIRateLimitServiceSettings,
        ToXContentFragment,
        Writeable {

    // See https://jina.ai/contact-sales/#rate-limit
    public static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(2_000);

    /**
     * Registers the common JinaAI service-settings fields (model_id, rate_limit) onto the given parser.
     */
    public static <B extends Builder<?>> void declareCommonFields(AbstractObjectParser<B, ConfigurationParseContext> parser) {
        parser.declareString(Builder::setModelId, new ParseField(ServiceFields.MODEL_ID));
        parser.declareObject(
            Builder::setRateLimitSettings,
            // An explicitly empty rate_limit object ({}) resolves to the default rate limit rather than null, so the setter is never
            // invoked with null.
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, DEFAULT_RATE_LIMIT_SETTINGS).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        // api_key appears in the same JSON block as service settings in REST requests; DefaultSecretSettings extracts it separately.
        // Declare it here as a no-op so the strict REQUEST parser does not reject it as an unknown field.
        parser.declareString((b, v) -> {}, new ParseField(DefaultSecretSettings.API_KEY));
    }

    private final String modelId;
    private final RateLimitSettings rateLimitSettings;

    public JinaAICommonServiceSettings(String modelId, @Nullable RateLimitSettings rateLimitSettings) {
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public JinaAICommonServiceSettings(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(JINA_AI_EMBEDDING_REFACTOR) == false) {
            // URI is no longer part of service settings since it's only used for testing
            in.readOptionalString();
            // ModelID was incorrectly being serialized as optional
            modelId = in.readOptionalString();
        } else {
            modelId = in.readString();
        }
        rateLimitSettings = new RateLimitSettings(in);
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    public String modelId() {
        return modelId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContentFragmentOfExposedFields(builder, params);
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(ServiceFields.MODEL_ID, modelId);
        rateLimitSettings.toXContent(builder, params);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(JINA_AI_EMBEDDING_REFACTOR) == false) {
            // URI is no longer part of service settings since it's only used for testing
            out.writeOptionalString(null);
            // ModelID was incorrectly being serialized as optional
            out.writeOptionalString(modelId);
        } else {
            out.writeString(modelId);
        }
        rateLimitSettings.writeTo(out);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAICommonServiceSettings that = (JinaAICommonServiceSettings) o;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }

    /**
     * Accumulates the parsed common fields and assembles a {@link JinaAICommonServiceSettings}, enforcing that the required
     * {@code model_id} field is present. Task-specific builders extend this and contribute their own fields. The {@link
     * ConfigurationParseContext} is captured so subclasses can resolve context-dependent fields at build time.
     *
     * @param <T> the task-specific settings type produced by {@link #build(JinaAICommonServiceSettings)}
     */
    public abstract static class Builder<T> {

        protected final ConfigurationParseContext context;

        private String modelId;
        protected RateLimitSettings rateLimitSettings;

        protected Builder(ConfigurationParseContext context) {
            this.context = Objects.requireNonNull(context);
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        protected JinaAICommonServiceSettings buildCommonSettings() {
            validateStringIsNotNullOrEmpty(modelId, ServiceFields.MODEL_ID);
            return new JinaAICommonServiceSettings(modelId, rateLimitSettings);
        }

        protected abstract T build(JinaAICommonServiceSettings commonSettings);

        public final T build() {
            return build(buildCommonSettings());
        }
    }

    /**
     * Creates a task-specific settings instance from a map of settings using the given parser.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser to use for parsing the settings
     * @return the created settings instance
     */
    public static <T> T fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        ObjectParser<? extends Builder<T>, ConfigurationParseContext> parser
    ) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            return parser.apply(xParser, context).build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    /**
     * Registers the common JinaAI fields that may be changed by an update request. Only {@code rate_limit} is mutable; the
     * immutable fields (such as {@code model_id}) are intentionally not declared so that a strict update parser rejects attempts
     * to change them.
     */
    public static void declareCommonUpdatableFields(AbstractObjectParser<? extends CommonUpdate, Void> parser) {
        StatefulValue.declareNullable(
            parser,
            (update, value) -> update.rateLimitSettings = value,
            (p) -> RateLimitSettings.createParser(false, null).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME),
            ObjectParser.ValueType.OBJECT_OR_NULL
        );
    }

    /**
     * Common fields parsed from an update request. Because settings are immutable, each subclass builds the new instance itself,
     * calling {@link #update(CommonUpdate)} to resolve the shared fields.
     */
    public static class CommonUpdate {

        protected StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();
    }

    /**
     * Resolves the rate limit settings to use after applying the update following the tri-state convention: an omitted field keeps
     * the current value, an explicit null resets the field to the default rate limit, and a present value replaces the current one.
     */
    public JinaAICommonServiceSettings update(CommonUpdate update) {
        return new JinaAICommonServiceSettings(
            modelId,
            applyUpdate(update.rateLimitSettings, rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS)
        );
    }
}
