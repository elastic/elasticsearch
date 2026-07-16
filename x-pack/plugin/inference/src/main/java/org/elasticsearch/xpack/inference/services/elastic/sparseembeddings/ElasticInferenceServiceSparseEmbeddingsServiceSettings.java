/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.sparseembeddings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE;

public class ElasticInferenceServiceSparseEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings {

    // Default rate limits for ELSER endpoints hosted on EIS are in the eis-gateway repo under config/default.yaml
    // 6k requests per minute
    protected static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(6_000);

    public static final String NAME = "elastic_inference_service_sparse_embeddings_service_settings";

    private static final TransportVersion INFERENCE_API_DISABLE_EIS_RATE_LIMITING = TransportVersion.fromName(
        "inference_api_disable_eis_rate_limiting"
    );

    public static <B extends Builder<? extends ElasticInferenceServiceSparseEmbeddingsServiceSettings>> void declareCommonFields(
        AbstractObjectParser<B, ConfigurationParseContext> parser
    ){

        parser.declareString(Builder::setModelId, new ParseField(MODEL_ID));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        parser.declareObject(
            Builder::setRateLimitSettings,
            // An explicitly empty rate_limit object ({}) resolves to the default rate limit rather than null, so the setter is never
            // invoked with null.
            (p, c) -> RateLimitSettings.createParser(c == ConfigurationParseContext.PERSISTENT, DEFAULT_RATE_LIMIT_SETTINGS).apply(p, null),
            new ParseField(RateLimitSettings.FIELD_NAME)
        );
        parser.declareInt(Builder::setMaxBatchSize, new ParseField(MAX_BATCH_SIZE));
    }

    /**
     * Creates an {@link ElasticInferenceServiceSparseEmbeddingsServiceSettings} from a map of settings using the given parser.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @param parser  the parser to use for parsing the settings
     * @return the created {@link ElasticInferenceServiceSparseEmbeddingsServiceSettings}
     */
    public static <T extends ElasticInferenceServiceSparseEmbeddingsServiceSettings> T fromMap(
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
     * Registers the common Elastic Inference Sparse Embeddings fields that may be changed by an update request. Only {@code rate_limit} is mutable; the
     * immutable fields (such as {@code model_id}, {@code max_batch_size} and {@code max_input_tokens}) are intentionally not
     * declared so that a strict update parser rejects attempts to change them.
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
     * calling {@link #mergedRateLimitSettings(ElasticInferenceServiceSparseEmbeddingsServiceSettings)} to resolve the shared fields.
     */
    public static class CommonUpdate {

        protected StatefulValue<RateLimitSettings> rateLimitSettings = StatefulValue.undefined();

        /**
         * Resolves the rate limit settings to use after applying the update following the tri-state convention: an omitted field keeps
         * the current value, an explicit null resets the field to the default rate limit, and a present value replaces the current one.
         */
        protected RateLimitSettings mergedRateLimitSettings(ElasticInferenceServiceSparseEmbeddingsServiceSettings existing) {
            return applyUpdate(rateLimitSettings, existing.rateLimitSettings(), DEFAULT_RATE_LIMIT_SETTINGS);
        }
    }

    private final String modelId;
    private final Integer maxInputTokens;
    private final RateLimitSettings rateLimitSettings;
    private final Integer maxBatchSize;

    public ElasticInferenceServiceSparseEmbeddingsServiceSettings(
        String modelId,
        @Nullable Integer maxInputTokens,
        @Nullable Integer maxBatchSize
    ) {
        this.modelId = Objects.requireNonNull(modelId);
        this.maxInputTokens = maxInputTokens;
        this.maxBatchSize = maxBatchSize;
        this.rateLimitSettings = RateLimitSettings.DISABLED_INSTANCE;
    }

    public ElasticInferenceServiceSparseEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.maxInputTokens = in.readOptionalVInt();
        this.rateLimitSettings = RateLimitSettings.DISABLED_INSTANCE;
        if (in.getTransportVersion().supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING) == false) {
            new RateLimitSettings(in);
        }
        if (in.getTransportVersion().supports(ElasticInferenceServiceSettingsUtils.INFERENCE_API_EIS_MAX_BATCH_SIZE)) {
            this.maxBatchSize = in.readOptionalVInt();
        } else {
            this.maxBatchSize = null;
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String modelId() {
        return modelId;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    public Integer maxBatchSize() {
        return maxBatchSize;
    }

    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

        builder.endObject();

        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (maxBatchSize != null) {
            builder.field(MAX_BATCH_SIZE, maxBatchSize);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeOptionalVInt(maxInputTokens);
        if (out.getTransportVersion().supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING) == false) {
            rateLimitSettings.writeTo(out);
        }
        if (out.getTransportVersion().supports(ElasticInferenceServiceSettingsUtils.INFERENCE_API_EIS_MAX_BATCH_SIZE)) {
            out.writeOptionalVInt(maxBatchSize);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ElasticInferenceServiceSparseEmbeddingsServiceSettings that = (ElasticInferenceServiceSparseEmbeddingsServiceSettings) object;
        return Objects.equals(modelId, that.modelId)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(maxBatchSize, that.maxBatchSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, maxInputTokens, rateLimitSettings, maxBatchSize);
    }


    private abstract static class Builder<T extends ElasticInferenceServiceSparseEmbeddingsServiceSettings> {
        private String modelId;
        private Integer maxInputTokens;
        private Integer maxBatchSize;
        protected RateLimitSettings rateLimitSettings;

        public void setModelId(String modelId){
            this.modelId = modelId;
        }

        public void setMaxBatchSize(Integer maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public void setMaxInputTokens(Integer maxInputTokens) {
            this.maxInputTokens = maxInputTokens;
        }

        public void setRateLimitSettings(RateLimitSettings rateLimitSettings) {
            this.rateLimitSettings = rateLimitSettings;
        }

        Builder(String modelId) {
            this.modelId = Objects.requireNonNull(modelId);
        }

        Builder maxInputTokens(Integer maxInputTokens) {
            this.maxInputTokens = maxInputTokens;
            return this;
        }

        Builder maxBatchSize(Integer maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        protected abstract T build(String modelId, Integer maxInputTokens, Integer maxBatchSize, RateLimitSettings rateLimitSettings);

        public final T build() {
            validateStringIsNotNullOrEmpty(modelId, MODEL_ID);
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            validatePositiveInteger(maxBatchSize, MAX_BATCH_SIZE);
            return build(modelId, maxInputTokens, maxBatchSize, rateLimitSettings);
        }
    }
}
