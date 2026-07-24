/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.sparseembeddings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsUtils;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveIntegerLessThanOrEqualToMax;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsUtils.MAX_BATCH_SIZE_UPPER_BOUND;

public class ElasticInferenceServiceSparseEmbeddingsServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        ElasticInferenceServiceRateLimitServiceSettings {

    public static final String NAME = "elastic_inference_service_sparse_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(
        false,
        ConfigurationParseContext.REQUEST
    );
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(
        true,
        ConfigurationParseContext.PERSISTENT
    );

    private static final TransportVersion INFERENCE_API_DISABLE_EIS_RATE_LIMITING = TransportVersion.fromName(
        "inference_api_disable_eis_rate_limiting"
    );

    /**
     * Creates a parser for the sparse embeddings service settings. In the {@link ConfigurationParseContext#REQUEST} context the
     * {@code rate_limit} field is declared solely to reject it with a validation error, since this service does not permit
     * user-supplied rate limits; in the {@link ConfigurationParseContext#PERSISTENT} context it is ignored like any other
     * unknown field, so that previously persisted configurations remain readable.
     *
     * @param ignoreUnknownFields whether unknown fields are tolerated; {@code false} for user requests, {@code true} for persisted config
     * @param context the parse context the returned parser is intended for
     */
    public static ObjectParser<Builder, ConfigurationParseContext> createParser(
        boolean ignoreUnknownFields,
        ConfigurationParseContext context
    ) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );

        parser.declareString(Builder::setModelId, new ParseField(MODEL_ID));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        parser.declareInt(Builder::setMaxBatchSize, new ParseField(MAX_BATCH_SIZE));
        RateLimitSettings.declareUnsupportedRateLimitField(
            parser,
            ModelConfigurations.SERVICE_SETTINGS,
            ElasticInferenceService.NAME,
            TaskType.SPARSE_EMBEDDING,
            context
        );

        return parser;
    }

    public static ElasticInferenceServiceSparseEmbeddingsServiceSettings fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context
    ) {

        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;

        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            var builder = parser.apply(xParser, context);
            // TODO: remove once all elastic service settings are parser-based and usesParserForServiceSettings can be enabled on
            // ElasticInferenceService. The object parser reads the map through an XContent view without consuming its entries, so
            // the parsed fields must be removed explicitly to satisfy the caller's check that no unknown settings remain in the map.
            map.remove(MODEL_ID);
            map.remove(MAX_INPUT_TOKENS);
            map.remove(MAX_BATCH_SIZE);
            map.remove(RateLimitSettings.FIELD_NAME);
            return builder.build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
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

    @Override
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

    /**
     * Parses an update request. Only {@code max_batch_size} is mutable; immutable fields (e.g. {@code model_id},
     * {@code max_input_tokens}) are rejected by the strict parser.
     */
    private static class Update {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        private StatefulValue<Integer> maxBatchSize = StatefulValue.undefined();

        static {
            StatefulValue.declareNullable(PARSER, (update, value) -> update.maxBatchSize = value, p -> {
                Integer value = p.intValue();
                validatePositiveIntegerLessThanOrEqualToMax(value, MAX_BATCH_SIZE, MAX_BATCH_SIZE_UPPER_BOUND);
                return value;
            }, new ParseField(MAX_BATCH_SIZE), ObjectParser.ValueType.INT_OR_NULL);
        }

        public ElasticInferenceServiceSparseEmbeddingsServiceSettings mergeInto(
            ElasticInferenceServiceSparseEmbeddingsServiceSettings existing
        ) {
            var updatedMaxBatchSize = applyUpdate(this.maxBatchSize, existing.maxBatchSize());
            return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(
                existing.modelId(),
                existing.maxInputTokens(),
                updatedMaxBatchSize
            );
        }
    }

    @Override
    public ElasticInferenceServiceSparseEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            var update = Update.PARSER.apply(xParser, null);
            // TODO: remove once all elastic service settings are parser-based and usesParserForServiceSettings can be enabled on
            // ElasticInferenceService. The object parser reads the map through an XContent view without consuming its entries, so
            // the parsed field must be removed explicitly to satisfy the caller's check that no unknown settings remain in the map.
            serviceSettings.remove(MAX_BATCH_SIZE);
            return update.mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Elastic Inference Sparse Embeddings service settings update", e);
        }
    }

    public static class Builder {
        private String modelId;
        private Integer maxInputTokens;
        private Integer maxBatchSize;

        public void setModelId(String modelId) {
            this.modelId = Objects.requireNonNull(modelId);
        }

        public void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        public void setMaxBatchSize(Integer maxBatchSize) {
            validatePositiveIntegerLessThanOrEqualToMax(maxBatchSize, MAX_BATCH_SIZE, MAX_BATCH_SIZE_UPPER_BOUND);
            this.maxBatchSize = maxBatchSize;
        }

        public ElasticInferenceServiceSparseEmbeddingsServiceSettings build() {
            validateStringIsNotNullOrEmpty(modelId, MODEL_ID);
            return new ElasticInferenceServiceSparseEmbeddingsServiceSettings(modelId, maxInputTokens, maxBatchSize);
        }
    }
}
