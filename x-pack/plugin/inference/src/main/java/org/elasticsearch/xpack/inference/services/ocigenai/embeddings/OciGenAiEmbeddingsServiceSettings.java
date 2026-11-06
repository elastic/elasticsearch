/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.embeddings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.parser.EnumParser;
import org.elasticsearch.xpack.inference.common.parser.StatefulValue;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ServiceFields;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.inference.InferenceUtils.missingSettingErrorMsg;
import static org.elasticsearch.xpack.inference.common.parser.NumberParser.validatePositiveInteger;
import static org.elasticsearch.xpack.inference.common.parser.StatefulValue.applyUpdate;
import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.SettingsScope.SERVICE_SETTINGS;

/**
 * Service settings of the OCI Generative AI text embedding task. Adds the embedding dimensions, similarity measure and maximum input
 * token count to the common OCI Generative AI settings. When the user sets {@code dimensions}, the value is passed to the service as
 * {@code outputDimensions} (supported by {@code cohere.embed-v4.0}); otherwise the model's native dimensionality is discovered from the
 * first embedding response.
 */
public class OciGenAiEmbeddingsServiceSettings extends OciGenAiServiceSettings {

    public static final String NAME = "oci_genai_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(ConfigurationParseContext.REQUEST);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(
        ConfigurationParseContext.PERSISTENT
    );

    /**
     * Creates the {@link ObjectParser} of the given context. The request parser is strict (unexpected fields are rejected) whereas the
     * persisted configuration parser tolerates fields written by other versions. Only the persisted configuration parser declares
     * {@code dimensions_set_by_user}: it is written by Elasticsearch when the endpoint is stored and must not be supplied in requests,
     * which the strict request parser rejects as an unknown field.
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(ConfigurationParseContext context) {
        var ignoreUnknownFields = context == ConfigurationParseContext.PERSISTENT;
        var parser = OciGenAiServiceSettings.buildCommonParser(ignoreUnknownFields, () -> new Builder(context));
        parser.declareInt(Builder::setDimensions, new ParseField(DIMENSIONS));
        parser.declareString(Builder::setSimilarity, EnumParser::parseSimilarity, new ParseField(SIMILARITY));
        parser.declareInt(Builder::setMaxInputTokens, new ParseField(MAX_INPUT_TOKENS));
        if (context == ConfigurationParseContext.PERSISTENT) {
            parser.declareBoolean(Builder::setDimensionsSetByUser, new ParseField(ServiceFields.DIMENSIONS_SET_BY_USER));
        }
        return parser;
    }

    public static OciGenAiEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return OciGenAiServiceSettings.fromMap(map, context, parser);
    }

    private final Boolean dimensionsSetByUser;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final SimilarityMeasure similarity;

    public OciGenAiEmbeddingsServiceSettings(
        CommonSettings common,
        Boolean dimensionsSetByUser,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable SimilarityMeasure similarity
    ) {
        super(common);
        this.dimensionsSetByUser = Objects.requireNonNull(dimensionsSetByUser);
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.similarity = similarity;
    }

    public OciGenAiEmbeddingsServiceSettings(StreamInput in) throws IOException {
        this(
            new CommonSettings(in),
            in.readBoolean(),
            in.readOptionalVInt(),
            in.readOptionalVInt(),
            in.readOptionalEnum(SimilarityMeasure.class)
        );
    }

    @Override
    public OciGenAiEmbeddingsServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        return parseUpdate(serviceSettings, Update.PARSER).mergeInto(this);
    }

    @Override
    public Boolean dimensionsSetByUser() {
        return dimensionsSetByUser;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    public Integer maxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        common().writeTo(out);
        out.writeBoolean(dimensionsSetByUser);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeOptionalEnum(similarity);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragmentOfExposedFields(builder, params);
        builder.field(ServiceFields.DIMENSIONS_SET_BY_USER, dimensionsSetByUser);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, ToXContent.Params params) throws IOException {
        super.toXContentFragmentOfExposedFields(builder, params);
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (super.equals(o) == false) return false;
        OciGenAiEmbeddingsServiceSettings that = (OciGenAiEmbeddingsServiceSettings) o;
        return Objects.equals(dimensionsSetByUser, that.dimensionsSetByUser)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(similarity, that.similarity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), dimensionsSetByUser, dimensions, maxInputTokens, similarity);
    }

    /**
     * Accumulates the embeddings specific fields on top of the common OCI Generative AI fields and builds an
     * {@link OciGenAiEmbeddingsServiceSettings}, enforcing that {@code dimensions} and {@code max_input_tokens} are positive. In
     * {@link ConfigurationParseContext#REQUEST} context {@code dimensions_set_by_user} is derived from the presence of
     * {@code dimensions}; in {@link ConfigurationParseContext#PERSISTENT} context the stored value is required.
     */
    public static class Builder extends OciGenAiServiceSettings.Builder<OciGenAiEmbeddingsServiceSettings> {

        private final ConfigurationParseContext context;
        private Integer dimensions;
        private Boolean dimensionsSetByUser;
        private Integer maxInputTokens;
        private SimilarityMeasure similarity;

        public Builder(ConfigurationParseContext context) {
            this.context = Objects.requireNonNull(context);
        }

        public void setDimensions(Integer dimensions) {
            validatePositiveInteger(dimensions, DIMENSIONS);
            this.dimensions = dimensions;
        }

        public void setDimensionsSetByUser(Boolean dimensionsSetByUser) {
            this.dimensionsSetByUser = dimensionsSetByUser;
        }

        public void setMaxInputTokens(Integer maxInputTokens) {
            validatePositiveInteger(maxInputTokens, MAX_INPUT_TOKENS);
            this.maxInputTokens = maxInputTokens;
        }

        public void setSimilarity(SimilarityMeasure similarity) {
            this.similarity = similarity;
        }

        @Override
        protected OciGenAiEmbeddingsServiceSettings build(CommonSettings common) {
            boolean resolvedDimensionsSetByUser = switch (context) {
                case REQUEST -> dimensions != null;
                case PERSISTENT -> {
                    if (dimensionsSetByUser == null) {
                        throw new IllegalArgumentException(
                            missingSettingErrorMsg(ServiceFields.DIMENSIONS_SET_BY_USER, SERVICE_SETTINGS.toString())
                        );
                    }
                    yield dimensionsSetByUser;
                }
            };
            return new OciGenAiEmbeddingsServiceSettings(common, resolvedDimensionsSetByUser, dimensions, maxInputTokens, similarity);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code max_input_tokens} and {@code rate_limit} fields (and the
     * signing key fields). Including any immutable field (such as {@code model_id}, {@code dimensions} or {@code similarity}) causes
     * the strict parser to reject the request.
     */
    private static class Update extends OciGenAiServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = createUpdateParser();

        private static ObjectParser<Update, Void> createUpdateParser() {
            var parser = OciGenAiServiceSettings.buildCommonUpdateParser(Update::new);
            StatefulValue.declareNullable(parser, (update, value) -> update.maxInputTokens = value, p -> {
                Integer value = p.intValue();
                validatePositiveInteger(value, MAX_INPUT_TOKENS);
                return value;
            }, new ParseField(MAX_INPUT_TOKENS), ObjectParser.ValueType.INT_OR_NULL);
            return parser;
        }

        private StatefulValue<Integer> maxInputTokens = StatefulValue.undefined();

        OciGenAiEmbeddingsServiceSettings mergeInto(OciGenAiEmbeddingsServiceSettings existing) {
            return new OciGenAiEmbeddingsServiceSettings(
                existing.common().withRateLimitSettings(mergedRateLimitSettings(existing)),
                existing.dimensionsSetByUser(),
                existing.dimensions(),
                applyUpdate(maxInputTokens, existing.maxInputTokens()),
                existing.similarity()
            );
        }
    }
}
