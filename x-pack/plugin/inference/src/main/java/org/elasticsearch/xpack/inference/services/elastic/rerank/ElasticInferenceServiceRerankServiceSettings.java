/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.rerank;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.parser.StringParser.validateStringIsNotNullOrEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MODEL_ID;

public class ElasticInferenceServiceRerankServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        ElasticInferenceServiceRateLimitServiceSettings {

    public static final String NAME = "elastic_rerank_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(
        false,
        ConfigurationParseContext.REQUEST
    );
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(
        true,
        ConfigurationParseContext.PERSISTENT
    );

    private static final TransportVersion ML_INFERENCE_ELASTIC_RERANK = TransportVersion.fromName("ml_inference_elastic_rerank");
    private static final TransportVersion INFERENCE_API_DISABLE_EIS_RATE_LIMITING = TransportVersion.fromName(
        "inference_api_disable_eis_rate_limiting"
    );

    /**
     * Creates a parser for the rerank service settings. In the {@link ConfigurationParseContext#REQUEST} context the
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
        RateLimitSettings.declareUnsupportedRateLimitField(
            parser,
            ModelConfigurations.SERVICE_SETTINGS,
            ElasticInferenceService.NAME,
            TaskType.RERANK,
            context
        );

        return parser;
    }

    public static ElasticInferenceServiceRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;

        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, map)) {
            var builder = parser.apply(xParser, context);
            // TODO: remove once all elastic service settings are parser-based and usesParserForServiceSettings can be enabled on
            // ElasticInferenceService. The object parser reads the map through an XContent view without consuming its entries, so
            // the parsed fields must be removed explicitly to satisfy the caller's check that no unknown settings remain in the map.
            map.remove(MODEL_ID);
            map.remove(RateLimitSettings.FIELD_NAME);
            return builder.build();
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse [{}]", e, ModelConfigurations.SERVICE_SETTINGS);
        }
    }

    public static class Builder {
        private String modelId;

        public void setModelId(String modelId) {
            this.modelId = Objects.requireNonNull(modelId);
        }

        public ElasticInferenceServiceRerankServiceSettings build() {
            validateStringIsNotNullOrEmpty(modelId, MODEL_ID);
            return new ElasticInferenceServiceRerankServiceSettings(modelId);
        }
    }

    private final String modelId;

    private final RateLimitSettings rateLimitSettings;

    public ElasticInferenceServiceRerankServiceSettings(String modelId) {
        this.modelId = Objects.requireNonNull(modelId);
        this.rateLimitSettings = RateLimitSettings.DISABLED_INSTANCE;
    }

    public ElasticInferenceServiceRerankServiceSettings(StreamInput in) throws IOException {
        this.modelId = in.readString();
        this.rateLimitSettings = RateLimitSettings.DISABLED_INSTANCE;
        if (in.getTransportVersion().supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING) == false) {
            new RateLimitSettings(in);
        }
    }

    @Override
    public String modelId() {
        return modelId;
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return rateLimitSettings;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        assert false : "should never be called when supportsVersion is used";
        return ML_INFERENCE_ELASTIC_RERANK;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(ML_INFERENCE_ELASTIC_RERANK);
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(MODEL_ID, modelId);
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragmentOfExposedFields(builder, params);

        builder.endObject();

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        if (out.getTransportVersion().supports(INFERENCE_API_DISABLE_EIS_RATE_LIMITING) == false) {
            rateLimitSettings.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceRerankServiceSettings that = (ElasticInferenceServiceRerankServiceSettings) o;
        return Objects.equals(modelId, that.modelId) && Objects.equals(rateLimitSettings, that.rateLimitSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, rateLimitSettings);
    }
}
