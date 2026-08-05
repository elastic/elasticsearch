/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.rerank;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_API_VERSION;
import static org.elasticsearch.xpack.inference.services.cohere.CohereCommonServiceSettings.ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR;

/**
 * Settings for the Cohere reranking service.
 */
public class CohereRerankServiceSettings extends FilteredXContentObject implements CohereServiceSettings {

    public static final String NAME = "cohere_rerank_service_settings";

    public static class Builder extends CohereCommonServiceSettings.Builder<CohereRerankServiceSettings> {

        protected Builder(ConfigurationParseContext context) {
            super(context);
        }

        @Override
        protected CohereRerankServiceSettings build(CohereCommonServiceSettings commonSettings) {
            return new CohereRerankServiceSettings(commonSettings);
        }
    }

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(
        false,
        ConfigurationParseContext.REQUEST
    );
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(
        true,
        ConfigurationParseContext.PERSISTENT
    );

    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields, ConfigurationParseContext context) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            () -> new Builder(context)
        );
        CohereCommonServiceSettings.declareCommonFields(parser, context);
        return parser;
    }

    /**
     * Creates {@link CohereRerankServiceSettings} from a map of settings.
     *
     * @param map     the map to parse
     * @param context the context in which the parsing is done
     * @return the created {@link CohereRerankServiceSettings}
     */
    public static CohereRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return CohereCommonServiceSettings.fromMap(map, context, parser);
    }

    private final CohereCommonServiceSettings commonSettings;

    public CohereRerankServiceSettings(CohereCommonServiceSettings commonSettings) {
        this.commonSettings = Objects.requireNonNull(commonSettings);
    }

    public CohereRerankServiceSettings(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR) == false) {
            // Old format: uri, modelId, rateLimitSettings, [apiVersion]
            var uri = createOptionalUri(in.readOptionalString());
            var modelId = in.readOptionalString();
            var rateLimitSettings = new RateLimitSettings(in);
            var apiVersion = in.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)
                ? in.readEnum(CohereCommonServiceSettings.CohereApiVersion.class)
                : CohereCommonServiceSettings.CohereApiVersion.V1;
            this.commonSettings = new CohereCommonServiceSettings(uri, modelId, rateLimitSettings, apiVersion);
        } else {
            this.commonSettings = new CohereCommonServiceSettings(in);
        }
    }

    @Override
    public CohereCommonServiceSettings commonSettings() {
        return commonSettings;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public CohereRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse Cohere rerank service settings update", e);
        }
    }

    public RateLimitSettings rateLimitSettings() {
        return commonSettings.rateLimitSettings();
    }

    public CohereCommonServiceSettings.CohereApiVersion apiVersion() {
        return commonSettings.apiVersion();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        commonSettings.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        commonSettings.toXContentFragmentOfExposedFields(builder, params);
        return builder;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ML_INFERENCE_COHERE_SERVICE_SETTINGS_REFACTOR) == false) {
            out.writeOptionalString(commonSettings.uri() != null ? commonSettings.uri().toString() : null);
            out.writeOptionalString(commonSettings.modelId());
            commonSettings.rateLimitSettings().writeTo(out);
            if (out.getTransportVersion().supports(ML_INFERENCE_COHERE_API_VERSION)) {
                out.writeEnum(commonSettings.apiVersion());
            }
        } else {
            commonSettings.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        CohereRerankServiceSettings that = (CohereRerankServiceSettings) object;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static class Update extends CohereCommonServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            CohereCommonServiceSettings.declareCommonUpdatableFields(PARSER);
        }

        public CohereRerankServiceSettings mergeInto(CohereRerankServiceSettings existing) {
            return new CohereRerankServiceSettings(existing.commonSettings().update(this));
        }
    }
}
