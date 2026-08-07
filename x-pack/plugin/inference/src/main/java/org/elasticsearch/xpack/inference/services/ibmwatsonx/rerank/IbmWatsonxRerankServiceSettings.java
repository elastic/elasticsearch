/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.ibmwatsonx.IbmWatsonxServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.createUri;

/**
 * Represents the settings for an IBM watsonx rerank service. Extends {@link IbmWatsonxServiceSettings}, which carries the endpoint
 * URL, API version, model ID, project ID, and rate limit settings shared across all IBM watsonx tasks. Rerank adds no settings of its
 * own.
 */
public class IbmWatsonxRerankServiceSettings extends IbmWatsonxServiceSettings {
    public static final String NAME = "ibm_watsonx_rerank_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the IBM watsonx rerank service settings.
     *
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions are tolerated).
     * @return the parser
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        ObjectParser<Builder, ConfigurationParseContext> parser = new ObjectParser<>(
            ModelConfigurations.SERVICE_SETTINGS,
            ignoreUnknownFields,
            Builder::new
        );
        IbmWatsonxServiceSettings.declareCommonFields(parser);
        return parser;
    }

    public static IbmWatsonxRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return IbmWatsonxServiceSettings.fromMap(map, context, parser);
    }

    public IbmWatsonxRerankServiceSettings(
        URI uri,
        String apiVersion,
        String modelId,
        String projectId,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        super(uri, apiVersion, modelId, projectId, rateLimitSettings);
    }

    public IbmWatsonxRerankServiceSettings(StreamInput in) throws IOException {
        super(createUri(in.readString()), in.readString(), in.readString(), in.readString(), new RateLimitSettings(in));
    }

    @Override
    public IbmWatsonxRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse IBM watsonx rerank service settings update", e);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uri().toString());
        out.writeString(apiVersion());
        out.writeString(modelId());
        out.writeString(projectId());
        rateLimitSettings().writeTo(out);
    }

    /**
     * Builds an {@link IbmWatsonxRerankServiceSettings} from the common IBM watsonx fields, enforcing that the required {@code url},
     * {@code api_version}, {@code model_id}, and {@code project_id} fields are present.
     */
    public static class Builder extends IbmWatsonxServiceSettings.Builder<IbmWatsonxRerankServiceSettings> {

        @Override
        protected IbmWatsonxRerankServiceSettings build(
            URI uri,
            String apiVersion,
            String modelId,
            String projectId,
            RateLimitSettings rateLimitSettings
        ) {
            return new IbmWatsonxRerankServiceSettings(uri, apiVersion, modelId, projectId, rateLimitSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code url}, {@code api_version}, {@code model_id}, or {@code project_id}) causes the strict parser to reject the request.
     */
    private static class Update extends IbmWatsonxServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            IbmWatsonxServiceSettings.declareCommonUpdatableFields(PARSER);
        }

        public IbmWatsonxRerankServiceSettings mergeInto(IbmWatsonxRerankServiceSettings existing) {
            return new IbmWatsonxRerankServiceSettings(
                existing.uri(),
                existing.apiVersion(),
                existing.modelId(),
                existing.projectId(),
                mergedRateLimitSettings(existing)
            );
        }
    }
}
