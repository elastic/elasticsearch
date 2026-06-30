/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.rerank;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAICommonServiceSettings;
import org.elasticsearch.xpack.inference.services.jinaai.JinaAIRateLimitServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class JinaAIRerankServiceSettings extends FilteredXContentObject implements ServiceSettings, JinaAIRateLimitServiceSettings {
    public static final String NAME = "jinaai_rerank_service_settings";

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
        JinaAICommonServiceSettings.declareCommonFields(parser);
        return parser;
    }

    public static JinaAIRerankServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return JinaAICommonServiceSettings.fromMap(map, context, parser);
    }

    private final JinaAICommonServiceSettings commonSettings;

    public JinaAIRerankServiceSettings(JinaAICommonServiceSettings commonSettings) {
        this.commonSettings = commonSettings;
    }

    public JinaAIRerankServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new JinaAICommonServiceSettings(in);
    }

    public JinaAICommonServiceSettings getCommonSettings() {
        return commonSettings;
    }

    @Override
    public JinaAIRerankServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse JinaAI rerank service settings update", e);
        }
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public RateLimitSettings rateLimitSettings() {
        return commonSettings.rateLimitSettings();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        commonSettings.toXContentFragmentOfExposedFields(builder, params);

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
        commonSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JinaAIRerankServiceSettings that = (JinaAIRerankServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings);
    }

    /**
     * Builds a {@link JinaAIRerankServiceSettings} from the common JinaAI fields. Rerank adds no settings of its own.
     */
    static class Builder extends JinaAICommonServiceSettings.Builder<JinaAIRerankServiceSettings> {

        Builder(ConfigurationParseContext context) {
            super(context);
        }

        @Override
        protected JinaAIRerankServiceSettings build(JinaAICommonServiceSettings commonSettings) {
            return new JinaAIRerankServiceSettings(commonSettings);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code rate_limit} field. Including any immutable field (such as
     * {@code model_id}) causes the strict parser to reject the request.
     */
    private static class Update extends JinaAICommonServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = new ObjectParser<>(ModelConfigurations.SERVICE_SETTINGS, Update::new);

        static {
            JinaAICommonServiceSettings.declareCommonUpdatableFields(PARSER);
        }

        public JinaAIRerankServiceSettings mergeInto(JinaAIRerankServiceSettings existing) {
            return new JinaAIRerankServiceSettings(existing.getCommonSettings().update(this));
        }
    }
}
