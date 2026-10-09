/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.sparse;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchServiceSettings;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Settings for the AlibabaCloud AI Search sparse embeddings task. Wraps the {@link AlibabaCloudSearchServiceSettings} common to every
 * AlibabaCloud AI Search task; sparse embeddings adds no settings of its own.
 */
public class AlibabaCloudSearchSparseServiceSettings implements ServiceSettings {
    public static final String NAME = "alibabacloud_search_sparse_embeddings_service_settings";

    private static final ObjectParser<Builder, ConfigurationParseContext> REQUEST_PARSER = createParser(false);
    private static final ObjectParser<Builder, ConfigurationParseContext> PERSISTENT_PARSER = createParser(true);

    /**
     * Creates an {@link ObjectParser} for the AlibabaCloud AI Search sparse embeddings service settings.
     *
     * @param ignoreUnknownFields whether the parser should tolerate unknown fields. This is {@code false} for request parsing (so that
     *                            unexpected fields are rejected) and {@code true} for persisted configuration (so that fields written by
     *                            other versions are tolerated).
     * @return the parser
     */
    static ObjectParser<Builder, ConfigurationParseContext> createParser(boolean ignoreUnknownFields) {
        return AlibabaCloudSearchServiceSettings.buildCommonParser(ignoreUnknownFields, Builder::new);
    }

    public static AlibabaCloudSearchSparseServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        var parser = context == ConfigurationParseContext.REQUEST ? REQUEST_PARSER : PERSISTENT_PARSER;
        return AlibabaCloudSearchServiceSettings.fromMap(map, context, parser);
    }

    /**
     * Builds an {@link AlibabaCloudSearchSparseServiceSettings} from the common AlibabaCloud AI Search fields, enforcing that the
     * required fields are present.
     */
    public static class Builder extends AlibabaCloudSearchServiceSettings.Builder<AlibabaCloudSearchSparseServiceSettings> {

        @Override
        protected AlibabaCloudSearchSparseServiceSettings build(AlibabaCloudSearchServiceSettings commonSettings) {
            return new AlibabaCloudSearchSparseServiceSettings(commonSettings);
        }
    }

    private final AlibabaCloudSearchServiceSettings commonSettings;

    public AlibabaCloudSearchSparseServiceSettings(AlibabaCloudSearchServiceSettings commonSettings) {
        this.commonSettings = commonSettings;
    }

    public AlibabaCloudSearchSparseServiceSettings(StreamInput in) throws IOException {
        this.commonSettings = new AlibabaCloudSearchServiceSettings(in);
    }

    public AlibabaCloudSearchServiceSettings getCommonSettings() {
        return commonSettings;
    }

    @Override
    public String modelId() {
        return commonSettings.modelId();
    }

    @Override
    public AlibabaCloudSearchSparseServiceSettings updateServiceSettings(Map<String, Object> serviceSettings) {
        try (var xParser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, serviceSettings)) {
            return Update.PARSER.apply(xParser, null).mergeInto(this);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse AlibabaCloud AI Search sparse embeddings service settings update", e);
        }
    }

    /**
     * Parses an update request, which may only contain the mutable {@code http_schema} and {@code rate_limit} fields. Including any
     * immutable field (such as {@code service_id}, {@code host} or {@code workspace}) causes the strict parser to reject the request.
     */
    private static class Update extends AlibabaCloudSearchServiceSettings.CommonUpdate {

        private static final ObjectParser<Update, Void> PARSER = AlibabaCloudSearchServiceSettings.buildCommonUpdateParser(Update::new);

        public AlibabaCloudSearchSparseServiceSettings mergeInto(AlibabaCloudSearchSparseServiceSettings existing) {
            return new AlibabaCloudSearchSparseServiceSettings(mergedCommonSettings(existing.getCommonSettings()));
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        commonSettings.toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
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
        AlibabaCloudSearchSparseServiceSettings that = (AlibabaCloudSearchSparseServiceSettings) o;
        return Objects.equals(commonSettings, that.commonSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commonSettings);
    }
}
