/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceFields.URL;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertToUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.createOptionalUri;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;

public class AlibabaCloudSearchServiceSettings extends FilteredXContentObject
    implements
        ServiceSettings,
        AlibabaCloudSearchRateLimitServiceSettings {

    public static final String NAME = "alibabacloud_search_service_settings";
    public static final String MODEL_ID = "service_id";
    public static final String HOST = "host";
    public static final String WORKSPACE_NAME = "workspace";
    public static final String HTTP_SCHEMA_NAME = "http_schema";

    // todo: 确认开放搜索平台的ratelimt
    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static AlibabaCloudSearchServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
        ValidationException validationException = new ValidationException();

        String url = extractOptionalString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);
        URI uri = convertToUri(url, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String modelId = extractRequiredString(map, MODEL_ID, ModelConfigurations.SERVICE_SETTINGS, validationException);
        String host = extractRequiredString(map, HOST, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var workspaceName = extractRequiredString(map, WORKSPACE_NAME, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var httpSchema = extractOptionalString(map, HTTP_SCHEMA_NAME, ModelConfigurations.SERVICE_SETTINGS, validationException);

        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            AlibabaCloudSearchService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AlibabaCloudSearchServiceSettings(
            uri,
            similarity,
            dims,
            maxInputTokens,
            modelId,
            host,
            workspaceName,
            httpSchema,
            rateLimitSettings
        );
    }

    private final URI uri;
    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final String modelId;
    private final String host;
    private final String workspaceName;
    private final String httpSchema;
    private final RateLimitSettings rateLimitSettings;

    public AlibabaCloudSearchServiceSettings(
        @Nullable URI uri,
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        String modelId,
        String host,
        String workspaceName,
        @Nullable String httpSchema,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.uri = uri;
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.modelId = modelId;
        this.host = host;
        this.workspaceName = workspaceName;
        this.httpSchema = httpSchema;
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public AlibabaCloudSearchServiceSettings(StreamInput in) throws IOException {
        uri = createOptionalUri(in.readOptionalString());
        similarity = in.readOptionalEnum(SimilarityMeasure.class);
        dimensions = in.readOptionalVInt();
        maxInputTokens = in.readOptionalVInt();
        modelId = in.readString();
        host = in.readString();
        workspaceName = in.readString();
        httpSchema = in.readOptionalString();
        rateLimitSettings = new RateLimitSettings(in);
    }

    public URI getUri() {
        return uri;
    }

    public SimilarityMeasure getSimilarity() {
        return similarity;
    }

    public Integer getDimensions() {
        return dimensions;
    }

    public Integer getMaxInputTokens() {
        return maxInputTokens;
    }

    @Override
    public String modelId() {
        return modelId;
    }

    public String getHost() {
        return host;
    }

    public String getWorkspaceName() {
        return workspaceName;
    }

    public String getHttpSchema() {
        return httpSchema;
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        toXContentFragment(builder, params);

        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        return toXContentFragmentOfExposedFields(builder, params);
    }

    @Override
    public XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
        if (uri != null) {
            builder.field(URL, uri.toString());
        }
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        if (modelId != null) {
            builder.field(MODEL_ID, modelId);
        }
        builder.field(HOST, host);
        builder.field(WORKSPACE_NAME, workspaceName);
        if (httpSchema != null) {
            builder.field(HTTP_SCHEMA_NAME, httpSchema);
        }
        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_ALIBABACLOUD_SEARCH_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        var uriToWrite = uri != null ? uri.toString() : null;
        out.writeOptionalString(uriToWrite);
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeString(modelId);
        out.writeString(host);
        out.writeString(workspaceName);
        out.writeOptionalString(httpSchema);
        rateLimitSettings.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlibabaCloudSearchServiceSettings that = (AlibabaCloudSearchServiceSettings) o;
        return Objects.equals(uri, that.uri)
            && Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(host, that.host)
            && Objects.equals(workspaceName, that.workspaceName)
            && Objects.equals(httpSchema, that.httpSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uri, similarity, dimensions, maxInputTokens, modelId, host, workspaceName, httpSchema);
    }
}
