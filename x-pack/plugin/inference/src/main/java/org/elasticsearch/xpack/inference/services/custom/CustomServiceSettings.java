/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.custom.response.CompletionResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.CustomResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.ErrorResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.NoopResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.SparseEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.custom.response.TextEmbeddingResponseParser;
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceFields.DIMENSIONS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.MAX_INPUT_TOKENS;
import static org.elasticsearch.xpack.inference.services.ServiceFields.SIMILARITY;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractSimilarity;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeAsType;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeNullValues;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.validateMapStringValues;

public class CustomServiceSettings extends FilteredXContentObject implements ServiceSettings, CustomRateLimitServiceSettings {
    public static final String NAME = "custom_service_settings";
    public static final String URL = "url";
    public static final String HEADERS = "headers";
    public static final String REQUEST = "request";
    public static final String RESPONSE = "response";
    public static final String JSON_PARSER = "json_parser";
    public static final String ERROR_PARSER = "error_parser";

    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);
    private static final String RESPONSE_SCOPE = String.join(".", ModelConfigurations.SERVICE_SETTINGS, RESPONSE);

    public static CustomServiceSettings fromMap(
        Map<String, Object> map,
        ConfigurationParseContext context,
        TaskType taskType,
        String inferenceId
    ) {
        ValidationException validationException = new ValidationException();

        var textEmbeddingSettings = TextEmbeddingSettings.fromMap(map, taskType, validationException);

        String url = extractRequiredString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        var queryParams = QueryParameters.fromMap(map, validationException);

        Map<String, Object> headers = extractOptionalMap(map, HEADERS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        removeNullValues(headers);
        var stringHeaders = validateMapStringValues(headers, HEADERS, validationException, false);

        String requestContentString = extractRequiredString(map, REQUEST, ModelConfigurations.SERVICE_SETTINGS, validationException);

        Map<String, Object> responseParserMap = extractRequiredMap(
            map,
            RESPONSE,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        Map<String, Object> jsonParserMap = extractRequiredMap(
            Objects.requireNonNullElse(responseParserMap, new HashMap<>()),
            JSON_PARSER,
            RESPONSE_SCOPE,
            validationException
        );

        var responseJsonParser = extractResponseParser(taskType, jsonParserMap, validationException);

        Map<String, Object> errorParserMap = extractRequiredMap(
            Objects.requireNonNullElse(responseParserMap, new HashMap<>()),
            ERROR_PARSER,
            RESPONSE_SCOPE,
            validationException
        );

        var errorParser = ErrorResponseParser.fromMap(errorParserMap, RESPONSE_SCOPE, inferenceId, validationException);

        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            CustomService.NAME,
            context
        );

        if (responseParserMap == null || jsonParserMap == null || errorParserMap == null) {
            throw validationException;
        }

        throwIfNotEmptyMap(jsonParserMap, JSON_PARSER, NAME);
        throwIfNotEmptyMap(responseParserMap, RESPONSE, NAME);
        throwIfNotEmptyMap(errorParserMap, ERROR_PARSER, NAME);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CustomServiceSettings(
            textEmbeddingSettings,
            url,
            stringHeaders,
            queryParams,
            requestContentString,
            responseJsonParser,
            rateLimitSettings,
            errorParser
        );
    }

    public record TextEmbeddingSettings(
        @Nullable SimilarityMeasure similarityMeasure,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        @Nullable DenseVectorFieldMapper.ElementType elementType
    ) implements ToXContentFragment, Writeable {

        // This specifies float for the element type but null for all other settings
        public static final TextEmbeddingSettings DEFAULT_FLOAT = new TextEmbeddingSettings(
            null,
            null,
            null,
            DenseVectorFieldMapper.ElementType.FLOAT
        );

        // This refers to settings that are not related to the text embedding task type (all the settings should be null)
        public static final TextEmbeddingSettings NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS = new TextEmbeddingSettings(null, null, null, null);

        public static TextEmbeddingSettings fromMap(Map<String, Object> map, TaskType taskType, ValidationException validationException) {
            if (taskType != TaskType.TEXT_EMBEDDING) {
                return NON_TEXT_EMBEDDING_TASK_TYPE_SETTINGS;
            }

            SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
            Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
            Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);
            return new TextEmbeddingSettings(similarity, dims, maxInputTokens, DenseVectorFieldMapper.ElementType.FLOAT);
        }

        public TextEmbeddingSettings(StreamInput in) throws IOException {
            this(
                in.readOptionalEnum(SimilarityMeasure.class),
                in.readOptionalVInt(),
                in.readOptionalVInt(),
                in.readOptionalEnum(DenseVectorFieldMapper.ElementType.class)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalEnum(similarityMeasure);
            out.writeOptionalVInt(dimensions);
            out.writeOptionalVInt(maxInputTokens);
            out.writeOptionalEnum(elementType);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (similarityMeasure != null) {
                builder.field(SIMILARITY, similarityMeasure);
            }
            if (dimensions != null) {
                builder.field(DIMENSIONS, dimensions);
            }
            if (maxInputTokens != null) {
                builder.field(MAX_INPUT_TOKENS, maxInputTokens);
            }
            return builder;
        }
    }

    private final TextEmbeddingSettings textEmbeddingSettings;
    private final String url;
    private final Map<String, String> headers;
    private final QueryParameters queryParameters;
    private final String requestContentString;
    private final CustomResponseParser responseJsonParser;
    private final RateLimitSettings rateLimitSettings;
    private final ErrorResponseParser errorParser;

    public CustomServiceSettings(
        TextEmbeddingSettings textEmbeddingSettings,
        String url,
        @Nullable Map<String, String> headers,
        @Nullable QueryParameters queryParameters,
        String requestContentString,
        CustomResponseParser responseJsonParser,
        @Nullable RateLimitSettings rateLimitSettings,
        ErrorResponseParser errorParser
    ) {
        this.textEmbeddingSettings = Objects.requireNonNull(textEmbeddingSettings);
        this.url = Objects.requireNonNull(url);
        this.headers = Collections.unmodifiableMap(Objects.requireNonNullElse(headers, Map.of()));
        this.queryParameters = Objects.requireNonNullElse(queryParameters, QueryParameters.EMPTY);
        this.requestContentString = Objects.requireNonNull(requestContentString);
        this.responseJsonParser = Objects.requireNonNull(responseJsonParser);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        this.errorParser = Objects.requireNonNull(errorParser);
    }

    public CustomServiceSettings(StreamInput in) throws IOException {
        textEmbeddingSettings = new TextEmbeddingSettings(in);
        url = in.readString();
        headers = in.readImmutableMap(StreamInput::readString);
        queryParameters = new QueryParameters(in);
        requestContentString = in.readString();
        responseJsonParser = in.readNamedWriteable(CustomResponseParser.class);
        rateLimitSettings = new RateLimitSettings(in);
        errorParser = new ErrorResponseParser(in);
    }

    @Override
    public String modelId() {
        // returning null because the model id is embedded in the url or the request body
        return null;
    }

    @Override
    public SimilarityMeasure similarity() {
        return textEmbeddingSettings.similarityMeasure;
    }

    @Override
    public Integer dimensions() {
        return textEmbeddingSettings.dimensions;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return textEmbeddingSettings.elementType;
    }

    public Integer getMaxInputTokens() {
        return textEmbeddingSettings.maxInputTokens;
    }

    public String getUrl() {
        return url;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public QueryParameters getQueryParameters() {
        return queryParameters;
    }

    public String getRequestContentString() {
        return requestContentString;
    }

    public CustomResponseParser getResponseJsonParser() {
        return responseJsonParser;
    }

    public ErrorResponseParser getErrorParser() {
        return errorParser;
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
        textEmbeddingSettings.toXContent(builder, params);
        builder.field(URL, url);

        if (headers.isEmpty() == false) {
            builder.field(HEADERS, headers);
        }

        queryParameters.toXContent(builder, params);

        builder.field(REQUEST, requestContentString);

        builder.startObject(RESPONSE);
        {
            responseJsonParser.toXContent(builder, params);
            errorParser.toXContent(builder, params);
        }
        builder.endObject();

        rateLimitSettings.toXContent(builder, params);

        return builder;
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.INFERENCE_CUSTOM_SERVICE_ADDED_8_19;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        textEmbeddingSettings.writeTo(out);
        out.writeString(url);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        queryParameters.writeTo(out);
        out.writeString(requestContentString);
        out.writeNamedWriteable(responseJsonParser);
        rateLimitSettings.writeTo(out);
        errorParser.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomServiceSettings that = (CustomServiceSettings) o;
        return Objects.equals(textEmbeddingSettings, that.textEmbeddingSettings)
            && Objects.equals(url, that.url)
            && Objects.equals(headers, that.headers)
            && Objects.equals(queryParameters, that.queryParameters)
            && Objects.equals(requestContentString, that.requestContentString)
            && Objects.equals(responseJsonParser, that.responseJsonParser)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(errorParser, that.errorParser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            textEmbeddingSettings,
            url,
            headers,
            queryParameters,
            requestContentString,
            responseJsonParser,
            rateLimitSettings,
            errorParser
        );
    }

    private static CustomResponseParser extractResponseParser(
        TaskType taskType,
        Map<String, Object> responseParserMap,
        ValidationException validationException
    ) {
        if (responseParserMap == null) {
            return NoopResponseParser.INSTANCE;
        }

        return switch (taskType) {
            case TEXT_EMBEDDING -> TextEmbeddingResponseParser.fromMap(responseParserMap, RESPONSE_SCOPE, validationException);
            case SPARSE_EMBEDDING -> SparseEmbeddingResponseParser.fromMap(responseParserMap, RESPONSE_SCOPE, validationException);
            case RERANK -> RerankResponseParser.fromMap(responseParserMap, RESPONSE_SCOPE, validationException);
            case COMPLETION -> CompletionResponseParser.fromMap(responseParserMap, RESPONSE_SCOPE, validationException);
            default -> throw new IllegalArgumentException(
                Strings.format("Invalid task type received [%s] while constructing response parser", taskType)
            );
        };
    }
}
