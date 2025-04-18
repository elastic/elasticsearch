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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
import org.elasticsearch.xpack.inference.services.custom.response.BaseCustomResponseParser;
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
    public static final String REQUEST_CONTENT = "content";
    public static final String RESPONSE = "response";
    public static final String JSON_PARSER = "json_parser";
    public static final String ERROR_PARSER = "error_parser";

    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CustomServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context, TaskType taskType) {
        ValidationException validationException = new ValidationException();

        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

        String url = extractRequiredString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        Map<String, Object> headers = extractOptionalMap(map, HEADERS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        removeNullValues(headers);
        var stringHeaders = validateMapStringValues(headers, HEADERS, validationException, false);

        Map<String, Object> requestBodyMap = extractRequiredMap(map, REQUEST, ModelConfigurations.SERVICE_SETTINGS, validationException);

        String requestContentString = extractRequiredString(
            Objects.requireNonNullElse(requestBodyMap, new HashMap<>()),
            REQUEST_CONTENT,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        Map<String, Object> responseParserMap = extractRequiredMap(
            map,
            RESPONSE,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        Map<String, Object> jsonParserMap = extractRequiredMap(
            Objects.requireNonNullElse(responseParserMap, new HashMap<>()),
            JSON_PARSER,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var responseJsonParser = extractResponseParser(taskType, jsonParserMap, validationException);

        Map<String, Object> errorParserMap = extractRequiredMap(
            Objects.requireNonNullElse(responseParserMap, new HashMap<>()),
            ERROR_PARSER,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        var errorParser = ErrorResponseParser.fromMap(errorParserMap, validationException);

        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            CustomService.NAME,
            context
        );

        if (requestBodyMap == null || responseParserMap == null || jsonParserMap == null || errorParserMap == null) {
            throw validationException;
        }

        throwIfNotEmptyMap(requestBodyMap, REQUEST, NAME);
        throwIfNotEmptyMap(jsonParserMap, JSON_PARSER, NAME);
        throwIfNotEmptyMap(responseParserMap, RESPONSE, NAME);
        throwIfNotEmptyMap(errorParserMap, ERROR_PARSER, NAME);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CustomServiceSettings(
            similarity,
            dims,
            maxInputTokens,
            url,
            stringHeaders,
            requestContentString,
            responseJsonParser,
            rateLimitSettings,
            errorParser
        );
    }

    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final String url;
    private final Map<String, String> headers;
    private final String requestContentString;
    private final CustomResponseParser responseJsonParser;
    private final RateLimitSettings rateLimitSettings;
    private final ErrorResponseParser errorParser;

    public CustomServiceSettings(
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        String url,
        @Nullable Map<String, String> headers,
        String requestContentString,
        CustomResponseParser responseJsonParser,
        @Nullable RateLimitSettings rateLimitSettings,
        ErrorResponseParser errorParser
    ) {
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.url = Objects.requireNonNull(url);
        this.headers = Collections.unmodifiableMap(Objects.requireNonNullElse(headers, Map.of()));
        this.requestContentString = Objects.requireNonNull(requestContentString);
        this.responseJsonParser = Objects.requireNonNull(responseJsonParser);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
        this.errorParser = Objects.requireNonNull(errorParser);
    }

    public CustomServiceSettings(StreamInput in) throws IOException {
        similarity = in.readOptionalEnum(SimilarityMeasure.class);
        dimensions = in.readOptionalVInt();
        maxInputTokens = in.readOptionalVInt();
        url = in.readString();
        headers = in.readImmutableMap(StreamInput::readString);
        requestContentString = in.readString();
        responseJsonParser = in.readNamedWriteable(BaseCustomResponseParser.class);
        rateLimitSettings = new RateLimitSettings(in);
        errorParser = new ErrorResponseParser(in);
    }

    @Override
    public SimilarityMeasure similarity() {
        return similarity;
    }

    @Override
    public Integer dimensions() {
        return dimensions;
    }

    @Override
    public DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    public Integer getMaxInputTokens() {
        return maxInputTokens;
    }

    public String getUrl() {
        return url;
    }

    public Map<String, String> getHeaders() {
        return headers;
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
        if (similarity != null) {
            builder.field(SIMILARITY, similarity);
        }
        if (dimensions != null) {
            builder.field(DIMENSIONS, dimensions);
        }
        if (maxInputTokens != null) {
            builder.field(MAX_INPUT_TOKENS, maxInputTokens);
        }
        builder.field(URL, url);

        if (headers.isEmpty() == false) {
            builder.field(HEADERS, headers);
        }

        builder.startObject(REQUEST);
        {
            builder.field(REQUEST_CONTENT, requestContentString);
        }
        builder.endObject();

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
        return TransportVersions.ADD_INFERENCE_CUSTOM_MODEL;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalEnum(similarity);
        out.writeOptionalVInt(dimensions);
        out.writeOptionalVInt(maxInputTokens);
        out.writeString(url);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
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
        return Objects.equals(similarity, that.similarity)
            && Objects.equals(dimensions, that.dimensions)
            && Objects.equals(maxInputTokens, that.maxInputTokens)
            && Objects.equals(url, that.url)
            && Objects.equals(headers, that.headers)
            && Objects.equals(requestContentString, that.requestContentString)
            && Objects.equals(responseJsonParser, that.responseJsonParser)
            && Objects.equals(rateLimitSettings, that.rateLimitSettings)
            && Objects.equals(errorParser, that.errorParser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            similarity,
            dimensions,
            maxInputTokens,
            url,
            headers,
            requestContentString,
            responseJsonParser,
            rateLimitSettings,
            errorParser
        );
    }

    @Override
    public String modelId() {
        // returning null because the model id is embedded in the url
        return null;
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
            case TEXT_EMBEDDING -> TextEmbeddingResponseParser.fromMap(responseParserMap, validationException);
            case SPARSE_EMBEDDING -> SparseEmbeddingResponseParser.fromMap(responseParserMap, validationException);
            case RERANK -> RerankResponseParser.fromMap(responseParserMap, validationException);
            case COMPLETION -> CompletionResponseParser.fromMap(responseParserMap, validationException);
            default -> throw new IllegalArgumentException(
                Strings.format("Invalid task type received [%s] while constructing response parser", taskType)
            );
        };
    }
}
