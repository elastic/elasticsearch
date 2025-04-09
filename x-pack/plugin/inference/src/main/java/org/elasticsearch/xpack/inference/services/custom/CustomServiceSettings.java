/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
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
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class CustomServiceSettings extends FilteredXContentObject implements ServiceSettings, CustomRateLimitServiceSettings {
    public static final String NAME = "custom_service_settings";
    public static final String URL = "url";
    public static final String HEADERS = "headers";
    public static final String REQUEST = "request";
    public static final String REQUEST_CONTENT = "content";
    public static final String RESPONSE = "response";
    public static final String JSON_PARSER = "json_parser";

    public static final String TEXT_EMBEDDING_PARSER_EMBEDDINGS = "text_embeddings";

    public static final String SPARSE_EMBEDDING_RESULT = "sparse_result";
    public static final String SPARSE_RESULT_PATH = "path";
    public static final String SPARSE_RESULT_VALUE = "value";
    public static final String SPARSE_EMBEDDING_PARSER_TOKEN = "sparse_token";
    public static final String SPARSE_EMBEDDING_PARSER_WEIGHT = "sparse_weight";

    public static final String RERANK_PARSER_INDEX = "reranked_index";
    public static final String RERANK_PARSER_SCORE = "relevance_score";
    public static final String RERANK_PARSER_DOCUMENT_TEXT = "document_text";

    public static final String COMPLETION_PARSER_RESULT = "completion_result";

    private static final RateLimitSettings DEFAULT_RATE_LIMIT_SETTINGS = new RateLimitSettings(10_000);

    public static CustomServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context, TaskType taskType) {
        ValidationException validationException = new ValidationException();

        SimilarityMeasure similarity = extractSimilarity(map, ModelConfigurations.SERVICE_SETTINGS, validationException);
        Integer dims = removeAsType(map, DIMENSIONS, Integer.class);
        Integer maxInputTokens = removeAsType(map, MAX_INPUT_TOKENS, Integer.class);

        String url = extractRequiredString(map, URL, ModelConfigurations.SERVICE_SETTINGS, validationException);

        Map<String, Object> headers = extractOptionalMap(map, HEADERS, ModelConfigurations.SERVICE_SETTINGS, validationException);
        // TODO validate that values are only strings

        Map<String, Object> requestBodyMap = extractRequiredMap(map, REQUEST, ModelConfigurations.SERVICE_SETTINGS, validationException);

        if (requestBodyMap == null) {
            throw validationException;
        }

        String requestContentString = extractRequiredString(
            requestBodyMap,
            REQUEST_CONTENT,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        throwIfNotEmptyMap(requestBodyMap, NAME);

        Map<String, Object> responseParserMap = extractRequiredMap(
            map,
            RESPONSE,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        if (responseParserMap == null) {
            throw validationException;
        }

        Map<String, Object> jsonParserMap = extractRequiredMap(
            responseParserMap,
            JSON_PARSER,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        if (jsonParserMap == null) {
            throw validationException;
        }

        var responseJsonParser = extractResponseParser(taskType, jsonParserMap, validationException);
        throwIfNotEmptyMap(responseParserMap, NAME);

        RateLimitSettings rateLimitSettings = RateLimitSettings.of(
            map,
            DEFAULT_RATE_LIMIT_SETTINGS,
            validationException,
            CustomService.NAME,
            context
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CustomServiceSettings(
            similarity,
            dims,
            maxInputTokens,
            url,
            headers,
            requestContentString,
            responseJsonParser,
            rateLimitSettings
        );
    }

    private final SimilarityMeasure similarity;
    private final Integer dimensions;
    private final Integer maxInputTokens;
    private final String url;
    private final Map<String, Object> headers;
    private final String requestContentString;
    private final ResponseJsonParser responseJsonParser;
    private final RateLimitSettings rateLimitSettings;

    public CustomServiceSettings(
        @Nullable SimilarityMeasure similarity,
        @Nullable Integer dimensions,
        @Nullable Integer maxInputTokens,
        String url,
        Map<String, Object> headers,
        String requestContentString,
        ResponseJsonParser responseJsonParser,
        @Nullable RateLimitSettings rateLimitSettings
    ) {
        this.similarity = similarity;
        this.dimensions = dimensions;
        this.maxInputTokens = maxInputTokens;
        this.url = Objects.requireNonNull(url);
        this.headers = Objects.requireNonNull(headers);
        this.requestContentString = Objects.requireNonNull(requestContentString);
        this.responseJsonParser = Objects.requireNonNull(responseJsonParser);
        this.rateLimitSettings = Objects.requireNonNullElse(rateLimitSettings, DEFAULT_RATE_LIMIT_SETTINGS);
    }

    public CustomServiceSettings(StreamInput in) throws IOException {
        similarity = in.readOptionalEnum(SimilarityMeasure.class);
        dimensions = in.readOptionalVInt();
        maxInputTokens = in.readOptionalVInt();
        url = in.readString();
        if (in.readBoolean()) {
            headers = in.readGenericMap();
        } else {
            headers = null;
        }
        requestContentString = in.readString();
        if (in.readBoolean()) {
            responseJsonParser = new ResponseJsonParser(in);
        } else {
            responseJsonParser = null;
        }
        rateLimitSettings = new RateLimitSettings(in);
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

    public SimilarityMeasure getSimilarity() {
        return similarity;
    }

    public Integer getDimensions() {
        return dimensions;
    }

    public Integer getMaxInputTokens() {
        return maxInputTokens;
    }

    public String getUrl() {
        return url;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public String getRequestContentString() {
        return requestContentString;
    }

    public ResponseJsonParser getResponseJsonParser() {
        return responseJsonParser;
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

        if (headers != null) {
            builder.field(HEADERS, headers);
        }

        builder.startObject(REQUEST);
        {
            builder.field(REQUEST_CONTENT, requestContentString);
        }
        builder.endObject();

        if (responseJsonParser != null) {
            builder.startObject(RESPONSE);
            {
                responseJsonParser.toXContent(builder, params);
            }
            builder.endObject();
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
        if (headers != null) {
            out.writeBoolean(true);
            out.writeGenericMap(headers);
        } else {
            out.writeBoolean(false);
        }
        out.writeString(requestContentString);
        if (responseJsonParser != null) {
            out.writeBoolean(true);
            responseJsonParser.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        rateLimitSettings.writeTo(out);
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
            && Objects.equals(rateLimitSettings, that.rateLimitSettings);
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
            rateLimitSettings
        );
    }

    @Override
    public String modelId() {
        return CustomService.NAME; // TODO, what is required here?
    }

    private static ResponseJsonParser extractResponseParser(
        TaskType taskType,
        Map<String, Object> responseParserMap,
        ValidationException validationException
    ) {
        return new ResponseJsonParser(taskType, responseParserMap, validationException);
    }
}
