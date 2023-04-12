/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;
import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.rest.RestController.ELASTIC_PRODUCT_HTTP_HEADER;

public class RestResponse {

    public static final String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";

    private static final String STATUS = "status";

    private static final Logger SUPPRESSED_ERROR_LOGGER = LogManager.getLogger("rest.suppressed");

    private final RestStatus status;

    @Nullable
    private final BytesReference content;

    @Nullable
    private final ChunkedRestResponseBody chunkedResponseBody;
    private final String responseMediaType;
    private Map<String, List<String>> customHeaders;

    /**
     * Creates a new response based on {@link XContentBuilder}.
     */
    public RestResponse(RestStatus status, XContentBuilder builder) {
        this(status, builder.getResponseContentTypeString(), BytesReference.bytes(builder));
    }

    /**
     * Creates a new plain text response.
     */
    public RestResponse(RestStatus status, String content) {
        this(status, TEXT_CONTENT_TYPE, new BytesArray(content));
    }

    /**
     * Creates a new plain text response.
     */
    public RestResponse(RestStatus status, String responseMediaType, String content) {
        this(status, responseMediaType, new BytesArray(content));
    }

    public RestResponse(RestStatus status, String responseMediaType, BytesReference content) {
        this(status, responseMediaType, content, null);
    }

    public RestResponse(RestStatus status, ChunkedRestResponseBody content) {
        this(status, content.getResponseContentTypeString(), null, content);
    }

    /**
     * Creates a binary response.
     */
    private RestResponse(
        RestStatus status,
        String responseMediaType,
        @Nullable BytesReference content,
        @Nullable ChunkedRestResponseBody chunkedResponseBody
    ) {
        this.status = status;
        this.content = content;
        this.responseMediaType = responseMediaType;
        this.chunkedResponseBody = chunkedResponseBody;
        assert (content == null) != (chunkedResponseBody == null);
    }

    public RestResponse(RestChannel channel, Exception e) throws IOException {
        this(channel, ExceptionsHelper.status(e), e);
    }

    public RestResponse(RestChannel channel, RestStatus status, Exception e) throws IOException {
        this.status = status;
        ToXContent.Params params = paramsFromRequest(channel.request());
        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) && e != null) {
            // log exception only if it is not returned in the response
            Supplier<?> messageSupplier = () -> String.format(
                Locale.ROOT,
                "path: %s, params: %s",
                channel.request().rawPath(),
                channel.request().params()
            );
            if (status.getStatus() < 500) {
                SUPPRESSED_ERROR_LOGGER.debug(messageSupplier, e);
            } else {
                SUPPRESSED_ERROR_LOGGER.warn(messageSupplier, e);
            }
        }
        try (XContentBuilder builder = channel.newErrorBuilder()) {
            build(builder, params, status, channel.detailedErrorsEnabled(), e);
            this.content = BytesReference.bytes(builder);
            this.responseMediaType = builder.getResponseContentTypeString();
        }
        if (e instanceof ElasticsearchException) {
            copyHeaders(((ElasticsearchException) e));
        }
        this.chunkedResponseBody = null;
    }

    public String contentType() {
        return this.responseMediaType;
    }

    @Nullable
    public BytesReference content() {
        return this.content;
    }

    @Nullable
    public ChunkedRestResponseBody chunkedContent() {
        return chunkedResponseBody;
    }

    public boolean isChunked() {
        return chunkedResponseBody != null;
    }

    public RestStatus status() {
        return this.status;
    }

    private ToXContent.Params paramsFromRequest(RestRequest restRequest) {
        ToXContent.Params params = restRequest;
        if (params.paramAsBoolean("error_trace", REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT == false) && skipStackTrace() == false) {
            params = new ToXContent.DelegatingMapParams(singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"), params);
        }
        return params;
    }

    protected boolean skipStackTrace() {
        return status() == RestStatus.UNAUTHORIZED;
    }

    private static void build(
        XContentBuilder builder,
        ToXContent.Params params,
        RestStatus status,
        boolean detailedErrorsEnabled,
        Exception e
    ) throws IOException {
        builder.startObject();
        ElasticsearchException.generateFailureXContent(builder, params, e, detailedErrorsEnabled);
        builder.field(STATUS, status.getStatus());
        builder.endObject();
    }

    static RestResponse createSimpleErrorResponse(RestChannel channel, RestStatus status, String errorMessage) throws IOException {
        return new RestResponse(
            status,
            channel.newErrorBuilder().startObject().field("error", errorMessage).field("status", status.getStatus()).endObject()
        );
    }

    public static ElasticsearchStatusException errorFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        ElasticsearchException exception = null;
        RestStatus status = null;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }
            if (STATUS.equals(currentFieldName)) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    status = RestStatus.fromCode(parser.intValue());
                }
            } else {
                exception = ElasticsearchException.failureFromXContent(parser);
            }
        }

        if (exception == null) {
            throw new IllegalStateException("Failed to parse elasticsearch status exception: no exception was found");
        }

        ElasticsearchStatusException result = new ElasticsearchStatusException(exception.getMessage(), status, exception.getCause());
        for (String header : exception.getHeaderKeys()) {
            result.addHeader(header, exception.getHeader(header));
        }
        for (String metadata : exception.getMetadataKeys()) {
            result.addMetadata(metadata, exception.getMetadata(metadata));
        }
        return result;
    }

    public void copyHeaders(ElasticsearchException ex) {
        Set<String> headerKeySet = ex.getHeaderKeys();
        if (customHeaders == null) {
            customHeaders = Maps.newMapWithExpectedSize(headerKeySet.size());
        }
        for (String key : headerKeySet) {
            customHeaders.computeIfAbsent(key, k -> new ArrayList<>()).addAll(ex.getHeader(key));
        }
    }

    /**
     * Add a custom header.
     */
    public void addHeader(String name, String value) {
        if (customHeaders == null) {
            customHeaders = Maps.newMapWithExpectedSize(2);
        }
        customHeaders.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
    }

    /**
     * Returns custom headers that have been added. This method should not be used to mutate headers.
     */
    public Map<String, List<String>> getHeaders() {
        return Objects.requireNonNullElse(customHeaders, Map.of());
    }

    public Map<String, List<String>> filterHeaders(Map<String, List<String>> headers) {
        if (status() == RestStatus.UNAUTHORIZED || status() == RestStatus.FORBIDDEN) {
            if (headers.containsKey("Warning")) {
                headers = Maps.copyMapWithRemovedEntry(headers, "Warning");
            }
            if (headers.containsKey(ELASTIC_PRODUCT_HTTP_HEADER)) {
                headers = Maps.copyMapWithRemovedEntry(headers, ELASTIC_PRODUCT_HTTP_HEADER);
            }
        }
        return headers;
    }
}
