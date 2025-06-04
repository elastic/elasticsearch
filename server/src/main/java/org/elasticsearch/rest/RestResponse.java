/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.exception.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;
import static org.elasticsearch.rest.RestController.ELASTIC_PRODUCT_HTTP_HEADER;
import static org.elasticsearch.rest.RestController.ERROR_TRACE_DEFAULT;

public final class RestResponse implements Releasable {

    public static final String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";
    public static final Set<String> RESPONSE_PARAMS = Set.of("error_trace");

    static final String STATUS = "status";

    private static final Logger SUPPRESSED_ERROR_LOGGER = LogManager.getLogger("rest.suppressed");
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(AbstractRestChannel.class);

    private final RestStatus status;

    @Nullable
    private final BytesReference content;

    @Nullable
    private final ChunkedRestResponseBodyPart chunkedResponseBody;
    private final String responseMediaType;
    private Map<String, List<String>> customHeaders;

    @Nullable
    private final Releasable releasable;

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
        this(status, responseMediaType, content, null, null);
    }

    private RestResponse(RestStatus status, String responseMediaType, BytesReference content, @Nullable Releasable releasable) {
        this(status, responseMediaType, content, null, releasable);
    }

    public static RestResponse chunked(RestStatus restStatus, ChunkedRestResponseBodyPart content, @Nullable Releasable releasable) {
        if (content.isPartComplete()) {
            assert content.isLastPart() : "response with continuations must have at least one (possibly-empty) chunk in each part";
            return new RestResponse(restStatus, content.getResponseContentTypeString(), BytesArray.EMPTY, releasable);
        } else {
            return new RestResponse(restStatus, content.getResponseContentTypeString(), null, content, releasable);
        }
    }

    /**
     * Creates a binary response.
     */
    private RestResponse(
        RestStatus status,
        String responseMediaType,
        @Nullable BytesReference content,
        @Nullable ChunkedRestResponseBodyPart chunkedResponseBody,
        @Nullable Releasable releasable
    ) {
        this.status = status;
        this.content = content;
        this.responseMediaType = responseMediaType;
        this.chunkedResponseBody = chunkedResponseBody;
        this.releasable = releasable;
        assert (content == null) != (chunkedResponseBody == null);
    }

    public RestResponse(RestChannel channel, Exception e) throws IOException {
        this(channel, ExceptionsHelper.status(e), e);
    }

    public RestResponse(RestChannel channel, RestStatus status, Exception e) throws IOException {
        this.status = status;
        ToXContent.Params params = channel.request();
        if (e != null) {
            Supplier<?> messageSupplier = () -> String.format(
                Locale.ROOT,
                "path: %s, params: %s, status: %d",
                channel.request().rawPath(),
                channel.request().params(),
                status.getStatus()
            );
            if (status.getStatus() < 500 || ExceptionsHelper.isNodeOrShardUnavailableTypeException(e)) {
                SUPPRESSED_ERROR_LOGGER.debug(messageSupplier, e);
            } else {
                SUPPRESSED_ERROR_LOGGER.warn(messageSupplier, e);
            }
        }
        // if "error_trace" is turned on in the request, we want to render it in the rest response
        // for that the REST_EXCEPTION_SKIP_STACK_TRACE flag that if "true" omits the stack traces is
        // switched in the xcontent rendering parameters.
        // For authorization problems (RestStatus.UNAUTHORIZED) we don't want to do this since this could
        // leak information to the caller who is unauthorized to make this call
        if (params.paramAsBoolean("error_trace", ERROR_TRACE_DEFAULT) && status != RestStatus.UNAUTHORIZED) {
            params = new ToXContent.DelegatingMapParams(singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"), params);
        }

        if (channel.request().getRestApiVersion() == RestApiVersion.V_8 && channel.detailedErrorsEnabled() == false) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                "http_detailed_errors",
                "The JSON format of non-detailed errors has changed in Elasticsearch 9.0 to match the JSON structure"
                    + " used for detailed errors."
            );
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
        this.releasable = null;
    }

    public String contentType() {
        return this.responseMediaType;
    }

    @Nullable
    public BytesReference content() {
        return this.content;
    }

    @Nullable
    public ChunkedRestResponseBodyPart chunkedContent() {
        return chunkedResponseBody;
    }

    public boolean isChunked() {
        return chunkedResponseBody != null;
    }

    public RestStatus status() {
        return this.status;
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

    @Override
    public void close() {
        Releasables.closeExpectNoException(releasable);
    }
}
