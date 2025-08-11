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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;
import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class BytesRestResponse extends RestResponse {

    public static final String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";

    private static final String STATUS = "status";

    private static final Logger SUPPRESSED_ERROR_LOGGER = LogManager.getLogger("rest.suppressed");

    private final RestStatus status;
    private final BytesReference content;
    private final String contentType;

    /**
     * Creates a new response based on {@link XContentBuilder}.
     */
    public BytesRestResponse(RestStatus status, XContentBuilder builder) {
        this(status, builder.contentType().mediaType(), BytesReference.bytes(builder));
    }

    /**
     * Creates a new plain text response.
     */
    public BytesRestResponse(RestStatus status, String content) {
        this(status, TEXT_CONTENT_TYPE, new BytesArray(content));
    }

    /**
     * Creates a new plain text response.
     */
    public BytesRestResponse(RestStatus status, String contentType, String content) {
        this(status, contentType, new BytesArray(content));
    }

    /**
     * Creates a binary response.
     */
    public BytesRestResponse(RestStatus status, String contentType, byte[] content) {
        this(status, contentType, new BytesArray(content));
    }

    /**
     * Creates a binary response.
     */
    public BytesRestResponse(RestStatus status, String contentType, BytesReference content) {
        this.status = status;
        this.content = content;
        this.contentType = contentType;
    }

    public BytesRestResponse(RestChannel channel, Exception e) throws IOException {
        this(channel, ExceptionsHelper.status(e), e);
    }

    public BytesRestResponse(RestChannel channel, RestStatus status, Exception e) throws IOException {
        this.status = status;
        ToXContent.Params params = paramsFromRequest(channel.request());
        if (params.paramAsBoolean(REST_EXCEPTION_SKIP_STACK_TRACE, REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT) && e != null) {
            // log exception only if it is not returned in the response
            Supplier<?> messageSupplier = () -> new ParameterizedMessage(
                "path: {}, params: {}",
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
            this.contentType = builder.contentType().mediaType();
        }
        if (e instanceof ElasticsearchException) {
            copyHeaders(((ElasticsearchException) e));
        }
    }

    @Override
    public String contentType() {
        return this.contentType;
    }

    @Override
    public BytesReference content() {
        return this.content;
    }

    @Override
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

    private void build(XContentBuilder builder, ToXContent.Params params, RestStatus status, boolean detailedErrorsEnabled, Exception e)
        throws IOException {
        builder.startObject();
        ElasticsearchException.generateFailureXContent(builder, params, e, detailedErrorsEnabled);
        builder.field(STATUS, status.getStatus());
        builder.endObject();
    }

    static BytesRestResponse createSimpleErrorResponse(RestChannel channel, RestStatus status, String errorMessage) throws IOException {
        return new BytesRestResponse(
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
}
