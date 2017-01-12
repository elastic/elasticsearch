/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.rest;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;
import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT;


public class BytesRestResponse extends RestResponse {

    public static final String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";

    private final RestStatus status;
    private final BytesReference content;
    private final String contentType;

    /**
     * Creates a new response based on {@link XContentBuilder}.
     */
    public BytesRestResponse(RestStatus status, XContentBuilder builder) {
        this(status, builder.contentType().mediaType(), builder.bytes());
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
        if (channel.request().method() == RestRequest.Method.HEAD) {
            this.content = BytesArray.EMPTY;
            this.contentType = TEXT_CONTENT_TYPE;
        } else {
            try (final XContentBuilder builder = build(channel, status, e)) {
                this.content = builder.bytes();
                this.contentType = builder.contentType().mediaType();
            }
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

    private static final Logger SUPPRESSED_ERROR_LOGGER = ESLoggerFactory.getLogger("rest.suppressed");

    private static XContentBuilder build(RestChannel channel, RestStatus status, Exception e) throws IOException {
        ToXContent.Params params = channel.request();
        if (params.paramAsBoolean("error_trace", !REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT)) {
            params =  new ToXContent.DelegatingMapParams(singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"), params);
        } else if (e != null) {
            Supplier<?> messageSupplier = () -> new ParameterizedMessage("path: {}, params: {}",
                    channel.request().rawPath(), channel.request().params());

            if (status.getStatus() < 500) {
                SUPPRESSED_ERROR_LOGGER.debug(messageSupplier, e);
            } else {
                SUPPRESSED_ERROR_LOGGER.warn(messageSupplier, e);
            }
        }

        XContentBuilder builder = channel.newErrorBuilder().startObject();
        ElasticsearchException.toXContentError(builder, params, e, channel.detailedErrorsEnabled());
        builder.field("status", status.getStatus());
        builder.endObject();
        return builder;
    }

    /*
     * Builds a simple error string from the message of the first ElasticsearchException
     */
    private static String simpleMessage(Throwable t) throws IOException {
        int counter = 0;
        Throwable next = t;
        while (next != null && counter++ < 10) {
            if (t instanceof ElasticsearchException) {
                return next.getClass().getSimpleName() + "[" + next.getMessage() + "]";
            }
            next = next.getCause();
        }

        return "No ElasticsearchException found";
    }
}
