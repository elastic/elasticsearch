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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.bootstrap.Elasticsearch;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;


public class BytesRestResponse extends RestResponse {

    public static final String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";

    private final RestStatus status;
    private final BytesReference content;
    private final String contentType;

    public BytesRestResponse(RestStatus status) {
        this(status, TEXT_CONTENT_TYPE, BytesArray.EMPTY);
    }

    /**
     * Creates a new response based on {@link XContentBuilder}.
     */
    public BytesRestResponse(RestStatus status, XContentBuilder builder) {
        this(status, builder.contentType().restContentType(), builder.bytes());
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

    public BytesRestResponse(RestChannel channel, Throwable t) throws IOException {
        this(channel, ExceptionsHelper.status(t), t);
    }

    public BytesRestResponse(RestChannel channel, RestStatus status, Throwable t) throws IOException {
        this.status = status;
        if (channel.request().method() == RestRequest.Method.HEAD) {
            this.content = BytesArray.EMPTY;
            this.contentType = TEXT_CONTENT_TYPE;
        } else {
            XContentBuilder builder = convert(channel, status, t);
            this.content = builder.bytes();
            this.contentType = builder.contentType().restContentType();
        }
        if (t instanceof ElasticsearchException) {
            copyHeaders(((ElasticsearchException) t));
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

    private static final ESLogger SUPPRESSED_ERROR_LOGGER = ESLoggerFactory.getLogger("rest.suppressed");

    private static XContentBuilder convert(RestChannel channel, RestStatus status, Throwable t) throws IOException {
        XContentBuilder builder = channel.newErrorBuilder().startObject();
        if (t == null) {
            builder.field("error", "unknown");
        } else if (channel.detailedErrorsEnabled()) {
            final ToXContent.Params params;
            if (channel.request().paramAsBoolean("error_trace", !ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE_DEFAULT)) {
                params =  new ToXContent.DelegatingMapParams(Collections.singletonMap(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"), channel.request());
            } else {
                SUPPRESSED_ERROR_LOGGER.info("{} Params: {}", t, channel.request().path(), channel.request().params());
                params = channel.request();
            }
            builder.field("error");
            builder.startObject();
            final ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(t);
            builder.field("root_cause");
            builder.startArray();
            for (ElasticsearchException rootCause : rootCauses){
                builder.startObject();
                rootCause.toXContent(builder, new ToXContent.DelegatingMapParams(Collections.singletonMap(ElasticsearchException.REST_EXCEPTION_SKIP_CAUSE, "true"), params));
                builder.endObject();
            }
            builder.endArray();

            ElasticsearchException.toXContent(builder, params, t);
            builder.endObject();
        } else {
            builder.field("error", simpleMessage(t));
        }
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
