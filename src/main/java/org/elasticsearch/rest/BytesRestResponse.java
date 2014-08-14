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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.ExceptionsHelper.detailedMessage;

public class BytesRestResponse extends RestResponse {

    public static final String TEXT_CONTENT_TYPE = "text/plain; charset=UTF-8";

    private final RestStatus status;
    private final BytesReference content;
    private final boolean contentThreadSafe;
    private final String contentType;

    public BytesRestResponse(RestStatus status) {
        this(status, TEXT_CONTENT_TYPE, BytesArray.EMPTY, true);
    }

    /**
     * Creates a new response based on {@link XContentBuilder}.
     */
    public BytesRestResponse(RestStatus status, XContentBuilder builder) {
        this(status, builder.contentType().restContentType(), builder.bytes(), true);
    }

    /**
     * Creates a new plain text response.
     */
    public BytesRestResponse(RestStatus status, String content) {
        this(status, TEXT_CONTENT_TYPE, new BytesArray(content), true);
    }

    /**
     * Creates a new plain text response.
     */
    public BytesRestResponse(RestStatus status, String contentType, String content) {
        this(status, contentType, new BytesArray(content), true);
    }

    /**
     * Creates a binary response.
     */
    public BytesRestResponse(RestStatus status, String contentType, byte[] content) {
        this(status, contentType, new BytesArray(content), true);
    }

    /**
     * Creates a binary response.
     */
    public BytesRestResponse(RestStatus status, String contentType, BytesReference content, boolean contentThreadSafe) {
        this.status = status;
        this.content = content;
        this.contentThreadSafe = contentThreadSafe;
        this.contentType = contentType;
    }

    public BytesRestResponse(RestChannel channel, Throwable t) throws IOException {
        this(channel, ((t instanceof ElasticsearchException) ? ((ElasticsearchException) t).status() : RestStatus.INTERNAL_SERVER_ERROR), t);
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
        if (t instanceof HasRestHeaders) {
            addHeaders(((HasRestHeaders) t).getHeaders());
        }
        this.contentThreadSafe = true;
    }

    @Override
    public String contentType() {
        return this.contentType;
    }

    @Override
    public boolean contentThreadSafe() {
        return this.contentThreadSafe;
    }

    @Override
    public BytesReference content() {
        return this.content;
    }

    @Override
    public RestStatus status() {
        return this.status;
    }

    private static XContentBuilder convert(RestChannel channel, RestStatus status, Throwable t) throws IOException {
        XContentBuilder builder = channel.newBuilder().startObject()
                .field("error", detailedMessage(t))
                .field("status", status.getStatus());
        if (t != null && channel.request().paramAsBoolean("error_trace", false)) {
            builder.startObject("error_trace");
            boolean first = true;
            int counter = 0;
            while (t != null) {
                // bail if there are more than 10 levels, becomes useless really...
                if (counter++ > 10) {
                    break;
                }
                if (!first) {
                    builder.startObject("cause");
                }
                buildThrowable(t, builder);
                if (!first) {
                    builder.endObject();
                }
                t = t.getCause();
                first = false;
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private static void buildThrowable(Throwable t, XContentBuilder builder) throws IOException {
        builder.field("message", t.getMessage());
        for (StackTraceElement stElement : t.getStackTrace()) {
            builder.startObject("at")
                    .field("class", stElement.getClassName())
                    .field("method", stElement.getMethodName());
            if (stElement.getFileName() != null) {
                builder.field("file", stElement.getFileName());
            }
            if (stElement.getLineNumber() >= 0) {
                builder.field("line", stElement.getLineNumber());
            }
            builder.endObject();
        }
    }
}