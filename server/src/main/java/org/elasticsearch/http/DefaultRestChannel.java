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

package org.elasticsearch.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.tasks.Task.X_OPAQUE_ID;

/**
 * The default rest channel for incoming requests. This class implements the basic logic for sending a rest
 * response. It will set necessary headers nad ensure that bytes are released after the response is sent.
 */
public class DefaultRestChannel extends AbstractRestChannel implements RestChannel {

    static final String CLOSE = "close";
    static final String CONNECTION = "connection";
    static final String KEEP_ALIVE = "keep-alive";
    static final String CONTENT_TYPE = "content-type";
    static final String CONTENT_LENGTH = "content-length";
    static final String SET_COOKIE = "set-cookie";

    private final HttpRequest httpRequest;
    private final BigArrays bigArrays;
    private final HttpHandlingSettings settings;
    private final ThreadContext threadContext;
    private final HttpChannel httpChannel;

    @Nullable
    private final HttpTracer tracerLog;

    DefaultRestChannel(HttpChannel httpChannel, HttpRequest httpRequest, RestRequest request, BigArrays bigArrays,
                       HttpHandlingSettings settings, ThreadContext threadContext, @Nullable HttpTracer tracerLog) {
        super(request, settings.getDetailedErrorsEnabled());
        this.httpChannel = httpChannel;
        // TODO: Fix
        this.httpRequest = httpRequest;
        this.bigArrays = bigArrays;
        this.settings = settings;
        this.threadContext = threadContext;
        this.tracerLog = tracerLog;
    }

    @Override
    protected BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(bigArrays);
    }

    @Override
    public void sendResponse(RestResponse restResponse) {
        // We're sending a response so we know we won't be needing the request content again and release it
        Releasables.closeWhileHandlingException(httpRequest::release);

        final ArrayList<Releasable> toClose = new ArrayList<>(3);
        if (isCloseConnection()) {
            toClose.add(() -> CloseableChannel.closeChannel(httpChannel));
        }

        boolean success = false;
        String opaque = null;
        String contentLength = null;
        try {
            final BytesReference content = restResponse.content();
            if (content instanceof Releasable) {
                toClose.add((Releasable) content);
            }

            BytesReference finalContent = content;
            try {
                if (request.method() == RestRequest.Method.HEAD) {
                    finalContent = BytesArray.EMPTY;
                }
            } catch (IllegalArgumentException ignored) {
                assert restResponse.status() == RestStatus.METHOD_NOT_ALLOWED :
                    "request HTTP method is unsupported but HTTP status is not METHOD_NOT_ALLOWED(405)";
            }

            final HttpResponse httpResponse = httpRequest.createResponse(restResponse.status(), finalContent);

            // TODO: Ideally we should move the setting of Cors headers into :server
            // NioCorsHandler.setCorsResponseHeaders(nettyRequest, resp, corsConfig);

            opaque = request.header(X_OPAQUE_ID);
            if (opaque != null) {
                setHeaderField(httpResponse, X_OPAQUE_ID, opaque);
            }

            // Add all custom headers
            addCustomHeaders(httpResponse, restResponse.getHeaders());
            addCustomHeaders(httpResponse, threadContext.getResponseHeaders());

            // If our response doesn't specify a content-type header, set one
            setHeaderField(httpResponse, CONTENT_TYPE, restResponse.contentType(), false);
            // If our response has no content-length, calculate and set one
            contentLength = String.valueOf(restResponse.content().length());
            setHeaderField(httpResponse, CONTENT_LENGTH, contentLength, false);

            addCookies(httpResponse);

            BytesStreamOutput bytesStreamOutput = bytesOutputOrNull();
            if (bytesStreamOutput instanceof ReleasableBytesStreamOutput) {
                toClose.add((Releasable) bytesStreamOutput);
            }

            ActionListener<Void> listener = ActionListener.wrap(() -> Releasables.close(toClose));
            httpChannel.sendResponse(httpResponse, listener);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(toClose);
            }
            if (tracerLog != null) {
                tracerLog.traceResponse(restResponse, httpChannel, contentLength, opaque, request.getRequestId(), success);
            }
        }
    }

    private void setHeaderField(HttpResponse response, String headerField, String value) {
        setHeaderField(response, headerField, value, true);
    }

    private void setHeaderField(HttpResponse response, String headerField, String value, boolean override) {
        if (override || !response.containsHeader(headerField)) {
            response.addHeader(headerField, value);
        }
    }

    private void addCustomHeaders(HttpResponse response, Map<String, List<String>> customHeaders) {
        if (customHeaders != null) {
            for (Map.Entry<String, List<String>> headerEntry : customHeaders.entrySet()) {
                for (String headerValue : headerEntry.getValue()) {
                    setHeaderField(response, headerEntry.getKey(), headerValue);
                }
            }
        }
    }

    private void addCookies(HttpResponse response) {
        if (settings.isResetCookies()) {
            List<String> cookies = request.getHttpRequest().strictCookies();
            if (cookies.isEmpty() == false) {
                for (String cookie : cookies) {
                    response.addHeader(SET_COOKIE, cookie);
                }
            }
        }
    }

    // Determine if the request connection should be closed on completion.
    private boolean isCloseConnection() {
        try {
            final boolean http10 = request.getHttpRequest().protocolVersion() == HttpRequest.HttpVersion.HTTP_1_0;
            return CLOSE.equalsIgnoreCase(request.header(CONNECTION))
                || (http10 && !KEEP_ALIVE.equalsIgnoreCase(request.header(CONNECTION)));
        } catch (Exception e) {
            // In case we fail to parse the http protocol version out of the request we always close the connection
            return true;
        }
    }
}
