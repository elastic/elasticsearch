/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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
    private final CorsHandler corsHandler;

    @Nullable
    private final HttpTracer tracerLog;

    DefaultRestChannel(HttpChannel httpChannel, HttpRequest httpRequest, RestRequest request, BigArrays bigArrays,
                       HttpHandlingSettings settings, ThreadContext threadContext, CorsHandler corsHandler,
                       @Nullable HttpTracer tracerLog) {
        super(request, settings.getDetailedErrorsEnabled());
        this.httpChannel = httpChannel;
        this.httpRequest = httpRequest;
        this.bigArrays = bigArrays;
        this.settings = settings;
        this.threadContext = threadContext;
        this.corsHandler = corsHandler;
        this.tracerLog = tracerLog;
    }

    @Override
    protected BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(bigArrays);
    }

    @Override
    public void sendResponse(RestResponse restResponse) {
        // We're sending a response so we know we won't be needing the request content again and release it
        httpRequest.release();

        final ArrayList<Releasable> toClose = new ArrayList<>(3);
        if (HttpUtils.shouldCloseConnection(httpRequest)) {
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
            toClose.add(this::releaseOutputBuffer);

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

            corsHandler.setCorsResponseHeaders(httpRequest, httpResponse);

            opaque = request.header(X_OPAQUE_ID);
            if (opaque != null) {
                setHeaderField(httpResponse, X_OPAQUE_ID, opaque);
            }

            // Add all custom headers
            addCustomHeaders(httpResponse, restResponse.getHeaders());
            addCustomHeaders(httpResponse, restResponse.filterHeaders(threadContext.getResponseHeaders()));

            // If our response doesn't specify a content-type header, set one
            setHeaderField(httpResponse, CONTENT_TYPE, restResponse.contentType(), false);
            // If our response has no content-length, calculate and set one
            contentLength = String.valueOf(restResponse.content().length());
            setHeaderField(httpResponse, CONTENT_LENGTH, contentLength, false);

            addCookies(httpResponse);

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
        if (override || response.containsHeader(headerField) == false) {
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
}
