/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tracing.Tracer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.tasks.Task.X_OPAQUE_ID_HTTP_HEADER;

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
    private final Recycler<BytesRef> recycler;
    private final HttpHandlingSettings settings;
    private final ThreadContext threadContext;
    private final HttpChannel httpChannel;
    private final CorsHandler corsHandler;
    private final Tracer tracer;

    @Nullable
    private final HttpTracer httpLogger;

    DefaultRestChannel(
        HttpChannel httpChannel,
        HttpRequest httpRequest,
        RestRequest request,
        Recycler<BytesRef> recycler,
        HttpHandlingSettings settings,
        ThreadContext threadContext,
        CorsHandler corsHandler,
        @Nullable HttpTracer httpLogger,
        Tracer tracer
    ) {
        super(request, settings.detailedErrorsEnabled());
        this.httpChannel = httpChannel;
        this.httpRequest = httpRequest;
        this.recycler = recycler;
        this.settings = settings;
        this.threadContext = threadContext;
        this.corsHandler = corsHandler;
        this.httpLogger = httpLogger;
        this.tracer = tracer;
    }

    @Override
    protected BytesStream newBytesOutput() {
        return new RecyclerBytesStreamOutput(recycler);
    }

    @Override
    public void sendResponse(RestResponse restResponse) {
        // We're sending a response so we know we won't be needing the request content again and release it
        httpRequest.release();

        final String traceId = "rest-" + this.request.getRequestId();

        final ArrayList<Releasable> toClose = new ArrayList<>(4);
        if (HttpUtils.shouldCloseConnection(httpRequest)) {
            toClose.add(() -> CloseableChannel.closeChannel(httpChannel));
        }
        toClose.add(() -> tracer.stopTrace(traceId));

        boolean success = false;
        String opaque = null;
        String contentLength = null;

        boolean isHeadRequest = false;
        try {
            if (request.method() == RestRequest.Method.HEAD) {
                isHeadRequest = true;
            }
        } catch (IllegalArgumentException ignored) {
            assert restResponse.status() == RestStatus.METHOD_NOT_ALLOWED
                : "request HTTP method is unsupported but HTTP status is not METHOD_NOT_ALLOWED(405)";
        }
        try {
            final HttpResponse httpResponse;
            if (isHeadRequest == false && restResponse.isChunked()) {
                httpResponse = httpRequest.createResponse(restResponse.status(), restResponse.chunkedContent());
            } else {
                final BytesReference content = restResponse.content();
                if (content instanceof Releasable) {
                    toClose.add((Releasable) content);
                }
                toClose.add(this::releaseOutputBuffer);

                BytesReference finalContent = isHeadRequest ? BytesArray.EMPTY : content;
                httpResponse = httpRequest.createResponse(restResponse.status(), finalContent);
            }
            corsHandler.setCorsResponseHeaders(httpRequest, httpResponse);

            opaque = request.header(X_OPAQUE_ID_HTTP_HEADER);
            if (opaque != null) {
                setHeaderField(httpResponse, X_OPAQUE_ID_HTTP_HEADER, opaque);
            }

            // Add all custom headers
            addCustomHeaders(httpResponse, restResponse.getHeaders());
            addCustomHeaders(httpResponse, restResponse.filterHeaders(threadContext.getResponseHeaders()));

            // If our response doesn't specify a content-type header, set one
            setHeaderField(httpResponse, CONTENT_TYPE, restResponse.contentType(), false);
            if (restResponse.isChunked() == false) {
                // If our response has no content-length, calculate and set one
                contentLength = String.valueOf(restResponse.content().length());
                setHeaderField(httpResponse, CONTENT_LENGTH, contentLength, false);
            } else {
                setHeaderField(httpResponse, "Transfer-Encoding", "chunked");
            }

            addCookies(httpResponse);

            tracer.setAttribute(traceId, "http.status_code", restResponse.status().getStatus());
            restResponse.getHeaders()
                .forEach((key, values) -> tracer.setAttribute(traceId, "http.response.headers." + key, String.join("; ", values)));

            ActionListener<Void> listener = ActionListener.wrap(() -> Releasables.close(toClose));
            try (ThreadContext.StoredContext existing = threadContext.stashContext()) {
                httpChannel.sendResponse(httpResponse, listener);
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(toClose);
            }
            if (httpLogger != null) {
                httpLogger.logResponse(restResponse, httpChannel, contentLength, opaque, request.getRequestId(), success);
            }
        }
    }

    private static void setHeaderField(HttpResponse response, String headerField, String value) {
        setHeaderField(response, headerField, value, true);
    }

    private static void setHeaderField(HttpResponse response, String headerField, String value, boolean override) {
        if (override || response.containsHeader(headerField) == false) {
            response.addHeader(headerField, value);
        }
    }

    private static void addCustomHeaders(HttpResponse response, Map<String, List<String>> customHeaders) {
        if (customHeaders != null) {
            for (Map.Entry<String, List<String>> headerEntry : customHeaders.entrySet()) {
                for (String headerValue : headerEntry.getValue()) {
                    setHeaderField(response, headerEntry.getKey(), headerValue);
                }
            }
        }
    }

    private void addCookies(HttpResponse response) {
        if (settings.resetCookies()) {
            List<String> cookies = request.getHttpRequest().strictCookies();
            if (cookies.isEmpty() == false) {
                for (String cookie : cookies) {
                    response.addHeader(SET_COOKIE, cookie);
                }
            }
        }
    }
}
