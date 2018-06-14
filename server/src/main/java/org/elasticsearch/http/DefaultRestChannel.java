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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
    static final String X_OPAQUE_ID = "X-Opaque-Id";

    private final HttpRequest httpRequest;
    private final BigArrays bigArrays;
    private final HttpHandlingSettings settings;
    private final ThreadContext threadContext;
    private final HttpChannel httpChannel;

    DefaultRestChannel(HttpChannel httpChannel, HttpRequest httpRequest, RestRequest request, BigArrays bigArrays,
                       HttpHandlingSettings settings, ThreadContext threadContext) {
        super(request, settings.getDetailedErrorsEnabled());
        this.httpChannel = httpChannel;
        this.httpRequest = httpRequest;
        this.bigArrays = bigArrays;
        this.settings = settings;
        this.threadContext = threadContext;
    }

    @Override
    protected BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(bigArrays);
    }

    @Override
    public void sendResponse(RestResponse restResponse) {
        HttpResponse httpResponse;
        if (RestRequest.Method.HEAD == request.method()) {
            httpResponse = httpRequest.createResponse(restResponse.status(), BytesArray.EMPTY);
        } else {
            httpResponse = httpRequest.createResponse(restResponse.status(), restResponse.content());
        }

        // TODO: Ideally we should move the setting of Cors headers into :server
        // NioCorsHandler.setCorsResponseHeaders(nettyRequest, resp, corsConfig);

        String opaque = request.header(X_OPAQUE_ID);
        if (opaque != null) {
            setHeaderField(httpResponse, X_OPAQUE_ID, opaque);
        }

        // Add all custom headers
        addCustomHeaders(httpResponse, restResponse.getHeaders());
        addCustomHeaders(httpResponse, threadContext.getResponseHeaders());

        ArrayList<Releasable> toClose = new ArrayList<>(3);

        boolean success = false;
        try {
            // If our response doesn't specify a content-type header, set one
            setHeaderField(httpResponse, CONTENT_TYPE, restResponse.contentType(), false);
            // If our response has no content-length, calculate and set one
            setHeaderField(httpResponse, CONTENT_LENGTH, String.valueOf(restResponse.content().length()), false);

            addCookies(httpResponse);

            BytesReference content = restResponse.content();
            if (content instanceof Releasable) {
                toClose.add((Releasable) content);
            }
            BytesStreamOutput bytesStreamOutput = bytesOutputOrNull();
            if (bytesStreamOutput instanceof ReleasableBytesStreamOutput) {
                toClose.add((Releasable) bytesStreamOutput);
            }

            if (isCloseConnection()) {
                toClose.add(httpChannel);
            }

            ActionListener<Void> listener = ActionListener.wrap(() -> Releasables.close(toClose));
            httpChannel.sendResponse(httpResponse, listener);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(toClose);
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
        final boolean http10 = isHttp10();
        return CLOSE.equalsIgnoreCase(request.header(CONNECTION)) || (http10 && !KEEP_ALIVE.equalsIgnoreCase(request.header(CONNECTION)));
    }

    // Determine if the request protocol version is HTTP 1.0
    private boolean isHttp10() {
        return request.getHttpRequest().protocolVersion() == HttpRequest.HttpVersion.HTTP_1_0;
    }
}
