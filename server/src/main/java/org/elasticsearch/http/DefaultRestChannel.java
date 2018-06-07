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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultRestChannel extends AbstractRestChannel implements RestChannel {

    private static final String CLOSE = "close";
    private static final String CONNECTION = "connection";
    private static final String KEEP_ALIVE = "keep-alive";
    private static final String CONTENT_TYPE = "content-type";
    private static final String CONTENT_LENGTH = "content-length";
    private static final String SET_COOKIE = "set-cookie";

    private final BigArrays bigArrays;
    private final HttpHandlingSettings settings;
    private final ThreadContext threadContext;
    private final LLHttpChannel channel = new LLHttpChannel();

    public DefaultRestChannel(BigArrays bigArrays, RestRequest request, HttpHandlingSettings settings, ThreadContext threadContext) {
        super(request, settings.getDetailedErrorsEnabled());
        this.bigArrays = bigArrays;
        this.settings = settings;
        this.threadContext = threadContext;
    }

    @Override
    protected BytesStreamOutput newBytesOutput() {
        return new ReleasableBytesStreamOutput(bigArrays);
    }

    @Override
    public void sendResponse(RestResponse response) {
//        ByteBuf buffer = ByteBufUtils.toByteBuf(response.content());
//        final FullHttpResponse resp;
//        if (HttpMethod.HEAD.equals(nettyRequest.method())) {
//            resp = newResponse(Unpooled.EMPTY_BUFFER);
//        } else {
//            resp = newResponse(buffer);
//        }
//        resp.setStatus(getStatus(response.status()));

//        NioCorsHandler.setCorsResponseHeaders(nettyRequest, resp, corsConfig);

        String opaque = request.header("X-Opaque-Id");
        if (opaque != null) {
            setHeaderField(response, "X-Opaque-Id", opaque);
        }

        // Add all custom headers
        addCustomHeaders(response, threadContext.getResponseHeaders());

        ArrayList<Releasable> toClose = new ArrayList<>(3);

        boolean success = false;
        try {
            // If our response doesn't specify a content-type header, set one
            setHeaderField(response, CONTENT_TYPE, response.contentType(), false);
            // If our response has no content-length, calculate and set one
            setHeaderField(response, CONTENT_LENGTH, String.valueOf(response.content().length()), false);

            addCookies(response);

            BytesReference content = response.content();
            if (content instanceof Releasable) {
                toClose.add((Releasable) content);
            }
            BytesStreamOutput bytesStreamOutput = bytesOutputOrNull();
            if (bytesStreamOutput instanceof ReleasableBytesStreamOutput) {
                toClose.add((Releasable) bytesStreamOutput);
            }

            if (isCloseConnection()) {
                toClose.add(channel::close);
            }

            ActionListener<Void> listener = ActionListener.wrap(() -> Releasables.close(toClose));
            channel.sendResponse(response, listener);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(toClose);
            }
        }

    }

    private void setHeaderField(RestResponse response, String headerField, String value) {
        setHeaderField(response, headerField, value, true);
    }

    private void setHeaderField(RestResponse response, String headerField, String value, boolean override) {
        if (override || !response.getHeaders().containsKey(headerField)) {
            response.addHeader(headerField, value);
        }
    }

    private void addCustomHeaders(RestResponse response, Map<String, List<String>> customHeaders) {
        if (customHeaders != null) {
            for (Map.Entry<String, List<String>> headerEntry : customHeaders.entrySet()) {
                for (String headerValue : headerEntry.getValue()) {
                    setHeaderField(response, headerEntry.getKey(), headerValue);
                }
            }
        }
    }

    private void addCookies(RestResponse response) {
        if (settings.isResetCookies()) {
            List<String> cookies = request.strictCookies();
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
        return false;
//        return request.protocolVersion().equals(HttpVersion.HTTP_1_0);
    }

    private static class LLHttpChannel implements Closeable {

        private void sendResponse(RestResponse response, ActionListener<Void> listener) {

        }

        @Override
        public void close() {

        }
    }
}
