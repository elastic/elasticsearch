/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.singletonList;

@SuppressForbidden(reason = "use http server")
@SuppressWarnings("restriction")
class SqlHandler implements HttpHandler, AutoCloseable {

    private static final Logger log = LogManager.getLogger(SqlHandler.class.getName());

    private final RestHandler handler;
    private final SqlNodeClient client;

    SqlHandler(SqlNodeClient client, RestHandler restHandler) {
        this.client = client;
        this.handler = restHandler;
    }

    @Override
    public void handle(HttpExchange http) throws IOException {
        log.debug("Received query call...");

        if ("HEAD".equals(http.getRequestMethod())) {
            http.sendResponseHeaders(RestStatus.OK.getStatus(), 0);
            http.close();
            return;
        }


        boolean closeEndpoint = http.getRequestURI().toString().contains("/close");

        FakeRestRequest request = createRequest(http);
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);

        try {
            handler.handleRequest(request, channel, client);
            if (!closeEndpoint) {
                if (channel.await()) {
                    sendHttpResponse(http, channel.capturedResponse());
                } else {
                    sendHttpResponse(http, new BytesRestResponse(channel, new IllegalStateException("Timed-out")));
                }
            } else {
                sendHttpResponse(http, new BytesRestResponse(RestStatus.OK, ""));
            }
        } catch (Exception e) {
            sendHttpResponse(http, new BytesRestResponse(channel, e));
        }
    }

    private FakeRestRequest createRequest(HttpExchange http) throws IOException {
        Headers headers = http.getRequestHeaders();
        XContentType contentType = XContentType.fromMediaTypeOrFormat(
            headers.getOrDefault(HttpHeaderNames.CONTENT_TYPE.toString(), singletonList(XContentType.JSON.mediaType())).get(0));

        Map<String, String> params = new LinkedHashMap<>();
        params.put("pretty", "");
        params.put("human", "true");
        params.put("error_trace", "true");

        BytesStreamOutput bso = new BytesStreamOutput();
        Streams.copy(http.getRequestBody(), bso);
        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withHeaders(headers)
            .withParams(params)
            .withRemoteAddress(http.getRemoteAddress())
            .withContent(bso.bytes(), contentType)
            .build();

        // consume error_trace (in server Netty does that)
        request.param("error_trace");
        return request;
    }

    private void sendHttpResponse(HttpExchange http, RestResponse response) {
        try {
            // first do the conversion in case an exception is triggered
            if (http.getResponseHeaders().isEmpty()) {
                Headers headers = http.getResponseHeaders();
                headers.put(HttpHeaderNames.CONTENT_TYPE.toString(), singletonList(response.contentType()));
                if (response.getHeaders() != null) {
                    headers.putAll(response.getHeaders());
                }

                // NB: this needs to be called last otherwise any calls to the headers are silently ignored...
                http.sendResponseHeaders(response.status().getStatus(), response.content().length());
            }
            response.content().writeTo(http.getResponseBody());
        } catch (IOException ex) {
            log.error("Caught error while trying to catch error", ex);
        } finally {
            http.close();
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
