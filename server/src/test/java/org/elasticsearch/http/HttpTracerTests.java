/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.ChunkedLoggingStreamTests;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class HttpTracerTests extends ESTestCase {

    // these loggers are named in the docs so must not be changed without due care
    public static final String HTTP_TRACER_LOGGER = "org.elasticsearch.http.HttpTracer";
    public static final String HTTP_BODY_TRACER_LOGGER = "org.elasticsearch.http.HttpBodyTracer";

    @TestLogging(reason = "testing trace logging", value = HTTP_TRACER_LOGGER + ":TRACE," + HTTP_BODY_TRACER_LOGGER + ":INFO")
    public void testLogging() {
        MockLogAppender appender = new MockLogAppender();
        try (var ignored = appender.capturing(HttpTracer.class)) {

            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "request log",
                    HTTP_TRACER_LOGGER,
                    Level.TRACE,
                    "\\[\\d+]\\[idHeader]\\[GET]\\[uri] received request from \\[.*] trace.id: 4bf92f3577b34da6a3ce929d0e0e4736"
                )
            );
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "response log",
                    HTTP_TRACER_LOGGER,
                    Level.TRACE,
                    "\\[\\d+]\\[idHeader]\\[ACCEPTED]\\[text/plain; charset=UTF-8]\\[length] sent response to \\[.*] success \\[true]"
                )
            );

            RestRequest request = new FakeRestRequest.Builder(new NamedXContentRegistry(List.of())).withMethod(RestRequest.Method.GET)
                .withPath("uri")
                .withHeaders(
                    Map.of(
                        Task.X_OPAQUE_ID_HTTP_HEADER,
                        List.of("idHeader"),
                        "traceparent",
                        List.of("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                    )
                )
                .build();

            HttpTracer tracer = new HttpTracer().maybeLogRequest(request, null);
            assertNotNull(tracer);
            assertFalse(tracer.isBodyTracerEnabled());

            tracer.logResponse(
                new RestResponse(RestStatus.ACCEPTED, ""),
                new FakeRestRequest.FakeHttpChannel(InetSocketAddress.createUnresolved("127.0.0.1", 9200)),
                "length",
                "idHeader",
                1L,
                true
            );

            appender.assertAllExpectationsMatched();
        }
    }

    @TestLogging(reason = "testing trace logging", value = HTTP_TRACER_LOGGER + ":TRACE," + HTTP_BODY_TRACER_LOGGER + ":TRACE")
    public void testBodyLogging() {
        HttpTracer tracer = new HttpTracer();
        assertTrue(tracer.isBodyTracerEnabled());

        var responseBody = new BytesArray(randomUnicodeOfLengthBetween(1, 100).getBytes(StandardCharsets.UTF_8));
        RestRequest request = new FakeRestRequest.Builder(new NamedXContentRegistry(List.of())).withMethod(RestRequest.Method.GET)
            .withPath("uri")
            .withContent(responseBody, null)
            .build();

        assertEquals(
            responseBody,
            ChunkedLoggingStreamTests.getDecodedLoggedBody(
                LogManager.getLogger(HTTP_BODY_TRACER_LOGGER),
                Level.TRACE,
                "[" + request.getRequestId() + "] request body",
                ReferenceDocs.HTTP_TRACER,
                () -> assertNotNull(tracer.maybeLogRequest(request, null))
            )
        );
    }
}
