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
import org.elasticsearch.common.logging.Loggers;
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
import java.util.List;
import java.util.Map;

public class HttpTracerTests extends ESTestCase {

    @TestLogging(reason = "Get HttpTracer to output trace logs", value = "org.elasticsearch.http.HttpTracer:TRACE")
    public void testLogging() {
        String httpTracerLog = HttpTracer.class.getName();

        MockLogAppender appender = new MockLogAppender();
        try {
            appender.start();
            Loggers.addAppender(LogManager.getLogger(httpTracerLog), appender);

            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "request log",
                    httpTracerLog,
                    Level.TRACE,
                    "\\[\\d+]\\[idHeader]\\[GET]\\[uri] received request from \\[.*] trace.id: 4bf92f3577b34da6a3ce929d0e0e4736"
                )
            );
            appender.addExpectation(
                new MockLogAppender.PatternSeenEventExpectation(
                    "response log",
                    httpTracerLog,
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

            tracer.logResponse(
                new RestResponse(RestStatus.ACCEPTED, ""),
                new FakeRestRequest.FakeHttpChannel(InetSocketAddress.createUnresolved("127.0.0.1", 9200)),
                "length",
                "idHeader",
                1L,
                true
            );

            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(LogManager.getLogger(httpTracerLog), appender);
            appender.stop();
        }
    }
}
