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
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpTracerTests extends ESTestCase {

    @TestLogging(reason = "Get HttpTracer to output trace logs", value = "org.elasticsearch.http.HttpTracer:TRACE")
    public void testLogging() throws IllegalAccessException {
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

            Map<String, List<String>> headers = new HashMap<>();
            headers.put(Task.X_OPAQUE_ID_HTTP_HEADER, Collections.singletonList("idHeader"));
            headers.put("traceparent", Collections.singletonList("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"));

            RestRequest request = new FakeRestRequest.Builder(new NamedXContentRegistry(Collections.emptyList())).withMethod(
                RestRequest.Method.GET
            ).withPath("uri").withHeaders(headers).build();

            HttpTracer tracer = new HttpTracer().maybeTraceRequest(request, null);
            assertNotNull(tracer);

            tracer.traceResponse(
                new BytesRestResponse(RestStatus.ACCEPTED, ""),
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
