/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportLogger;

import java.io.IOException;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, scope = ESIntegTestCase.Scope.TEST)
public class ESLoggingHandlerIT extends ESNetty4IntegTestCase {

    private MockLog mockLog;

    public void setUp() throws Exception {
        super.setUp();
        mockLog = MockLog.capture(ESLoggingHandler.class, TransportLogger.class, TcpTransport.class);
    }

    public void tearDown() throws Exception {
        mockLog.close();
        super.tearDown();
    }

    @TestLogging(
        value = "org.elasticsearch.transport.netty4.ESLoggingHandler:trace,org.elasticsearch.transport.TransportLogger:trace",
        reason = "to ensure we log network events on TRACE level"
    )
    public void testLoggingHandler() {
        final String writePattern = ".*\\[length: \\d+"
            + ", request id: \\d+"
            + ", type: request"
            + ", version: .*"
            + ", action: cluster:monitor/nodes/stats\\[n\\]\\]"
            + " WRITE: \\d+B";
        final MockLog.LoggingExpectation writeExpectation = new MockLog.PatternSeenEventExpectation(
            "hot threads request",
            TransportLogger.class.getCanonicalName(),
            Level.TRACE,
            writePattern
        );

        final MockLog.LoggingExpectation flushExpectation = new MockLog.SeenEventExpectation(
            "flush",
            ESLoggingHandler.class.getCanonicalName(),
            Level.TRACE,
            "*FLUSH*"
        );

        final String readPattern = ".*\\[length: \\d+"
            + ", request id: \\d+"
            + ", type: request"
            + ", version: .*"
            + ", action: cluster:monitor/nodes/stats\\[n\\]\\]"
            + " READ: \\d+B";

        final MockLog.LoggingExpectation readExpectation = new MockLog.PatternSeenEventExpectation(
            "hot threads request",
            TransportLogger.class.getCanonicalName(),
            Level.TRACE,
            readPattern
        );

        mockLog.addExpectation(writeExpectation);
        mockLog.addExpectation(flushExpectation);
        mockLog.addExpectation(readExpectation);
        client().admin().cluster().prepareNodesStats().get(TimeValue.timeValueSeconds(10));
        mockLog.assertAllExpectationsMatched();
    }

    @TestLogging(value = "org.elasticsearch.transport.TcpTransport:DEBUG", reason = "to ensure we log connection events on DEBUG level")
    public void testConnectionLogging() throws IOException {
        mockLog.addExpectation(
            new MockLog.PatternSeenEventExpectation(
                "open connection log",
                TcpTransport.class.getCanonicalName(),
                Level.DEBUG,
                ".*opened transport connection \\[[1-9][0-9]*\\] to .*"
            )
        );
        mockLog.addExpectation(
            new MockLog.PatternSeenEventExpectation(
                "close connection log",
                TcpTransport.class.getCanonicalName(),
                Level.DEBUG,
                ".*closed transport connection \\[[1-9][0-9]*\\] to .* with age \\[[0-9]+ms\\]$"
            )
        );

        final String nodeName = internalCluster().startNode();
        internalCluster().stopNode(nodeName);

        mockLog.assertAllExpectationsMatched();
    }

    @TestLogging(
        value = "org.elasticsearch.transport.TcpTransport:DEBUG",
        reason = "to ensure we log exception disconnect events on DEBUG level"
    )
    public void testExceptionalDisconnectLogging() throws Exception {
        mockLog.addExpectation(
            new MockLog.PatternSeenEventExpectation(
                "exceptional close connection log",
                TcpTransport.class.getCanonicalName(),
                Level.DEBUG,
                ".*closed transport connection \\[[1-9][0-9]*\\] to .* with age \\[[0-9]+ms\\], exception:.*"
            )
        );

        final String nodeName = internalCluster().startNode();
        internalCluster().restartNode(nodeName);

        mockLog.assertAllExpectationsMatched();
    }
}
