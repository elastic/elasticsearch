/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import org.elasticsearch.ESNetty4IntegTestCase;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.core.MockLogAppender;
import org.elasticsearch.logging.spi.AppenderSupport;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportLogger;

import java.io.IOException;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, scope = ESIntegTestCase.Scope.TEST)
public class ESLoggingHandlerIT extends ESNetty4IntegTestCase {

    private MockLogAppender appender;

    public void setUp() throws Exception {
        super.setUp();
        appender = new MockLogAppender();
        AppenderSupport.provider().addAppender(LogManager.getLogger(ESLoggingHandler.class), appender);
        AppenderSupport.provider().addAppender(LogManager.getLogger(TransportLogger.class), appender);
        AppenderSupport.provider().addAppender(LogManager.getLogger(TcpTransport.class), appender);
        appender.start();
    }

    public void tearDown() throws Exception {
        AppenderSupport.provider().removeAppender(LogManager.getLogger(ESLoggingHandler.class), appender);
        AppenderSupport.provider().removeAppender(LogManager.getLogger(TransportLogger.class), appender);
        AppenderSupport.provider().removeAppender(LogManager.getLogger(TcpTransport.class), appender);
        appender.stop();
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
            + ", action: cluster:monitor/nodes/hot_threads\\[n\\]\\]"
            + " WRITE: \\d+B";
        final MockLogAppender.LoggingExpectation writeExpectation = MockLogAppender.createPatternSeenEventExpectation(
            "hot threads request",
            TransportLogger.class.getCanonicalName(),
            Level.TRACE,
            writePattern
        );

        final MockLogAppender.LoggingExpectation flushExpectation = MockLogAppender.createSeenEventExpectation(
            "flush",
            ESLoggingHandler.class.getCanonicalName(),
            Level.TRACE,
            "*FLUSH*"
        );

        final String readPattern = ".*\\[length: \\d+"
            + ", request id: \\d+"
            + ", type: request"
            + ", version: .*"
            + ", action: cluster:monitor/nodes/hot_threads\\[n\\]\\]"
            + " READ: \\d+B";

        final MockLogAppender.LoggingExpectation readExpectation = MockLogAppender.createPatternSeenEventExpectation(
            "hot threads request",
            TransportLogger.class.getCanonicalName(),
            Level.TRACE,
            readPattern
        );

        appender.addExpectation(writeExpectation);
        appender.addExpectation(flushExpectation);
        appender.addExpectation(readExpectation);
        client().admin().cluster().nodesHotThreads(new NodesHotThreadsRequest()).actionGet();
        appender.assertAllExpectationsMatched();
    }

    @TestLogging(value = "org.elasticsearch.transport.TcpTransport:DEBUG", reason = "to ensure we log connection events on DEBUG level")
    public void testConnectionLogging() throws IOException {
        appender.addExpectation(
            MockLogAppender.createPatternSeenEventExpectation(
                "open connection log",
                TcpTransport.class.getCanonicalName(),
                Level.DEBUG,
                ".*opened transport connection \\[[1-9][0-9]*\\] to .*"
            )
        );
        appender.addExpectation(
            MockLogAppender.createPatternSeenEventExpectation(
                "close connection log",
                TcpTransport.class.getCanonicalName(),
                Level.DEBUG,
                ".*closed transport connection \\[[1-9][0-9]*\\] to .* with age \\[[0-9]+ms\\].*"
            )
        );

        final String nodeName = internalCluster().startNode();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeName));

        appender.assertAllExpectationsMatched();
    }
}
