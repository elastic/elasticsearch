/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;

import static org.mockito.Mockito.mock;

@TestLogging(value = "org.elasticsearch.transport.TransportLogger:trace", reason = "to ensure we log network events on TRACE level")
public class TransportLoggerTests extends ESTestCase {

    private MockLogAppender appender;

    public void setUp() throws Exception {
        super.setUp();
        appender = new MockLogAppender();
        Loggers.addAppender(LogManager.getLogger(TransportLogger.class), appender);
        appender.start();
    }

    public void tearDown() throws Exception {
        Loggers.removeAppender(LogManager.getLogger(TransportLogger.class), appender);
        appender.stop();
        super.tearDown();
    }

    public void testLoggingHandler() throws IOException {
        final String writePattern = ".*\\[length: \\d+"
            + ", request id: \\d+"
            + ", type: request"
            + ", version: .*"
            + ", header size: \\d+B"
            + ", action: cluster:monitor/stats]"
            + " WRITE: \\d+B";
        final MockLogAppender.LoggingExpectation writeExpectation = new MockLogAppender.PatternSeenEventExpectation(
            "hot threads request",
            TransportLogger.class.getCanonicalName(),
            Level.TRACE,
            writePattern
        );

        final String readPattern = ".*\\[length: \\d+"
            + ", request id: \\d+"
            + ", type: request"
            + ", version: .*"
            + ", header size: \\d+B"
            + ", action: cluster:monitor/stats]"
            + " READ: \\d+B";

        final MockLogAppender.LoggingExpectation readExpectation = new MockLogAppender.PatternSeenEventExpectation(
            "cluster monitor request",
            TransportLogger.class.getCanonicalName(),
            Level.TRACE,
            readPattern
        );

        appender.addExpectation(writeExpectation);
        appender.addExpectation(readExpectation);
        BytesReference bytesReference = buildRequest();
        TransportLogger.logInboundMessage(mock(TcpChannel.class), bytesReference.slice(6, bytesReference.length() - 6));
        TransportLogger.logOutboundMessage(mock(TcpChannel.class), bytesReference);
        appender.assertAllExpectationsMatched();
    }

    private BytesReference buildRequest() throws IOException {
        BytesRefRecycler recycler = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);
        Compression.Scheme compress = randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4, null);
        try (RecyclerBytesStreamOutput bytesStreamOutput = new RecyclerBytesStreamOutput(recycler)) {
            OutboundMessage.Request request = new OutboundMessage.Request(
                new ThreadContext(Settings.EMPTY),
                new ClusterStatsRequest(),
                    TransportVersion.current(),
                ClusterStatsAction.NAME,
                randomInt(30),
                false,
                compress
            );
            return request.serialize(bytesStreamOutput);
        }
    }
}
