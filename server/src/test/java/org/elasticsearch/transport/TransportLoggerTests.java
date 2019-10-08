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
package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
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
        final String writePattern =
            ".*\\[length: \\d+" +
                ", request id: \\d+" +
                ", type: request" +
                ", version: .*" +
                ", action: cluster:monitor/stats]" +
                " WRITE: \\d+B";
        final MockLogAppender.LoggingExpectation writeExpectation =
            new MockLogAppender.PatternSeenEventExpectation(
                "hot threads request", TransportLogger.class.getCanonicalName(), Level.TRACE, writePattern);

        final String readPattern =
            ".*\\[length: \\d+" +
                ", request id: \\d+" +
                ", type: request" +
                ", version: .*" +
                ", action: cluster:monitor/stats]" +
                " READ: \\d+B";

        final MockLogAppender.LoggingExpectation readExpectation =
            new MockLogAppender.PatternSeenEventExpectation(
                "cluster monitor request", TransportLogger.class.getCanonicalName(), Level.TRACE, readPattern);

        appender.addExpectation(writeExpectation);
        appender.addExpectation(readExpectation);
        BytesReference bytesReference = buildRequest();
        TransportLogger.logInboundMessage(mock(TcpChannel.class), bytesReference.slice(6, bytesReference.length() - 6));
        TransportLogger.logOutboundMessage(mock(TcpChannel.class), bytesReference);
        appender.assertAllExpectationsMatched();
    }

    private BytesReference buildRequest() throws IOException {
        try (BytesStreamOutput messageOutput = new BytesStreamOutput()) {
            messageOutput.setVersion(Version.CURRENT);
            try (ThreadContext context = new ThreadContext(Settings.EMPTY)) {
                context.writeTo(messageOutput);
            }
            messageOutput.writeString(ClusterStatsAction.NAME);
            new ClusterStatsRequest().writeTo(messageOutput);
            BytesReference messageBody = messageOutput.bytes();
            final BytesReference header = buildHeader(randomInt(30), messageBody.length());
            return new CompositeBytesReference(header, messageBody);
        }
    }

    private BytesReference buildHeader(long requestId, int length) throws IOException {
        try (BytesStreamOutput headerOutput = new BytesStreamOutput(TcpHeader.HEADER_SIZE)) {
            headerOutput.setVersion(Version.CURRENT);
            TcpHeader.writeHeader(headerOutput, requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT, length);
            final BytesReference bytes = headerOutput.bytes();
            assert bytes.length() == TcpHeader.HEADER_SIZE : "header size mismatch expected: " + TcpHeader.HEADER_SIZE + " but was: "
                + bytes.length();
            return bytes;
        }
    }
}
