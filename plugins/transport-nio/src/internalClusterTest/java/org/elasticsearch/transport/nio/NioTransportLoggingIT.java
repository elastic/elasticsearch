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

package org.elasticsearch.transport.nio;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.NioIntegTestCase;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TransportLogger;

import java.io.IOException;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, scope = ESIntegTestCase.Scope.TEST)
public class NioTransportLoggingIT extends NioIntegTestCase {

    private MockLogAppender appender;

    public void setUp() throws Exception {
        super.setUp();
        appender = new MockLogAppender();
        Loggers.addAppender(LogManager.getLogger(TransportLogger.class), appender);
        Loggers.addAppender(LogManager.getLogger(TcpTransport.class), appender);
        appender.start();
    }

    public void tearDown() throws Exception {
        Loggers.removeAppender(LogManager.getLogger(TransportLogger.class), appender);
        Loggers.removeAppender(LogManager.getLogger(TcpTransport.class), appender);
        appender.stop();
        super.tearDown();
    }

    @TestLogging(value = "org.elasticsearch.transport.TransportLogger:trace", reason = "to ensure we log network events on TRACE level")
    public void testLoggingHandler() {
        final String writePattern =
                ".*\\[length: \\d+" +
                        ", request id: \\d+" +
                        ", type: request" +
                        ", version: .*" +
                        ", action: cluster:monitor/nodes/hot_threads\\[n\\]\\]" +
                        " WRITE: \\d+B";
        final MockLogAppender.LoggingExpectation writeExpectation =
                new MockLogAppender.PatternSeenEventExpectation(
                        "hot threads request", TransportLogger.class.getCanonicalName(), Level.TRACE, writePattern);

        final String readPattern =
                ".*\\[length: \\d+" +
                        ", request id: \\d+" +
                        ", type: request" +
                        ", version: .*" +
                        ", action: cluster:monitor/nodes/hot_threads\\[n\\]\\]" +
                        " READ: \\d+B";

        final MockLogAppender.LoggingExpectation readExpectation =
                new MockLogAppender.PatternSeenEventExpectation(
                        "hot threads request", TransportLogger.class.getCanonicalName(), Level.TRACE, readPattern);

        appender.addExpectation(writeExpectation);
        appender.addExpectation(readExpectation);
        client().admin().cluster().nodesHotThreads(new NodesHotThreadsRequest()).actionGet();
        appender.assertAllExpectationsMatched();
    }

    @TestLogging(value = "org.elasticsearch.transport.TcpTransport:DEBUG", reason = "to ensure we log connection events on DEBUG level")
    public void testConnectionLogging() throws IOException {
        appender.addExpectation(new MockLogAppender.PatternSeenEventExpectation("open connection log",
                TcpTransport.class.getCanonicalName(), Level.DEBUG,
                ".*opened transport connection \\[[1-9][0-9]*\\] to .*"));
        appender.addExpectation(new MockLogAppender.PatternSeenEventExpectation("close connection log",
                TcpTransport.class.getCanonicalName(), Level.DEBUG,
                ".*closed transport connection \\[[1-9][0-9]*\\] to .* with age \\[[0-9]+ms\\].*"));

        final String nodeName = internalCluster().startNode();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeName));

        appender.assertAllExpectationsMatched();
    }
}
