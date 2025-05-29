/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.Level;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, scope = ESIntegTestCase.Scope.TEST)
public class ClusterConnectionManagerIntegTests extends ESIntegTestCase {
    private MockLog mockLog;

    public void setUp() throws Exception {
        super.setUp();
        mockLog = MockLog.capture(ClusterConnectionManager.class);
    }

    public void tearDown() throws Exception {
        mockLog.close();
        super.tearDown();
    }

    @TestLogging(
        value = "org.elasticsearch.transport.ClusterConnectionManager:WARN",
        reason = "to ensure we log cluster manager disconnect events on WARN level"
    )
    public void testExceptionalDisconnectLoggingInClusterConnectionManager() throws Exception {
        mockLog.addExpectation(
            new MockLog.PatternSeenEventExpectation(
                "cluster connection manager exceptional disconnect log",
                ClusterConnectionManager.class.getCanonicalName(),
                Level.WARN,
                "transport connection to \\[.*\\] closed (by remote )?with exception .*"
            )
        );

        final String nodeName = internalCluster().startNode();
        internalCluster().restartNode(nodeName);

        mockLog.assertAllExpectationsMatched();
    }
}
