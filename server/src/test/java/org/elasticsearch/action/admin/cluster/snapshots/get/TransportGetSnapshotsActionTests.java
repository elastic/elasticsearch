/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.apache.logging.log4j.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;

public class TransportGetSnapshotsActionTests extends ESTestCase {

    private static final Logger testLogger = LogManager.getLogger(TransportGetSnapshotsAction.class);

    @TestLogging(
        reason = "verifies that cancellation-induced fetch failures log at DEBUG",
        value = "org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction:DEBUG"
    )
    public void testTaskCancelledExceptionLogsAtDebug() {
        try (var mockLog = MockLog.capture(TransportGetSnapshotsAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "debug message",
                    TransportGetSnapshotsAction.class.getCanonicalName(),
                    Level.DEBUG,
                    "failed to fetch snapshot info after cancellation for [test-snapshot]"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no warn", TransportGetSnapshotsAction.class.getCanonicalName(), Level.WARN, "*")
            );
            TransportGetSnapshotsAction.logFetchSnapshotInfoFailure(
                testLogger,
                "test-snapshot",
                new TaskCancelledException("task cancelled")
            );
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(
        reason = "verifies that wrapped TaskCancelledException is unwrapped and logged at DEBUG",
        value = "org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction:DEBUG"
    )
    public void testWrappedTaskCancelledExceptionLogsAtDebug() {
        try (var mockLog = MockLog.capture(TransportGetSnapshotsAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "debug message",
                    TransportGetSnapshotsAction.class.getCanonicalName(),
                    Level.DEBUG,
                    "failed to fetch snapshot info after cancellation for [test-snapshot]"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no warn", TransportGetSnapshotsAction.class.getCanonicalName(), Level.WARN, "*")
            );
            TransportGetSnapshotsAction.logFetchSnapshotInfoFailure(
                testLogger,
                "test-snapshot",
                new RuntimeException("wrapper", new TaskCancelledException("task cancelled"))
            );
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(
        reason = "verifies that non-cancellation fetch failures still log at WARN and not DEBUG",
        value = "org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction:DEBUG"
    )
    public void testNonCancellationExceptionLogsAtWarn() {
        try (var mockLog = MockLog.capture(TransportGetSnapshotsAction.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "warn message",
                    TransportGetSnapshotsAction.class.getCanonicalName(),
                    Level.WARN,
                    "failed to fetch snapshot info for [test-snapshot]"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no debug", TransportGetSnapshotsAction.class.getCanonicalName(), Level.DEBUG, "*")
            );
            TransportGetSnapshotsAction.logFetchSnapshotInfoFailure(testLogger, "test-snapshot", new IOException("blob not readable"));
            mockLog.assertAllExpectationsMatched();
        }
    }
}
