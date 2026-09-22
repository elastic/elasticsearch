/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;
import static org.hamcrest.Matchers.equalTo;

public class IndexNodeExternalScanWarningTests extends ESTestCase {

    public void testCoordinatorWarningAbsentWithoutIndexRole() {
        DiscoveryNode search = DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build();
        DiscoveryNode data = DiscoveryNodeUtils.builder("data-1").roles(Set.of(DATA_HOT_NODE_ROLE)).build();
        AtomicBoolean warned = new AtomicBoolean();

        MockLog.assertThatLogger(() -> {
            ComputeService.warnIndexCoordinatorOnce(search, warned);
            ComputeService.warnIndexCoordinatorOnce(data, warned);
        },
            ComputeService.class,
            new MockLog.UnseenEventExpectation(
                "no coordinator warning on non-index nodes",
                ComputeService.class.getCanonicalName(),
                Level.WARN,
                "index node"
            )
        );
        assertFalse(warned.get());
    }

    public void testCoordinatorWarningOnceForRepeatedCalls() {
        DiscoveryNode index = DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build();
        Runnable warning = ComputeService.newIndexCoordinatorWarningCallback(index);

        MockLog.assertThatLogger(() -> {
            warning.run();
            warning.run();
        },
            ComputeService.class,
            new ExactEventCountExpectation(
                "coordinator warning",
                ComputeService.class.getCanonicalName(),
                Level.WARN,
                "index node [index-1] is running an external ES|QL scan locally",
                1
            )
        );
    }

    public void testRequestScopedCoordinatorWarningOnceAcrossConcurrentCalls() throws Exception {
        DiscoveryNode index = DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build();
        Runnable warning = ComputeService.newIndexCoordinatorWarningCallback(index);
        int threads = between(4, 8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger invocations = new AtomicInteger();

        MockLog.assertThatLogger(() -> {
            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                Thread t = new Thread(() -> {
                    try {
                        start.await();
                        warning.run();
                        invocations.incrementAndGet();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } finally {
                        done.countDown();
                    }
                });
                workers.add(t);
                t.start();
            }
            start.countDown();
            try {
                done.await();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
            for (Thread t : workers) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        },
            ComputeService.class,
            new ExactEventCountExpectation(
                "single coordinator warning",
                ComputeService.class.getCanonicalName(),
                Level.WARN,
                "index node [index-1] is running an external ES|QL scan locally",
                1
            )
        );

        assertEquals(threads, invocations.get());
    }

    public void testCoordinatorWarningCallbackIsNoOpWithoutIndexRole() {
        DiscoveryNode search = DiscoveryNodeUtils.builder("search-1").roles(Set.of(SEARCH_ROLE)).build();
        Runnable warning = ComputeService.newIndexCoordinatorWarningCallback(search);

        MockLog.assertThatLogger(
            warning::run,
            ComputeService.class,
            new MockLog.UnseenEventExpectation(
                "no coordinator warning on a search node",
                ComputeService.class.getCanonicalName(),
                Level.WARN,
                "index node"
            )
        );
    }

    public void testReceiverWarningFiresForRemoteParentOnIndexNode() {
        DiscoveryNode index = DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build();

        MockLog.assertThatLogger(
            () -> DataNodeComputeHandler.warnIfRemoteIndexWorker(index, "old-coordinator"),
            DataNodeComputeHandler.class,
            new MockLog.SeenEventExpectation(
                "receiver warning",
                DataNodeComputeHandler.class.getCanonicalName(),
                Level.WARN,
                "index node [index-1] received external ES|QL scan work from coordinator [old-coordinator]"
            )
        );
    }

    public void testReceiverWarningSuppressedForSelfAssignment() {
        DiscoveryNode index = DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build();

        MockLog.assertThatLogger(
            () -> DataNodeComputeHandler.warnIfRemoteIndexWorker(index, "index-1"),
            DataNodeComputeHandler.class,
            new MockLog.UnseenEventExpectation(
                "no receiver warning for self-assignment",
                DataNodeComputeHandler.class.getCanonicalName(),
                Level.WARN,
                "received external ES|QL scan work"
            )
        );
    }

    public void testReceiverWarningSuppressedForUnsetParentTask() {
        DiscoveryNode index = DiscoveryNodeUtils.builder("index-1").roles(Set.of(INDEX_ROLE)).build();

        MockLog.assertThatLogger(
            () -> DataNodeComputeHandler.warnIfRemoteIndexWorker(index, TaskId.EMPTY_TASK_ID.getNodeId()),
            DataNodeComputeHandler.class,
            new MockLog.UnseenEventExpectation(
                "no receiver warning when no coordinator can be named",
                DataNodeComputeHandler.class.getCanonicalName(),
                Level.WARN,
                "received external ES|QL scan work"
            )
        );
    }

    private static final class ExactEventCountExpectation implements MockLog.LoggingExpectation {
        private final String name;
        private final String logger;
        private final Level level;
        private final String message;
        private final int expectedCount;
        private final AtomicInteger count = new AtomicInteger();

        private ExactEventCountExpectation(String name, String logger, Level level, String message, int expectedCount) {
            this.name = name;
            this.logger = logger;
            this.level = level;
            this.message = message;
            this.expectedCount = expectedCount;
        }

        @Override
        public void match(LogEvent event) {
            if (event.getLevel().equals(level)
                && event.getLoggerName().equals(logger)
                && event.getMessage().getFormattedMessage().contains(message)) {
                count.incrementAndGet();
            }
        }

        @Override
        public void assertMatched() {
            assertThat("unexpected number of " + name + " events", count.get(), equalTo(expectedCount));
        }
    }
}
