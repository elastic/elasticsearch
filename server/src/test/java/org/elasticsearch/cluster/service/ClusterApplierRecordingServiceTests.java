/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Recorder;
import org.elasticsearch.cluster.service.ClusterApplierRecordingService.Stats.Recording;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.contains;

public class ClusterApplierRecordingServiceTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        deterministicTaskQueue = new DeterministicTaskQueue();
        deterministicTaskQueue.scheduleAt(between(0, 1000000), () -> {});
        deterministicTaskQueue.runAllTasks();
        threadPool = deterministicTaskQueue.getThreadPool();
    }

    private void advanceTime(long millis) {
        deterministicTaskQueue.scheduleAt(deterministicTaskQueue.getCurrentTimeMillis() + millis, () -> {});
        deterministicTaskQueue.runAllTasks();
    }

    public void testRecorder() {
        var recorder = new Recorder(threadPool, TimeValue.ZERO);
        {
            Releasable releasable = recorder.record("action1");
            advanceTime(5);
            releasable.close();
        }
        {
            Releasable releasable = recorder.record("action2");
            advanceTime(37);
            releasable.close();
        }
        {
            Releasable releasable = recorder.record("action3");
            advanceTime(3);
            releasable.close();
        }

        var recordings = recorder.getRecordings();
        assertThat(recordings, contains(Tuple.tuple("action1", 5L), Tuple.tuple("action2", 37L), Tuple.tuple("action3", 3L)));
    }

    public void testRecorderAlreadyRecording() {
        var recorder = new Recorder(threadPool, TimeValue.ZERO);
        Releasable ignored = recorder.record("action1");
        expectThrows(IllegalStateException.class, () -> recorder.record("action2"));
    }

    public void testRecordingServiceStats() {
        var service = new ClusterApplierRecordingService();

        {
            var recorder = new Recorder(threadPool, TimeValue.ZERO);
            try (var r = recorder.record("action1")) {
                advanceTime(5);
            }
            try (var r = recorder.record("action2")) {
                advanceTime(37);
            }
            try (var r = recorder.record("action3")) {
                advanceTime(3);
            }
            service.updateStats(recorder);
            var stats = service.getStats();
            assertThat(
                stats.getRecordings().entrySet(),
                contains(
                    Map.entry("action2", new Recording(1, 37)),
                    Map.entry("action1", new Recording(1, 5)),
                    Map.entry("action3", new Recording(1, 3))
                )
            );
        }
        {
            var recorder = new Recorder(threadPool, TimeValue.ZERO);
            try (var r = recorder.record("action1")) {
                advanceTime(3);
            }
            try (var r = recorder.record("action2")) {
                advanceTime(32);
            }
            try (var r = recorder.record("action3")) {
                advanceTime(6);
            }
            service.updateStats(recorder);
            var stats = service.getStats();
            assertThat(
                stats.getRecordings().entrySet(),
                contains(
                    Map.entry("action2", new Recording(2, 69)),
                    Map.entry("action3", new Recording(2, 9)),
                    Map.entry("action1", new Recording(2, 8))
                )
            );
        }
        {
            var recorder = new Recorder(threadPool, TimeValue.ZERO);
            try (var r = recorder.record("action1")) {
                advanceTime(2);
            }
            try (var r = recorder.record("action3")) {
                advanceTime(4);
            }
            service.updateStats(recorder);
            var stats = service.getStats();
            assertThat(
                stats.getRecordings().entrySet(),
                contains(Map.entry("action3", new Recording(3, 13)), Map.entry("action1", new Recording(3, 10)))
            );
        }
    }

    @TestLogging(reason = "testing debug logging", value = "org.elasticsearch.cluster.service.ClusterApplierRecordingService:DEBUG")
    public void testSlowTaskDebugLogging() {
        final var debugLoggingTimeout = TimeValue.timeValueMillis(between(1, 100000));
        var recorder = new Recorder(threadPool, debugLoggingTimeout);

        // ensure hot threads is logged if the action is too slow
        var slowAction = recorder.record("slow_action");
        deterministicTaskQueue.scheduleAt(
            deterministicTaskQueue.getCurrentTimeMillis() + debugLoggingTimeout.millis() + between(1, 1000),
            slowAction::close
        );
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllTasksInTimeOrder,
            ClusterApplierRecordingService.class,
            new MockLog.SeenEventExpectation(
                "hot threads",
                ClusterApplierRecordingService.class.getCanonicalName(),
                Level.DEBUG,
                "hot threads while applying cluster state [slow_action]"
            )
        );

        // ensure hot threads is _NOT_ logged if the action completes quickly enough
        var fastAction = recorder.record("fast_action");
        deterministicTaskQueue.scheduleAt(
            randomLongBetween(0, deterministicTaskQueue.getCurrentTimeMillis() + debugLoggingTimeout.millis() - 1),
            fastAction::close
        );
        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllTasksInTimeOrder,
            ClusterApplierRecordingService.class,
            new MockLog.UnseenEventExpectation("hot threads", ClusterApplierRecordingService.class.getCanonicalName(), Level.DEBUG, "*")
        );
    }

}
