/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class UpdateJobProcessNotifierTests extends ESTestCase {

    private static final int QUEUE_CAPACITY = 1000;
    private static final String LOGGER_NAME = UpdateJobProcessNotifier.class.getCanonicalName();

    private ClusterService clusterService;
    private UpdateJobProcessNotifier notifier;

    @Before
    public void init() {
        clusterService = mock(ClusterService.class);
        notifier = new UpdateJobProcessNotifier(mock(Client.class), clusterService, mock(ThreadPool.class));
    }

    private static UpdateParams groupsUpdate(String jobId) {
        return UpdateParams.fromJobUpdate(new JobUpdate.Builder(jobId).setGroups(List.of("group")).build());
    }

    private void fillQueue() {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            assertThat(notifier.submitJobUpdate(groupsUpdate("queued-" + i), ActionListener.noop()), equalTo(true));
        }
    }

    private static MockLog.PatternSeenEventExpectation seen(String name, Level level, String regex) {
        return new MockLog.PatternSeenEventExpectation(name, LOGGER_NAME, level, regex);
    }

    private static MockLog.PatternNotSeenEventExpectation notSeen(String name, Level level, String regex) {
        return new MockLog.PatternNotSeenEventExpectation(name, LOGGER_NAME, level, regex);
    }

    public void testQueueWithSpaceShouldAcceptUpdatesSilently() {
        MockLog.assertThatLogger(
            () -> assertThat(notifier.submitJobUpdate(groupsUpdate("job-1"), ActionListener.noop()), equalTo(true)),
            UpdateJobProcessNotifier.class,
            notSeen("no warning", Level.WARN, ".*"),
            notSeen("no error", Level.ERROR, ".*")
        );
    }

    public void testFullQueueGivenGroupsChangeUpdateShouldLogCalendarUpdateDropped() {
        fillQueue();
        MockLog.assertThatLogger(
            () -> assertThat(notifier.submitJobUpdate(groupsUpdate("dropped-job"), ActionListener.noop()), equalTo(false)),
            UpdateJobProcessNotifier.class,
            seen(
                "dropped scheduled-events update at WARN with token, capacity and job id",
                Level.WARN,
                Pattern.quote("[ml-invariant:calendar_update_dropped] ") + ".*\\(1000\\).*\\[dropped-job\\].*"
            )
        );
    }

    public void testFullQueueGivenRepeatedGroupsChangeDropsShouldLogOnce() {
        fillQueue();
        MockLog.assertThatLogger(() -> {
            notifier.submitJobUpdate(groupsUpdate("first-dropped"), ActionListener.noop());
            notifier.submitJobUpdate(groupsUpdate("second-dropped"), ActionListener.noop());
        },
            UpdateJobProcessNotifier.class,
            seen("first drop logged", Level.WARN, ".*calendar_update_dropped.*first-dropped.*"),
            notSeen("second drop throttled across jobs", Level.WARN, ".*second-dropped.*")
        );
    }

    public void testFullQueueGivenOtherUpdateShouldLogUntokenedWarnPerJob() {
        fillQueue();
        MockLog.assertThatLogger(() -> {
            notifier.submitJobUpdate(UpdateParams.builder("first-dropped").build(), ActionListener.noop());
            notifier.submitJobUpdate(UpdateParams.builder("second-dropped").build(), ActionListener.noop());
        },
            UpdateJobProcessNotifier.class,
            seen("first drop logged without token", Level.WARN, "Update queue is full \\(1000\\).*\\[first-dropped\\]"),
            seen("second drop logged without token", Level.WARN, "Update queue is full \\(1000\\).*\\[second-dropped\\]"),
            notSeen("no invariant token for other updates", Level.WARN, ".*ml-invariant.*")
        );
    }

    public void testStopShouldClearQueueWithoutLogging() {
        fillQueue();
        ArgumentCaptor<LifecycleListener> lifecycleListener = ArgumentCaptor.forClass(LifecycleListener.class);
        verify(clusterService).addLifecycleListener(lifecycleListener.capture());

        MockLog.assertThatLogger(
            () -> lifecycleListener.getValue().beforeStop(),
            UpdateJobProcessNotifier.class,
            notSeen("no warning", Level.WARN, ".*"),
            notSeen("no error", Level.ERROR, ".*")
        );

        assertThat(notifier.submitJobUpdate(groupsUpdate("after-stop"), ActionListener.noop()), equalTo(true));
    }
}
