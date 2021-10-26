/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.task;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class JobTaskTests extends ESTestCase {

    XPackLicenseState licenseState = mock(XPackLicenseState.class);

    public void testJobTaskMatcherMatch() {
        Task nonJobTask1 = mock(Task.class);
        Task nonJobTask2 = mock(Task.class);
        JobTask jobTask1 = new JobTask("ml-1", 0, "persistent", "", null, null, licenseState);
        JobTask jobTask2 = new JobTask("ml-2", 1, "persistent", "", null, null, licenseState);

        assertThat(OpenJobAction.JobTaskMatcher.match(nonJobTask1, "_all"), is(false));
        assertThat(OpenJobAction.JobTaskMatcher.match(nonJobTask2, "_all"), is(false));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask1, "_all"), is(true));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask2, "_all"), is(true));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask1, "ml-1"), is(true));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask2, "ml-1"), is(false));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask1, "ml-2"), is(false));
        assertThat(OpenJobAction.JobTaskMatcher.match(jobTask2, "ml-2"), is(true));
    }

    public void testKillJob() {
        JobTask jobTask = new JobTask("job-to-kill", 0, "persistent", "", null, null, licenseState);
        AutodetectProcessManager processManager = mock(AutodetectProcessManager.class);
        jobTask.setAutodetectProcessManager(processManager);

        jobTask.killJob("test");

        assertThat(jobTask.isClosing(), is(true));
        verify(processManager).killProcess(jobTask, true, "test");
    }

    public void testCloseOrVacateTransitions() {

        JobTask jobTask = new JobTask("transition-test-task", 0, "persistent", "", null, null, licenseState);

        assertThat(jobTask.isClosing(), is(false));
        assertThat(jobTask.isVacating(), is(false));

        AutodetectProcessManager processManager = mock(AutodetectProcessManager.class);
        jobTask.setAutodetectProcessManager(processManager);

        assertThat(jobTask.isClosing(), is(false));
        assertThat(jobTask.isVacating(), is(false));

        // we can transition from neither closing nor vacating to vacating
        assertThat(jobTask.triggerVacate(), is(true));

        assertThat(jobTask.isClosing(), is(false));
        assertThat(jobTask.isVacating(), is(true));

        jobTask.closeJob("just testing");
        verify(processManager).closeJob(jobTask, "just testing");

        assertThat(jobTask.isClosing(), is(true));
        assertThat(jobTask.isVacating(), is(false));

        // we cannot transition from closing back to vacating
        assertThat(jobTask.triggerVacate(), is(false));

        assertThat(jobTask.isClosing(), is(true));
        assertThat(jobTask.isVacating(), is(false));
    }
}
