/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.groups;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.groups.GroupOrJob;

import java.util.Arrays;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class GroupOrJobTests extends ESTestCase {

    public void testSingleJob() {
        Job job = mock(Job.class);
        GroupOrJob groupOrJob = new GroupOrJob.SingleJob(job);
        assertThat(groupOrJob.isGroup(), is(false));
        assertThat(groupOrJob.jobs(), contains(job));
        expectThrows(UnsupportedOperationException.class, () -> groupOrJob.jobs().add(mock(Job.class)));
    }

    public void testGroup() {
        Job job1 = mock(Job.class);
        Job job2 = mock(Job.class);
        GroupOrJob groupOrJob = new GroupOrJob.Group(Arrays.asList(job1, job2));
        assertThat(groupOrJob.isGroup(), is(true));
        assertThat(groupOrJob.jobs(), contains(job1, job2));
        expectThrows(UnsupportedOperationException.class, () -> groupOrJob.jobs().add(mock(Job.class)));
    }
}