/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.xpack.prelert.action.StopSchedulerAction.Request;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.SchedulerConfig;
import org.elasticsearch.xpack.prelert.job.SchedulerStatus;
import org.elasticsearch.xpack.prelert.job.metadata.PrelertMetadata;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobRunnerTests.createScheduledJob;
import static org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobRunnerTests.createSchedulerConfig;
import static org.hamcrest.Matchers.equalTo;

public class StopJobSchedulerActionRequestTests extends AbstractStreamableTestCase<StopSchedulerAction.Request> {

    @Override
    protected Request createTestInstance() {
        return new Request(randomAsciiOfLengthBetween(1, 20));
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    public void testValidate() {
        Job job = createScheduledJob().build();
        PrelertMetadata prelertMetadata1 = new PrelertMetadata.Builder().putJob(job, false).build();
        Exception e = expectThrows(ResourceNotFoundException.class, () -> StopSchedulerAction.validate("foo", prelertMetadata1));
        assertThat(e.getMessage(), equalTo("No scheduler with id [foo] exists"));

        SchedulerConfig schedulerConfig = createSchedulerConfig("foo", "foo").build();
        PrelertMetadata prelertMetadata2 = new PrelertMetadata.Builder().putJob(job, false)
                .putScheduler(schedulerConfig)
                .build();
        e = expectThrows(ElasticsearchStatusException.class, () -> StopSchedulerAction.validate("foo", prelertMetadata2));
        assertThat(e.getMessage(), equalTo("scheduler already stopped, expected scheduler status [STARTED], but got [STOPPED]"));

        PrelertMetadata prelertMetadata3 = new PrelertMetadata.Builder().putJob(job, false)
                .putScheduler(schedulerConfig)
                .updateSchedulerStatus("foo", SchedulerStatus.STARTED)
                .build();
        StopSchedulerAction.validate("foo", prelertMetadata3);
    }

}
