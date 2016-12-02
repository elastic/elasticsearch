/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.schedulers;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.job.metadata.Allocation;
import org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobRunnerTests;

import java.util.Collections;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestStartJobSchedulerActionTests extends ESTestCase {

    public void testPrepareRequest() throws Exception {
        JobManager jobManager = mock(JobManager.class);
        Job.Builder job = ScheduledJobRunnerTests.createScheduledJob();
        when(jobManager.getJobOrThrowIfUnknown(anyString())).thenReturn(job.build());
        Allocation allocation =
                new Allocation(null, "foo", false, JobStatus.OPENED, null, new SchedulerState(JobSchedulerStatus.STOPPED, null, null));
        when(jobManager.getJobAllocation(anyString())).thenReturn(allocation);
        RestStartJobSchedulerAction action = new RestStartJobSchedulerAction(Settings.EMPTY, mock(RestController.class),
                jobManager, mock(ClusterService.class));

        RestRequest restRequest1 = new FakeRestRequest.Builder().withParams(Collections.singletonMap("start", "not-a-date")).build();
        ElasticsearchParseException e =  expectThrows(ElasticsearchParseException.class,
                () -> action.prepareRequest(restRequest1, mock(NodeClient.class)));
        assertEquals("Query param 'start' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());

        RestRequest restRequest2 = new FakeRestRequest.Builder().withParams(Collections.singletonMap("end", "not-a-date")).build();
        e =  expectThrows(ElasticsearchParseException.class, () -> action.prepareRequest(restRequest2, mock(NodeClient.class)));
        assertEquals("Query param 'end' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());
    }

    public void testParseDateOrThrow() {
        assertEquals(0L, RestStartJobSchedulerAction.parseDateOrThrow("0", "start"));
        assertEquals(0L, RestStartJobSchedulerAction.parseDateOrThrow("1970-01-01T00:00:00Z", "start"));

        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> RestStartJobSchedulerAction.parseDateOrThrow("not-a-date", "start"));
        assertEquals("Query param 'start' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());
    }

}
