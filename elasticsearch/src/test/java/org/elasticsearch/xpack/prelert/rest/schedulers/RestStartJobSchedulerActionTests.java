/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.rest.schedulers;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.JobStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerConfig;
import org.elasticsearch.xpack.prelert.job.metadata.PrelertMetadata;
import org.elasticsearch.xpack.prelert.job.scheduler.ScheduledJobRunnerTests;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestStartJobSchedulerActionTests extends ESTestCase {

    public void testPrepareRequest() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        Job.Builder job = ScheduledJobRunnerTests.createScheduledJob();
        SchedulerConfig schedulerConfig = ScheduledJobRunnerTests.createSchedulerConfig("foo-scheduler", "foo").build();
        PrelertMetadata prelertMetadata = new PrelertMetadata.Builder()
                .putJob(job.build(), false)
                .putScheduler(schedulerConfig)
                .updateStatus("foo", JobStatus.OPENED, null)
                .build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(PrelertMetadata.TYPE, prelertMetadata))
                .build());
        RestStartSchedulerAction action = new RestStartSchedulerAction(Settings.EMPTY, mock(RestController.class), clusterService);

        Map<String, String> params = new HashMap<>();
        params.put("start", "not-a-date");
        params.put("scheduler_id", "foo-scheduler");
        RestRequest restRequest1 = new FakeRestRequest.Builder().withParams(params).build();
        ElasticsearchParseException e =  expectThrows(ElasticsearchParseException.class,
                () -> action.prepareRequest(restRequest1, mock(NodeClient.class)));
        assertEquals("Query param 'start' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());

        params = new HashMap<>();
        params.put("end", "not-a-date");
        params.put("scheduler_id", "foo-scheduler");
        RestRequest restRequest2 = new FakeRestRequest.Builder().withParams(params).build();
        e =  expectThrows(ElasticsearchParseException.class, () -> action.prepareRequest(restRequest2, mock(NodeClient.class)));
        assertEquals("Query param 'end' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());
    }

    public void testParseDateOrThrow() {
        assertEquals(0L, RestStartSchedulerAction.parseDateOrThrow("0", "start"));
        assertEquals(0L, RestStartSchedulerAction.parseDateOrThrow("1970-01-01T00:00:00Z", "start"));

        Exception e = expectThrows(ElasticsearchParseException.class,
                () -> RestStartSchedulerAction.parseDateOrThrow("not-a-date", "start"));
        assertEquals("Query param 'start' with value 'not-a-date' cannot be parsed as a date or converted to a number (epoch).",
                e.getMessage());
    }

}
