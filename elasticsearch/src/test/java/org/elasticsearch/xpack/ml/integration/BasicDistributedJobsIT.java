/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

public class BasicDistributedJobsIT extends BaseMlIntegTestCase {

    public void testFailOverBasics() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        ensureStableCluster(4);

        Job.Builder job = createJob("job_id");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build(true, job.getId()));
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        ensureGreen();
        OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
        client().execute(OpenJobAction.INSTANCE, openJobRequest).get();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });

        internalCluster().stopRandomDataNode();
        ensureStableCluster(3);
        ensureGreen();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });

        internalCluster().stopRandomDataNode();
        ensureStableCluster(2);
        ensureGreen();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(JobState.OPENED, statsResponse.getResponse().results().get(0).getState());
        });
        cleanupWorkaround(2);
    }

}
