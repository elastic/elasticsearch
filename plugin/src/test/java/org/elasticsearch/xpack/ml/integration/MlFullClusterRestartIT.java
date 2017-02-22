/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class MlFullClusterRestartIT extends BaseMlIntegTestCase {

    public void testFullClusterRestart() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        ensureStableCluster(3);

        client().admin().indices().prepareCreate("data")
                .addMapping("type", "time", "type=date")
                .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long weekAgo = now - 604800000;
        long twoWeeksAgo = weekAgo - 604800000;
        indexDocs("data", numDocs1, twoWeeksAgo, weekAgo);

        Job.Builder job = createScheduledJob("job_id");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).actionGet();
        assertTrue(putJobResponse.isAcknowledged());

        DatafeedConfig config = createDatafeed("data_feed_id", job.getId(), Collections.singletonList("data"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(config);
        PutDatafeedAction.Response putDatadeedResponse = client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest)
                .actionGet();
        assertTrue(putDatadeedResponse.isAcknowledged());

        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(config.getId(), 0L);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertEquals(numDocs1, dataCounts.getProcessedRecordCount());
            assertEquals(0L, dataCounts.getOutOfOrderTimeStampCount());
        });

        logger.info("Restarting all nodes");
        internalCluster().fullRestart();
        logger.info("Restarted all nodes");
        assertBusy(() -> {
            ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            PersistentTasksInProgress tasks = clusterState.metaData().custom(PersistentTasksInProgress.TYPE);
            assertNotNull(tasks);
            assertEquals(2, tasks.taskMap().size());

            Collection<PersistentTaskInProgress<?>> taskCollection = tasks.findTasks(OpenJobAction.NAME, p -> true);
            assertEquals(1, taskCollection.size());
            PersistentTaskInProgress<?> task = taskCollection.iterator().next();
            assertEquals(JobState.OPENED, task.getStatus());

            taskCollection = tasks.findTasks(StartDatafeedAction.NAME, p -> true);
            assertEquals(1, taskCollection.size());
            task = taskCollection.iterator().next();
            assertEquals(DatafeedState.STARTED, task.getStatus());
        });

        long numDocs2 = randomIntBetween(2, 64);
        long yesterday = now - 86400000;
        indexDocs("data", numDocs2, yesterday, now);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertEquals(numDocs1 + numDocs2, dataCounts.getProcessedRecordCount());
            assertEquals(0L, dataCounts.getOutOfOrderTimeStampCount());
        }, 30, TimeUnit.SECONDS);
        cleanupWorkaround(3);
    }

}
