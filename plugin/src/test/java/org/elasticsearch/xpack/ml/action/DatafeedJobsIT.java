/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentActionResponse;
import org.elasticsearch.xpack.persistent.RemovePersistentTaskAction;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class DatafeedJobsIT extends BaseMlIntegTestCase {

    @Before
    public void startNode() {
        internalCluster().ensureAtLeastNumDataNodes(1);
    }

    @After
    public void stopNode() throws Exception {
        cleanupWorkaround(1);
    }

    public void testLookbackOnly() throws Exception {
        client().admin().indices().prepareCreate("data-1")
        .addMapping("type", "time", "type=date")
        .get();
        long numDocs = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs("data-1", numDocs, twoWeeksAgo, oneWeekAgo);

        client().admin().indices().prepareCreate("data-2")
                .addMapping("type", "time", "type=date")
                .get();
        long numDocs2 = randomIntBetween(32, 2048);
        indexDocs("data-2", numDocs2, oneWeekAgo, now);

        Job.Builder job = createScheduledJob("lookback-job");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data-*"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(datafeedConfig);
        PutDatafeedAction.Response putDatafeedResponse = client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();
        assertTrue(putDatafeedResponse.isAcknowledged());

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(datafeedConfig.getId(), 0L);
        startDatafeedRequest.setEndTime(now);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));

            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedConfig.getId());
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
    }

    public void testRealtime() throws Exception {
        client().admin().indices().prepareCreate("data")
        .addMapping("type", "time", "type=date")
        .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long lastWeek = now - 604800000;
        indexDocs("data", numDocs1, lastWeek, now);

        Job.Builder job = createScheduledJob("realtime-job");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(datafeedConfig);
        PutDatafeedAction.Response putDatafeedResponse = client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();
        assertTrue(putDatafeedResponse.isAcknowledged());

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(datafeedConfig.getId(), 0L);
        PersistentActionResponse startDatafeedResponse =
                client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs1));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        });

        long numDocs2 = randomIntBetween(2, 64);
        now = System.currentTimeMillis();
        indexDocs("data", numDocs2, now + 5000, now + 6000);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs1 + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        }, 30, TimeUnit.SECONDS);

        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedConfig.getId());
        try {
            RemovePersistentTaskAction.Response stopJobResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).get();
            assertTrue(stopJobResponse.isAcknowledged());
        } catch (Exception e) {
            NodesHotThreadsResponse nodesHotThreadsResponse = client().admin().cluster().prepareNodesHotThreads().get();
            int i = 0;
            for (NodeHotThreads nodeHotThreads : nodesHotThreadsResponse.getNodes()) {
                logger.info(i++ + ":\n" +nodeHotThreads.getHotThreads());
            }
            throw e;
        }
        assertBusy(() -> {
            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedConfig.getId());
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
    }

}
