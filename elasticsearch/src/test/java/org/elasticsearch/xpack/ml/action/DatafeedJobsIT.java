/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedStatus;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobStatus;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.persistent.PersistentActionResponse;
import org.elasticsearch.xpack.persistent.RemovePersistentTaskAction;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.integration.TooManyJobsIT.ensureClusterStateConsistencyWorkAround;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class DatafeedJobsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MlPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @After
    public void clearMlMetadata() throws Exception {
        clearMlMetadata(client());
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

        Job.Builder job = createJob("lookback-job");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build(true, job.getId()));
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        client().execute(InternalOpenJobAction.INSTANCE, new InternalOpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getStatus(), JobStatus.OPENED);
        });

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data-*"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(datafeedConfig);
        PutDatafeedAction.Response putDatafeedResponse = client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();
        assertTrue(putDatafeedResponse.isAcknowledged());

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(datafeedConfig.getId(), 0L);
        startDatafeedRequest.setEndTime(now);
        PersistentActionResponse startDatafeedResponse =
                client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));

            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedConfig.getId());
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedStatus(), equalTo(DatafeedStatus.STOPPED));
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

        Job.Builder job = createJob("realtime-job");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build(true, job.getId()));
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        client().execute(InternalOpenJobAction.INSTANCE, new InternalOpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getStatus(), JobStatus.OPENED);
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
            assertThat(response.getResponse().results().get(0).getDatafeedStatus(), equalTo(DatafeedStatus.STOPPED));
        });
    }

    private void indexDocs(String index, long numDocs, long start, long end) {
        int maxDelta = (int) (end - start - 1);
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest(index, "type");
            long timestamp = start + randomIntBetween(0, maxDelta);
            assert timestamp >= start && timestamp < end;
            indexRequest.source("time", timestamp);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        assertThat(bulkResponse.hasFailures(), is(false));
        logger.info("Indexed [{}] documents", numDocs);
    }

    private Job.Builder createJob(String jobId) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.JSON);
        dataDescription.setTimeFormat("yyyy-MM-dd HH:mm:ss");

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId(jobId);

        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
    }

    private DatafeedConfig createDatafeed(String datafeedId, String jobId, List<String> indexes) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedId, jobId);
        builder.setQueryDelay(1);
        builder.setFrequency(2);
        builder.setIndexes(indexes);
        builder.setTypes(Collections.singletonList("type"));
        return builder.build();
    }

    private DataCounts getDataCounts(String jobId) {
        GetResponse getResponse = client().prepareGet(AnomalyDetectorsIndex.jobResultsIndexName(jobId),
                DataCounts.TYPE.getPreferredName(), jobId + "-data-counts").get();
        if (getResponse.isExists() == false) {
            return new DataCounts(jobId);
        }

        try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, getResponse.getSourceAsBytesRef())) {
            return DataCounts.PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void clearMlMetadata(Client client) throws Exception {
        deleteAllDatafeeds(client);
        deleteAllJobs(client);
    }

    private static void deleteAllDatafeeds(Client client) throws Exception {
        MetaData metaData = client.admin().cluster().prepareState().get().getState().getMetaData();
        MlMetadata mlMetadata = metaData.custom(MlMetadata.TYPE);
        for (DatafeedConfig datafeed : mlMetadata.getDatafeeds().values()) {
            String datafeedId = datafeed.getId();
            try {
                RemovePersistentTaskAction.Response stopResponse =
                        client.execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId)).get();
                assertTrue(stopResponse.isAcknowledged());
            } catch (ExecutionException e) {
                // CONFLICT is ok, as it means the datafeed has already stopped, which isn't an issue at all.
                if (RestStatus.CONFLICT != ExceptionsHelper.status(e.getCause())) {
                    throw new RuntimeException(e);
                }
            }
            assertBusy(() -> {
                try {
                    GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
                    GetDatafeedsStatsAction.Response r = client.execute(GetDatafeedsStatsAction.INSTANCE, request).get();
                    assertThat(r.getResponse().results().get(0).getDatafeedStatus(), equalTo(DatafeedStatus.STOPPED));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            DeleteDatafeedAction.Response deleteResponse =
                    client.execute(DeleteDatafeedAction.INSTANCE, new DeleteDatafeedAction.Request(datafeedId)).get();
            assertTrue(deleteResponse.isAcknowledged());
        }
    }

    public static void deleteAllJobs(Client client) throws Exception {
        MetaData metaData = client.admin().cluster().prepareState().get().getState().getMetaData();
        MlMetadata mlMetadata = metaData.custom(MlMetadata.TYPE);
        for (Map.Entry<String, Job> entry : mlMetadata.getJobs().entrySet()) {
            String jobId = entry.getKey();
            try {
                CloseJobAction.Response response =
                        client.execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId)).get();
                assertTrue(response.isClosed());
            } catch (Exception e) {
                // ignore
            }
            DeleteJobAction.Response response =
                    client.execute(DeleteJobAction.INSTANCE, new DeleteJobAction.Request(jobId)).get();
            assertTrue(response.isAcknowledged());
        }
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        ensureClusterStateConsistencyWorkAround();
    }
}
