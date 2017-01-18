/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.DataCounts;
import org.elasticsearch.xpack.ml.job.DataDescription;
import org.elasticsearch.xpack.ml.job.Detector;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.scheduler.Scheduler;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.scheduler.SchedulerStatus;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.ml.integration.TooManyJobsIT.ensureClusterStateConsistencyWorkAround;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class ScheduledJobsIT extends ESIntegTestCase {

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

        Job.Builder job = createJob();
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build(true, job.getId()));
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        OpenJobAction.Response openJobResponse = client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).get();
        assertTrue(openJobResponse.isAcknowledged());

        SchedulerConfig schedulerConfig = createScheduler(job.getId() + "-scheduler", job.getId(), Collections.singletonList("data-*"));
        PutSchedulerAction.Request putSchedulerRequest = new PutSchedulerAction.Request(schedulerConfig);
        PutSchedulerAction.Response putSchedulerResponse = client().execute(PutSchedulerAction.INSTANCE, putSchedulerRequest).get();
        assertTrue(putSchedulerResponse.isAcknowledged());

        StartSchedulerAction.Request startSchedulerRequest = new StartSchedulerAction.Request(schedulerConfig.getId(), 0L);
        startSchedulerRequest.setEndTime(now);
        client().execute(StartSchedulerAction.INSTANCE, startSchedulerRequest).get();
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));

            MlMetadata mlMetadata = client().admin().cluster().prepareState().all().get()
                    .getState().metaData().custom(MlMetadata.TYPE);
            assertThat(mlMetadata.getScheduler(schedulerConfig.getId()).get().getStatus(), equalTo(SchedulerStatus.STOPPED));
        });
    }

    public void testRealtime() throws Exception {
        client().admin().indices().prepareCreate("data")
        .addMapping("type", "time", "type=date")
        .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long lastWeek = System.currentTimeMillis() - 604800000;
        indexDocs("data", numDocs1, lastWeek, now);

        Job.Builder job = createJob();
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build(true, job.getId()));
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        OpenJobAction.Response openJobResponse = client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).get();
        assertTrue(openJobResponse.isAcknowledged());

        SchedulerConfig schedulerConfig = createScheduler(job.getId() + "-scheduler", job.getId(), Collections.singletonList("data"));
        PutSchedulerAction.Request putSchedulerRequest = new PutSchedulerAction.Request(schedulerConfig);
        PutSchedulerAction.Response putSchedulerResponse = client().execute(PutSchedulerAction.INSTANCE, putSchedulerRequest).get();
        assertTrue(putSchedulerResponse.isAcknowledged());

        AtomicReference<Throwable> errorHolder = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                StartSchedulerAction.Request startSchedulerRequest = new StartSchedulerAction.Request(schedulerConfig.getId(), 0L);
                client().execute(StartSchedulerAction.INSTANCE, startSchedulerRequest).get();
            } catch (Exception | AssertionError e) {
                errorHolder.set(e);
            }
        });
        t.start();
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

        StopSchedulerAction.Request stopSchedulerRequest = new StopSchedulerAction.Request(schedulerConfig.getId());
        StopSchedulerAction.Response stopJobResponse = client().execute(StopSchedulerAction.INSTANCE, stopSchedulerRequest).get();
        assertTrue(stopJobResponse.isAcknowledged());
        assertBusy(() -> {
            MlMetadata mlMetadata = client().admin().cluster().prepareState().all().get()
                    .getState().metaData().custom(MlMetadata.TYPE);
            assertThat(mlMetadata.getScheduler(schedulerConfig.getId()).get().getStatus(), equalTo(SchedulerStatus.STOPPED));
        });
        assertThat(errorHolder.get(), nullValue());
    }

    private void indexDocs(String index, long numDocs, long start, long end) {
        int maxIncrement = (int) ((end - start) / numDocs);
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        long timestamp = start;
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest(index, "type");
            indexRequest.source("time", timestamp);
            bulkRequestBuilder.add(indexRequest);
            timestamp += randomIntBetween(1, maxIncrement);
        }
        BulkResponse bulkResponse = bulkRequestBuilder
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        assertThat(bulkResponse.hasFailures(), is(false));
        logger.info("Indexed [{}] documents", numDocs);
    }

    private Job.Builder createJob() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.JSON);
        dataDescription.setTimeFormat("yyyy-MM-dd HH:mm:ss");

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId("my_job_id");

        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
    }

    private SchedulerConfig createScheduler(String schedulerId, String jobId, List<String> indexes) {
        SchedulerConfig.Builder builder = new SchedulerConfig.Builder(schedulerId, jobId);
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
        deleteAllSchedulers(client);
        deleteAllJobs(client);
    }

    private static void deleteAllSchedulers(Client client) throws Exception {
        MetaData metaData = client.admin().cluster().prepareState().get().getState().getMetaData();
        MlMetadata mlMetadata = metaData.custom(MlMetadata.TYPE);
        for (Scheduler scheduler : mlMetadata.getSchedulers().values()) {
            String schedulerId = scheduler.getId();
            try {
                StopSchedulerAction.Response stopResponse =
                        client.execute(StopSchedulerAction.INSTANCE, new StopSchedulerAction.Request(schedulerId)).get();
                assertTrue(stopResponse.isAcknowledged());
            } catch (ExecutionException e) {
                // CONFLICT is ok, as it means the scheduler has already stopped, which isn't an issue at all.
                if (RestStatus.CONFLICT != ExceptionsHelper.status(e.getCause())) {
                    throw new RuntimeException(e);
                }
            }
            assertBusy(() -> {
                try {
                    GetSchedulersStatsAction.Request request = new GetSchedulersStatsAction.Request(schedulerId);
                    GetSchedulersStatsAction.Response r = client.execute(GetSchedulersStatsAction.INSTANCE, request).get();
                    assertThat(r.getResponse().results().get(0).getSchedulerStatus(), equalTo(SchedulerStatus.STOPPED));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            DeleteSchedulerAction.Response deleteResponse =
                    client.execute(DeleteSchedulerAction.INSTANCE, new DeleteSchedulerAction.Request(schedulerId)).get();
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
                assertTrue(response.isAcknowledged());
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
