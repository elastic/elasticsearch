/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParams;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.junit.After;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor.Factory.countNumberInferenceProcessors;
import static org.elasticsearch.xpack.ml.integration.ClassificationIT.KEYWORD_FIELD;
import static org.elasticsearch.xpack.ml.integration.MlNativeDataFrameAnalyticsIntegTestCase.buildAnalytics;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.getDataCounts;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.indexDocs;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TestFeatureResetIT extends MlNativeAutodetectIntegTestCase {

    @After
    public void cleanup() throws Exception {
        cleanUp();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/71072")
    public void testMLFeatureReset() throws Exception {
        startRealtime("feature_reset_anomaly_job");
        startDataFrameJob("feature_reset_data_frame_analytics_job");
        putTrainedModelIngestPipeline("feature_reset_inference_pipeline");
        for(int i = 0; i < 100; i ++) {
            indexDocForInference("feature_reset_inference_pipeline");
        }
        client().execute(DeletePipelineAction.INSTANCE, new DeletePipelineRequest("feature_reset_inference_pipeline")).actionGet();
        assertBusy(() ->
            assertThat(countNumberInferenceProcessors(client().admin().cluster().prepareState().get().getState()), equalTo(0))
        );
        client().execute(
            ResetFeatureStateAction.INSTANCE,
            new ResetFeatureStateRequest()
        ).actionGet();
        assertBusy(() -> assertThat(client().admin().indices().prepareGetIndex().addIndices(".ml*").get().indices(), emptyArray()));
        assertThat(isResetMode(), is(false));
    }

    public void testMLFeatureResetFailureDueToPipelines() throws Exception {
        putTrainedModelIngestPipeline("feature_reset_failure_inference_pipeline");
        Exception ex = expectThrows(Exception.class, () -> client().execute(
            ResetFeatureStateAction.INSTANCE,
            new ResetFeatureStateRequest()
        ).actionGet());
        assertThat(
            ex.getMessage(),
            containsString(
                "Unable to reset machine learning feature as there are ingest pipelines still referencing trained machine learning models"
            )
        );
        client().execute(DeletePipelineAction.INSTANCE, new DeletePipelineRequest("feature_reset_failure_inference_pipeline")).actionGet();
        assertThat(isResetMode(), is(false));
    }

    private boolean isResetMode() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        return MlMetadata.getMlMetadata(state).isResetMode();
    }

    private void startDataFrameJob(String jobId) throws Exception {
        String sourceIndex = jobId + "-src";
        String destIndex = jobId + "-dest";
        ClassificationIT.createIndex(sourceIndex, false);
        ClassificationIT.indexData(sourceIndex, 300, 50, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null,
            new Classification(
                KEYWORD_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
        PutDataFrameAnalyticsAction.Request request = new PutDataFrameAnalyticsAction.Request(config);
        client().execute(PutDataFrameAnalyticsAction.INSTANCE, request).actionGet();

        client().execute(StartDataFrameAnalyticsAction.INSTANCE, new StartDataFrameAnalyticsAction.Request(jobId));
    }

    private void startRealtime(String jobId) throws Exception {
        client().admin().indices().prepareCreate("data")
            .setMapping("time", "type=date")
            .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long lastWeek = now - 604800000;
        indexDocs(logger, "data", numDocs1, lastWeek, now);

        Job.Builder job = createScheduledJob(jobId);
        registerJob(job);
        putJob(job);
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data"));
        registerDatafeed(datafeedConfig);
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, null);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), is(equalTo(numDocs1)));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), is(equalTo(0L)));
        });

        long numDocs2 = randomIntBetween(2, 64);
        now = System.currentTimeMillis();
        indexDocs(logger, "data", numDocs2, now + 5000, now + 6000);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), is(equalTo(numDocs1 + numDocs2)));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), is(equalTo(0L)));
        }, 30, TimeUnit.SECONDS);
    }

    private void putTrainedModelIngestPipeline(String pipelineId) throws Exception {
        client().execute(
            PutPipelineAction.INSTANCE,
            new PutPipelineRequest(
                pipelineId,
                new BytesArray(
                    "{\n" +
                    "    \"processors\": [\n" +
                        "      {\n" +
                        "        \"inference\": {\n" +
                        "          \"inference_config\": {\"classification\":{}},\n" +
                        "          \"model_id\": \"lang_ident_model_1\",\n" +
                        "          \"field_map\": {}\n" +
                        "        }\n" +
                        "      }\n" +
                        "    ]\n" +
                        "  }"
                ),
                XContentType.JSON
            )
        ).actionGet();
    }

    private void indexDocForInference(String pipelineId) {
        client().prepareIndex("foo")
            .setPipeline(pipelineId)
            .setSource("{\"text\": \"this is some plain text.\"}", XContentType.JSON)
            .get();
    }

}
