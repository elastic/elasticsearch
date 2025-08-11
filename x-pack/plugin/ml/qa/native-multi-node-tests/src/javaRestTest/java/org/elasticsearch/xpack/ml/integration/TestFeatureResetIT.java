/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateAction;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateRequest;
import org.elasticsearch.action.ingest.DeletePipelineAction;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xcontent.XContentType;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.integration.ClassificationIT.KEYWORD_FIELD;
import static org.elasticsearch.xpack.ml.integration.MlNativeDataFrameAnalyticsIntegTestCase.buildAnalytics;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.getDataCounts;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.indexDocs;
import static org.elasticsearch.xpack.ml.utils.InferenceProcessorInfoExtractor.countInferenceProcessors;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class TestFeatureResetIT extends MlNativeAutodetectIntegTestCase {

    private final Set<String> createdPipelines = new HashSet<>();
    private final Set<String> jobIds = new HashSet<>();
    private final Set<String> datafeedIds = new HashSet<>();

    void cleanupDatafeed(String datafeedId) {
        try {
            stopDatafeed(datafeedId);
        } catch (Exception e) {
            // ignore
        }
        try {
            deleteDatafeed(datafeedId);
        } catch (Exception e) {
            // ignore
        }
    }

    void cleanupJob(String jobId) {
        try {
            closeJob(jobId);
        } catch (Exception e) {
            // ignore
        }
        try {
            deleteJob(jobId);
        } catch (Exception e) {
            // ignore
        }
    }

    @Override
    // The extra work of the original native clean up causes the indices to be recreated
    // It can complicate the logging and make it difficult to determine if the reset feature API cleaned up all the indices appropriately
    protected void cleanUpResources() {
        super.cleanUpResources();
        for (String jobId : jobIds) {
            cleanupJob(jobId);
        }
        for (String datafeedId : datafeedIds) {
            cleanupDatafeed(datafeedId);
        }
    }

    @After
    public void cleanup() throws Exception {
        cleanUp();
        for (String pipeline : createdPipelines) {
            try {
                client().execute(DeletePipelineAction.INSTANCE, new DeletePipelineRequest(pipeline)).actionGet();
            } catch (Exception ex) {
                logger.warn(() -> new ParameterizedMessage("error cleaning up pipeline [{}]", pipeline), ex);
            }
        }
    }

    public void testMLFeatureReset() throws Exception {
        startRealtime("feature_reset_anomaly_job");
        putAndStartJob("feature_reset_static_anomaly_job");
        startDataFrameJob("feature_reset_data_frame_analytics_job");
        putTrainedModelIngestPipeline("feature_reset_inference_pipeline");
        createdPipelines.add("feature_reset_inference_pipeline");
        for (int i = 0; i < 100; i++) {
            indexDocForInference("feature_reset_inference_pipeline");
        }
        client().execute(DeletePipelineAction.INSTANCE, new DeletePipelineRequest("feature_reset_inference_pipeline")).actionGet();
        createdPipelines.remove("feature_reset_inference_pipeline");

        assertBusy(() -> assertThat(countInferenceProcessors(client().admin().cluster().prepareState().get().getState()), equalTo(0)));
        client().execute(ResetFeatureStateAction.INSTANCE, new ResetFeatureStateRequest()).actionGet();
        assertBusy(() -> assertThat(client().admin().indices().prepareGetIndex().addIndices(".ml*").get().indices(), emptyArray()));
        assertThat(isResetMode(), is(false));
        // If we have succeeded, clear the jobs and datafeeds so that the delete API doesn't recreate the notifications index
        jobIds.clear();
        datafeedIds.clear();
    }

    public void testMLFeatureResetFailureDueToPipelines() throws Exception {
        putTrainedModelIngestPipeline("feature_reset_failure_inference_pipeline");
        createdPipelines.add("feature_reset_failure_inference_pipeline");
        Exception ex = expectThrows(
            Exception.class,
            () -> client().execute(ResetFeatureStateAction.INSTANCE, new ResetFeatureStateRequest()).actionGet()
        );
        assertThat(
            ex.getMessage(),
            containsString(
                "Unable to reset machine learning feature as there are ingest pipelines still referencing trained machine learning models"
            )
        );
        client().execute(DeletePipelineAction.INSTANCE, new DeletePipelineRequest("feature_reset_failure_inference_pipeline")).actionGet();
        createdPipelines.remove("feature_reset_failure_inference_pipeline");
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

        DataFrameAnalyticsConfig config = buildAnalytics(
            jobId,
            sourceIndex,
            destIndex,
            null,
            new Classification(
                KEYWORD_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        PutDataFrameAnalyticsAction.Request request = new PutDataFrameAnalyticsAction.Request(config);
        client().execute(PutDataFrameAnalyticsAction.INSTANCE, request).actionGet();

        client().execute(StartDataFrameAnalyticsAction.INSTANCE, new StartDataFrameAnalyticsAction.Request(jobId));
    }

    private void putAndStartJob(String jobId) throws Exception {
        Job.Builder job = createScheduledJob(jobId);
        jobIds.add(job.getId());
        putJob(job);
        openJob(job.getId());
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));
    }

    private void startRealtime(String jobId) throws Exception {
        client().admin().indices().prepareCreate("data").addMapping("type", "time", "type=date").get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long lastWeek = now - 604800000;
        indexDocs(logger, "data", numDocs1, lastWeek, now);

        putAndStartJob(jobId);
        DatafeedConfig datafeedConfig = createDatafeed(jobId + "-datafeed", jobId, Collections.singletonList("data"));
        datafeedIds.add(datafeedConfig.getId());
        putDatafeed(datafeedConfig);
        startDatafeed(datafeedConfig.getId(), 0L, null);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(jobId);
            assertThat(dataCounts.getProcessedRecordCount(), is(equalTo(numDocs1)));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), is(equalTo(0L)));
        });

        long numDocs2 = randomIntBetween(2, 64);
        now = System.currentTimeMillis();
        indexDocs(logger, "data", numDocs2, now + 5000, now + 6000);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(jobId);
            assertThat(dataCounts.getProcessedRecordCount(), greaterThan(0L));
        }, 30, TimeUnit.SECONDS);
    }

    private void putTrainedModelIngestPipeline(String pipelineId) throws Exception {
        client().execute(
            PutPipelineAction.INSTANCE,
            new PutPipelineRequest(
                pipelineId,
                new BytesArray(
                    "{\n"
                        + "    \"processors\": [\n"
                        + "      {\n"
                        + "        \"inference\": {\n"
                        + "          \"inference_config\": {\"classification\":{}},\n"
                        + "          \"model_id\": \"lang_ident_model_1\",\n"
                        + "          \"field_map\": {}\n"
                        + "        }\n"
                        + "      }\n"
                        + "    ]\n"
                        + "  }"
                ),
                XContentType.JSON
            )
        ).actionGet();
    }

    private void indexDocForInference(String pipelineId) {
        client().prepareIndex("type", "foo")
            .setPipeline(pipelineId)
            .setSource("{\"text\": \"this is some plain text.\"}", XContentType.JSON)
            .get();
    }

}
