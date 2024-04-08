/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.DeletePipelineTransportAction;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineTransportAction;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Strings;
import org.elasticsearch.license.GetFeatureUsageRequest;
import org.elasticsearch.license.GetFeatureUsageResponse;
import org.elasticsearch.license.TransportGetFeatureUsageAction;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.After;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.ml.MachineLearningField.ML_FEATURE_FAMILY;
import static org.elasticsearch.xpack.ml.inference.loadingservice.LocalModelTests.buildClassification;
import static org.elasticsearch.xpack.ml.integration.ModelInferenceActionIT.buildTrainedModelConfigBuilder;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TestFeatureLicenseTrackingIT extends MlSingleNodeTestCase {

    private final Set<String> createdPipelines = new HashSet<>();

    @After
    public void cleanup() throws Exception {
        for (String pipeline : createdPipelines) {
            try {
                client().execute(DeletePipelineTransportAction.TYPE, new DeletePipelineRequest(pipeline)).actionGet();
            } catch (Exception ex) {
                logger.warn(() -> "error cleaning up pipeline [" + pipeline + "]", ex);
            }
        }
        // Some of the tests have async side effects. We need to wait for these to complete before continuing
        // the cleanup, otherwise unexpected indices may get created during the cleanup process.
        BaseMlIntegTestCase.waitForPendingTasks(client());
    }

    public void testFeatureTrackingAnomalyJob() throws Exception {
        putAndStartJob("job-feature-usage");
        GetFeatureUsageResponse.FeatureUsageInfo mlFeatureUsage = getFeatureUsageInfo().stream()
            .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
            .filter(f -> f.getName().equals(MachineLearning.ML_ANOMALY_JOBS_FEATURE.getName()))
            .findAny()
            .orElse(null);
        assertThat(mlFeatureUsage, is(not(nullValue())));
        assertThat(mlFeatureUsage.getContext(), containsString("job-feature-usage"));
        // While the job is opened, the lastUsage moves forward to "now". Verify it does that
        ZonedDateTime lastUsage = mlFeatureUsage.getLastUsedTime();
        assertBusy(() -> {
            ZonedDateTime recentUsage = getFeatureUsageInfo().stream()
                .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
                .filter(f -> f.getName().equals(MachineLearning.ML_ANOMALY_JOBS_FEATURE.getName()))
                .map(GetFeatureUsageResponse.FeatureUsageInfo::getLastUsedTime)
                .findAny()
                .orElse(null);
            assertThat(recentUsage, is(not(nullValue())));
            assertThat(lastUsage.toInstant(), lessThan(recentUsage.toInstant()));
        });

        client().execute(CloseJobAction.INSTANCE, new CloseJobAction.Request("job-feature-usage")).actionGet();

        mlFeatureUsage = getFeatureUsageInfo().stream()
            .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
            .filter(f -> f.getName().equals(MachineLearning.ML_ANOMALY_JOBS_FEATURE.getName()))
            .findAny()
            .orElse(null);
        assertThat(mlFeatureUsage, is(not(nullValue())));
        assertThat(mlFeatureUsage.getContext(), containsString("job-feature-usage"));
        assertThat(mlFeatureUsage.getLastUsedTime(), is(not(nullValue())));

        ZonedDateTime lastUsageAfterClose = mlFeatureUsage.getLastUsedTime();

        assertBusy(() -> {
            ZonedDateTime recentUsage = getFeatureUsageInfo().stream()
                .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
                .filter(f -> f.getName().equals(MachineLearning.ML_ANOMALY_JOBS_FEATURE.getName()))
                .map(GetFeatureUsageResponse.FeatureUsageInfo::getLastUsedTime)
                .findAny()
                .orElse(null);
            assertThat(recentUsage, is(not(nullValue())));
            assertThat(lastUsageAfterClose.toInstant(), equalTo(recentUsage.toInstant()));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/102381")
    public void testFeatureTrackingInferenceModelPipeline() throws Exception {
        String modelId = "test-load-models-classification-license-tracking";
        Map<String, String> oneHotEncoding = new HashMap<>();
        oneHotEncoding.put("cat", "animal_cat");
        oneHotEncoding.put("dog", "animal_dog");
        TrainedModelConfig config = buildTrainedModelConfigBuilder(modelId).setInput(
            new TrainedModelInput(Arrays.asList("field.foo", "field.bar", "other.categorical"))
        )
            .setInferenceConfig(new ClassificationConfig(3))
            .setParsedDefinition(
                new TrainedModelDefinition.Builder().setPreProcessors(
                    List.of(new OneHotEncoding("other.categorical", oneHotEncoding, false))
                ).setTrainedModel(buildClassification(true))
            )
            .build();
        client().execute(PutTrainedModelAction.INSTANCE, new PutTrainedModelAction.Request(config, false)).actionGet();

        String pipelineId = "pipeline-inference-model-tracked";
        putTrainedModelIngestPipeline(pipelineId, modelId);
        createdPipelines.add(pipelineId);

        // wait for the feature to start being used
        assertBusy(() -> {
            GetFeatureUsageResponse.FeatureUsageInfo mlFeatureUsage = getFeatureUsageInfo().stream()
                .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
                .filter(f -> f.getName().equals(MachineLearning.ML_MODEL_INFERENCE_FEATURE.getName()))
                .findAny()
                .orElse(null);
            assertThat(mlFeatureUsage, is(not(nullValue())));
            assertThat(mlFeatureUsage.getContext(), containsString(modelId));
        });

        GetFeatureUsageResponse.FeatureUsageInfo mlFeatureUsage = getFeatureUsageInfo().stream()
            .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
            .filter(f -> f.getName().equals(MachineLearning.ML_MODEL_INFERENCE_FEATURE.getName()))
            .findAny()
            .orElse(null);
        assertThat(mlFeatureUsage, is(not(nullValue())));
        // While the model is referenced, the lastUsage moves forward to "now". Verify it does that
        ZonedDateTime lastUsage = mlFeatureUsage.getLastUsedTime();
        assertBusy(() -> {
            ZonedDateTime recentUsage = getFeatureUsageInfo().stream()
                .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
                .filter(f -> f.getName().equals(MachineLearning.ML_MODEL_INFERENCE_FEATURE.getName()))
                .map(GetFeatureUsageResponse.FeatureUsageInfo::getLastUsedTime)
                .findAny()
                .orElse(null);
            assertThat(recentUsage, is(not(nullValue())));
            assertThat(lastUsage.toInstant(), lessThan(recentUsage.toInstant()));
        });

        client().execute(DeletePipelineTransportAction.TYPE, new DeletePipelineRequest(pipelineId)).actionGet();
        createdPipelines.remove(pipelineId);

        // Make sure that feature usage keeps the last usage once the model is removed
        assertBusy(() -> {
            ZonedDateTime recentUsage = getFeatureUsageInfo().stream()
                .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
                .filter(f -> f.getName().equals(MachineLearning.ML_MODEL_INFERENCE_FEATURE.getName()))
                .map(GetFeatureUsageResponse.FeatureUsageInfo::getLastUsedTime)
                .findAny()
                .orElse(null);
            assertThat(recentUsage, is(not(nullValue())));
            ZonedDateTime secondRecentUsage = getFeatureUsageInfo().stream()
                .filter(f -> f.getFamily().equals(ML_FEATURE_FAMILY))
                .filter(f -> f.getName().equals(MachineLearning.ML_MODEL_INFERENCE_FEATURE.getName()))
                .map(GetFeatureUsageResponse.FeatureUsageInfo::getLastUsedTime)
                .findAny()
                .orElse(null);
            assertThat(secondRecentUsage, is(not(nullValue())));
            assertThat(secondRecentUsage.toInstant(), equalTo(recentUsage.toInstant()));
        });
    }

    private List<GetFeatureUsageResponse.FeatureUsageInfo> getFeatureUsageInfo() {
        return client().execute(TransportGetFeatureUsageAction.TYPE, new GetFeatureUsageRequest()).actionGet().getFeatures();
    }

    private void putAndStartJob(String jobId) throws Exception {
        Job.Builder job = createScheduledJob(jobId);
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(jobId)).actionGet();
        assertBusy(() -> assertEquals(getJobStats(job.getId()).get(0).getState(), JobState.OPENED));
    }

    private List<GetJobsStatsAction.Response.JobStats> getJobStats(String jobId) {
        GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).actionGet();
        return response.getResponse().results();
    }

    private void putTrainedModelIngestPipeline(String pipelineId, String modelId) throws Exception {
        client().execute(PutPipelineTransportAction.TYPE, new PutPipelineRequest(pipelineId, new BytesArray(Strings.format("""
            {
                "processors": [
                  {
                    "inference": {
                      "inference_config": {"classification":{}},
                      "model_id": "%s",
                      "field_map": {}
                    }
                  }
                ]
              }""", modelId)), XContentType.JSON)).actionGet();
    }

}
