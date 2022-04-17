/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction.Response.MlMemoryStats;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelType;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.BertTokenization;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.Tokenization;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.inference.nlp.tokenizers.BertTokenizer;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.After;

import java.util.Base64;
import java.util.List;

import static org.elasticsearch.xpack.ml.integration.ClassificationIT.KEYWORD_FIELD;
import static org.elasticsearch.xpack.ml.integration.PyTorchModelIT.BASE_64_ENCODED_MODEL;
import static org.elasticsearch.xpack.ml.integration.PyTorchModelIT.RAW_MODEL_SIZE;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class MlMemoryIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    @After
    public void cleanUpAfterTest() {
        cleanUp();
    }

    public void testMemoryStats() throws Exception {

        deployTrainedModel();
        openAnomalyDetectionJob();
        String dfaJobId = "dfa";
        startDataFrameAnalyticsJob(dfaJobId);

        MlMemoryAction.Response response = client().execute(MlMemoryAction.INSTANCE, new MlMemoryAction.Request("_all")).actionGet();

        assertThat(response.failures(), empty());

        List<MlMemoryStats> statsList = response.getNodes();
        // There are 4 nodes: 3 in the external cluster plus the test harness
        assertThat(statsList, hasSize(4));

        int mlNodes = 0;
        int nodesWithPytorchModel = 0;
        int nodesWithAnomalyJob = 0;
        int nodesWithDfaJob = 0;

        for (MlMemoryStats stats : statsList) {
            assertThat(stats.getMemTotal().getBytes(), greaterThan(0L));
            assertThat(stats.getMemAdjustedTotal().getBytes(), greaterThan(0L));
            assertThat(stats.getMemAdjustedTotal().getBytes(), lessThanOrEqualTo(stats.getMemTotal().getBytes()));
            boolean isMlNode = stats.getNode().getRoles().contains(DiscoveryNodeRole.ML_ROLE);
            boolean hasPyTorchModel = (stats.getMlNativeInference().getBytes() > 0);
            boolean hasAnomalyJob = (stats.getMlAnomalyDetectors().getBytes() > 0);
            boolean hasDfaJob = (stats.getMlDataFrameAnalytics().getBytes() > 0);
            if (isMlNode) {
                ++mlNodes;
                assertThat(stats.getMlMax().getBytes(), greaterThan(0L));
                if (hasPyTorchModel) {
                    ++nodesWithPytorchModel;
                }
                if (hasAnomalyJob) {
                    ++nodesWithAnomalyJob;
                }
                if (hasDfaJob) {
                    ++nodesWithDfaJob;
                }
            } else {
                assertThat(stats.getMlMax().getBytes(), equalTo(0L));
                assertThat(stats.getMlAnomalyDetectors().getBytes(), equalTo(0L));
                assertThat(stats.getMlDataFrameAnalytics().getBytes(), equalTo(0L));
                assertThat(stats.getMlNativeInference().getBytes(), equalTo(0L));
                assertThat(stats.getMlNativeCodeOverhead().getBytes(), equalTo(0L));
            }
            if (hasAnomalyJob || hasDfaJob || hasPyTorchModel) {
                assertThat(stats.getMlNativeCodeOverhead().getBytes(), greaterThan(0L));
            } else {
                assertThat(stats.getMlNativeCodeOverhead().getBytes(), equalTo(0L));
            }
            assertThat(stats.getJvmHeapMax().getBytes(), greaterThan(0L));
            assertThat(stats.getJvmInferenceMax().getBytes(), greaterThan(0L));
            // This next one has to be >= 0 rather than 0 because the cache is invalidated
            // lazily after models are no longer in use, and previous tests could have
            // caused a model to be cached.
            assertThat(stats.getJvmInference().getBytes(), greaterThanOrEqualTo(0L));
        }
        assertThat(mlNodes, is(2));
        assertThat(nodesWithPytorchModel, equalTo(mlNodes));
        assertThat(nodesWithAnomalyJob, is(1));
        // It's possible that the DFA job could have finished before the stats call was made
        assumeFalse(
            "Data frame analytics job finished really quickly, so cannot assert DFA memory stats",
            getProgress(dfaJobId).stream().allMatch(phaseProgress -> phaseProgress.getProgressPercent() == 100)
        );
        assertThat(nodesWithDfaJob, is(1));
    }

    private void openAnomalyDetectionJob() throws Exception {
        Job.Builder job = BaseMlIntegTestCase.createFareQuoteJob("ad", ByteSizeValue.ofMb(20));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId())).actionGet();
        assertBusy(() -> {
            GetJobsStatsAction.Response response = client().execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(job.getId())
            ).actionGet();
            assertEquals(JobState.OPENED, response.getResponse().results().get(0).getState());
        });
    }

    private void startDataFrameAnalyticsJob(String jobId) throws Exception {
        String sourceIndex = "source";
        String destIndex = "dest";
        ClassificationIT.createIndex(sourceIndex, false);
        ClassificationIT.indexData(sourceIndex, 350, 0, KEYWORD_FIELD);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Classification(KEYWORD_FIELD));
        putAnalytics(config);

        NodeAcknowledgedResponse response = startAnalytics(jobId);
        assertThat(response.getNode(), not(emptyString()));

        waitUntilSomeProgressHasBeenMadeForPhase(jobId, "loading_data");
    }

    private void deployTrainedModel() {
        String modelId = "pytorch";
        client().execute(
            PutTrainedModelAction.INSTANCE,
            new PutTrainedModelAction.Request(
                TrainedModelConfig.builder()
                    .setModelType(TrainedModelType.PYTORCH)
                    .setInferenceConfig(
                        new PassThroughConfig(null, new BertTokenization(null, false, null, Tokenization.Truncate.NONE, -1), null)
                    )
                    .setModelId(modelId)
                    .build(),
                false
            )
        ).actionGet();
        client().execute(
            PutTrainedModelDefinitionPartAction.INSTANCE,
            new PutTrainedModelDefinitionPartAction.Request(
                modelId,
                new BytesArray(Base64.getDecoder().decode(BASE_64_ENCODED_MODEL)),
                0,
                RAW_MODEL_SIZE,
                1
            )
        ).actionGet();
        client().execute(
            PutTrainedModelVocabularyAction.INSTANCE,
            new PutTrainedModelVocabularyAction.Request(
                modelId,
                List.of("these", "are", "my", "words", BertTokenizer.UNKNOWN_TOKEN, BertTokenizer.PAD_TOKEN),
                List.of()
            )
        ).actionGet();
        client().execute(
            StartTrainedModelDeploymentAction.INSTANCE,
            new StartTrainedModelDeploymentAction.Request(modelId).setWaitForState(AllocationStatus.State.STARTED)
        ).actionGet();
    }

    @Override
    boolean supportsInference() {
        return true;
    }
}
