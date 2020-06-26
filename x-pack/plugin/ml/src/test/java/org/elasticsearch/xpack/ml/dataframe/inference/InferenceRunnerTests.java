/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.inference;

import org.elasticsearch.client.Client;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RegressionTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelInput;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.inference.InferenceDefinition;
import org.elasticsearch.xpack.ml.dataframe.stats.ProgressTracker;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelProvider;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceRunnerTests extends ESTestCase {

    private Client client;
    private TrainedModelProvider trainedModelProvider;
    private ResultsPersisterService resultsPersisterService;
    private DataFrameAnalyticsConfig config;
    private ProgressTracker progressTracker;
    private TaskId parentTaskId;

    @Before
    public void setupTests() {
        client = mock(Client.class);
        trainedModelProvider = mock(TrainedModelProvider.class);
        resultsPersisterService = mock(ResultsPersisterService.class);
        config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setAnalysis(RegressionTests.createRandom())
            .setSource(new DataFrameAnalyticsSource(new String[] {"source_index"}, null, null))
            .setDest(new DataFrameAnalyticsDest("dest_index", "test_results_field"))
            .build();
        progressTracker = ProgressTracker.fromZeroes(config.getAnalysis().getProgressPhases(), config.getAnalysis().supportsInference());
        parentTaskId = new TaskId(randomAlphaOfLength(10), randomLong());
    }

    public void test() {
        Map<String, String> fieldMap = new HashMap<>();
        TrainedModelConfig trainedModelConfig = mock(TrainedModelConfig.class);
        when(trainedModelConfig.getDefaultFieldMap()).thenReturn(fieldMap);

        TrainedModelInput trainedModelInput = new TrainedModelInput(Arrays.asList("foo"));
        when(trainedModelConfig.getInput()).thenReturn(trainedModelInput);

        InferenceConfig inferenceConfig = config.getAnalysis().inferenceConfig(null);

        InferenceDefinition inferenceDefinition = mock(InferenceDefinition.class);

        TestDocsIterator testDocsIterator = mock(TestDocsIterator.class);

        InferenceRunner inferenceRunner = createInferenceRunner();
        inferenceRunner.inferTestDocs(trainedModelConfig, inferenceConfig, inferenceDefinition, testDocsIterator);


    }

    private InferenceRunner createInferenceRunner() {
        return new InferenceRunner(client, trainedModelProvider, resultsPersisterService, parentTaskId, config, progressTracker);
    }
}
