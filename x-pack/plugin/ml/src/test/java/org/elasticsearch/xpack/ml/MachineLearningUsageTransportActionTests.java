/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsActionResponseTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStatsTests;
import org.elasticsearch.xpack.core.ml.stats.StatsAccumulator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class MachineLearningUsageTransportActionTests extends ESTestCase {
    public void testAddTrainedModelStatsHandlesMultipleDeployments() {
        Map<String, Object> usage = new HashMap<>();

        var deploymentConfig = TrainedModelConfigTests.createTestInstance("id1").build();
        var stats = new GetTrainedModelsStatsAction.Response.TrainedModelStats(
            "id1",
            TrainedModelSizeStatsTests.createRandom(),
            GetTrainedModelsStatsActionResponseTests.randomIngestStats(),
            randomIntBetween(0, 10),
            null,
            null
        );
        StatsAccumulator actualMemoryUsage = new StatsAccumulator();
        actualMemoryUsage.add(stats.getModelSizeStats().getModelSizeBytes());

        var modelsResponse = new GetTrainedModelsAction.Response(
            new QueryPage<>(List.of(deploymentConfig), 1, GetTrainedModelsAction.Response.RESULTS_FIELD)
        );

        var statsResponse = new GetTrainedModelsStatsAction.Response(
            new QueryPage<>(List.of(stats), 1, GetTrainedModelsStatsAction.Response.RESULTS_FIELD)
        );

        MachineLearningUsageTransportAction.addTrainedModelStats(modelsResponse, statsResponse, usage);
        @SuppressWarnings("unchecked")
        var expectedModelMemoryUsage = ((Map<String, Object>) usage.get("trained_models")).get(
            TrainedModelConfig.MODEL_SIZE_BYTES.getPreferredName()
        );
        assertThat(expectedModelMemoryUsage, is(actualMemoryUsage.asMap()));
    }
}
