/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsActionResponseTests;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStatsTests;
import org.elasticsearch.xpack.core.ml.stats.MlConfigSizeUsage;
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

    public void testAddTrainedModelStatsHandlesNullModelSizeStats() {
        Map<String, Object> usage = new HashMap<>();

        var deploymentConfig = TrainedModelConfigTests.createTestInstance("id1").build();
        var stats = new GetTrainedModelsStatsAction.Response.TrainedModelStats(
            "id1",
            null, // set modelSizeStats as null
            GetTrainedModelsStatsActionResponseTests.randomIngestStats(),
            randomIntBetween(0, 10),
            null,
            null
        );

        var modelsResponse = new GetTrainedModelsAction.Response(
            new QueryPage<>(List.of(deploymentConfig), 1, GetTrainedModelsAction.Response.RESULTS_FIELD)
        );

        var statsResponse = new GetTrainedModelsStatsAction.Response(
            new QueryPage<>(List.of(stats), 1, GetTrainedModelsStatsAction.Response.RESULTS_FIELD)
        );

        MachineLearningUsageTransportAction.addTrainedModelStats(modelsResponse, statsResponse, usage);

        StatsAccumulator emptyAccumulator = new StatsAccumulator();
        @SuppressWarnings("unchecked")
        var actualModelMemoryUsage = ((Map<String, Object>) usage.get("trained_models")).get(
            TrainedModelConfig.MODEL_SIZE_BYTES.getPreferredName()
        );
        assertThat(actualModelMemoryUsage, is(emptyAccumulator.asMap()));
    }

    public void testAddTrainedModelStatsIncludesConfigSizes() {
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
        var modelsResponse = new GetTrainedModelsAction.Response(
            new QueryPage<>(List.of(deploymentConfig), 1, GetTrainedModelsAction.Response.RESULTS_FIELD)
        );
        var statsResponse = new GetTrainedModelsStatsAction.Response(
            new QueryPage<>(List.of(stats), 1, GetTrainedModelsStatsAction.Response.RESULTS_FIELD)
        );

        MachineLearningUsageTransportAction.addTrainedModelStats(modelsResponse, statsResponse, usage);

        @SuppressWarnings("unchecked")
        Map<String, Object> trainedModelsUsage = (Map<String, Object>) usage.get("trained_models");
        assertThat(trainedModelsUsage.containsKey(MlConfigSizeUsage.CONFIG_SIZES), is(true));
    }

    @TestLogging(value = "org.elasticsearch.xpack.ml.MachineLearningUsageTransportAction:DEBUG", reason = "asserting DEBUG output")
    public void testInferenceEndpointsFailureLoggedAtDebugForExpectedCauses() {
        var searchFailure = new SearchPhaseExecutionException("query", "all shards failed", ShardSearchFailure.EMPTY_ARRAY);
        assertInferenceEndpointsFailureLevel(searchFailure, Level.DEBUG);
        assertInferenceEndpointsFailureLevel(new MasterNotDiscoveredException(), Level.DEBUG);
        assertInferenceEndpointsFailureLevel(new RemoteTransportException("remote", searchFailure), Level.DEBUG);
        assertInferenceEndpointsFailureLevel(new ElasticsearchException("wrapper", new MasterNotDiscoveredException()), Level.DEBUG);
    }

    @TestLogging(value = "org.elasticsearch.xpack.ml.MachineLearningUsageTransportAction:DEBUG", reason = "asserting DEBUG output")
    public void testInferenceEndpointsFailureLoggedAtWarnForOtherCauses() {
        assertInferenceEndpointsFailureLevel(new IllegalStateException("boom"), Level.WARN);
        assertInferenceEndpointsFailureLevel(new RemoteTransportException("remote", new IllegalStateException("boom")), Level.WARN);
    }

    private static void assertInferenceEndpointsFailureLevel(Exception failure, Level expected) {
        Level other = expected == Level.WARN ? Level.DEBUG : Level.WARN;
        try (var mockLog = MockLog.capture(MachineLearningUsageTransportAction.class)) {
            String loggerName = MachineLearningUsageTransportAction.class.getCanonicalName();
            String message = "Failed to get inference endpoints to include in ML usage";
            mockLog.addExpectation(new MockLog.SeenEventExpectation("expected level", loggerName, expected, message));
            mockLog.addExpectation(new MockLog.UnseenEventExpectation("other level", loggerName, other, message));
            MachineLearningUsageTransportAction.logInferenceEndpointsFailure(failure);
            mockLog.assertAllExpectationsMatched();
        }
    }
}
