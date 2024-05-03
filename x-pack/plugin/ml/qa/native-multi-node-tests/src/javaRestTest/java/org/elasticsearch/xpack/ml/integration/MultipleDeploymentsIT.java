/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.ml.inference.assignment.AllocationStatus;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class MultipleDeploymentsIT extends PyTorchModelRestTestCase {

    @SuppressWarnings("unchecked")
    public void testDeployModelMultipleTimes() throws IOException {
        String baseModelId = "base-model";
        putAllModelParts(baseModelId);

        String forSearch = "for-search";
        startDeployment(baseModelId, forSearch, AllocationStatus.State.STARTED, 1, 1, Priority.LOW);

        Response inference = infer("my words", forSearch);
        assertOK(inference);

        String forIngest = "for-ingest";
        startDeployment(baseModelId, forIngest, AllocationStatus.State.STARTED, 1, 1, Priority.LOW);

        inference = infer("my words", forIngest);
        assertOK(inference);
        inference = infer("my words", forIngest);
        assertOK(inference);

        assertInferenceCountOnDeployment(1, forSearch);
        assertInferenceCountOnDeployment(2, forIngest);

        // infer by model Id
        inference = infer("my words", baseModelId);
        assertOK(inference);
        assertInferenceCountOnModel(4, baseModelId);

        inference = infer("my words", baseModelId);
        assertOK(inference);
        assertInferenceCountOnModel(5, baseModelId);

        stopDeployment(forSearch);
        stopDeployment(forIngest);

        Response statsResponse = getTrainedModelStats("_all");
        Map<String, Object> stats = entityAsMap(statsResponse);
        List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");
        assertThat(stats.toString(), trainedModelStats, hasSize(2));

        for (var statsMap : trainedModelStats) {
            // no deployment stats when the deployment is stopped
            assertNull(stats.toString(), statsMap.get("deployment_stats"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testGetStats() throws IOException {
        String undeployedModel1 = "undeployed_1";
        putAllModelParts(undeployedModel1);
        String undeployedModel2 = "undeployed_2";
        putAllModelParts(undeployedModel2);

        String modelWith1Deployment = "model-with-1-deployment";
        putAllModelParts(modelWith1Deployment);

        String modelWith2Deployments = "model-with-2-deployments";
        putAllModelParts(modelWith2Deployments);
        String forSearchDeployment = "for-search";
        startDeployment(modelWith2Deployments, forSearchDeployment, AllocationStatus.State.STARTED, 1, 1, Priority.LOW);

        String forIngestDeployment = "for-ingest";
        startDeployment(modelWith2Deployments, forIngestDeployment, AllocationStatus.State.STARTED, 1, 1, Priority.LOW);

        // deployment Id is the same as model
        startDeployment(modelWith1Deployment, modelWith1Deployment, AllocationStatus.State.STARTED, 1, 1, Priority.LOW);

        {
            Map<String, Object> stats = entityAsMap(getTrainedModelStats("_all"));
            List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");
            checkExpectedStats(
                List.of(
                    new Tuple<>(undeployedModel1, null),
                    new Tuple<>(undeployedModel2, null),
                    new Tuple<>(modelWith1Deployment, modelWith1Deployment),
                    new Tuple<>(modelWith2Deployments, forSearchDeployment),
                    new Tuple<>(modelWith2Deployments, forIngestDeployment)
                ),
                trainedModelStats,
                true
            );

            // check the sorted order
            assertEquals(trainedModelStats.get(0).get("model_id"), "lang_ident_model_1");
            assertEquals(trainedModelStats.get(1).get("model_id"), modelWith1Deployment);
            assertEquals(MapHelper.dig("deployment_stats.deployment_id", trainedModelStats.get(1)), modelWith1Deployment);
            assertEquals(trainedModelStats.get(2).get("model_id"), modelWith2Deployments);
            assertEquals(MapHelper.dig("deployment_stats.deployment_id", trainedModelStats.get(2)), forIngestDeployment);
            assertEquals(trainedModelStats.get(3).get("model_id"), modelWith2Deployments);
            assertEquals(MapHelper.dig("deployment_stats.deployment_id", trainedModelStats.get(3)), forSearchDeployment);
            assertEquals(trainedModelStats.get(4).get("model_id"), undeployedModel1);
            assertEquals(trainedModelStats.get(5).get("model_id"), undeployedModel2);
        }
        {
            Map<String, Object> stats = entityAsMap(getTrainedModelStats(modelWith1Deployment));
            List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");
            checkExpectedStats(List.of(new Tuple<>(modelWith1Deployment, modelWith1Deployment)), trainedModelStats);
        }
        {
            Map<String, Object> stats = entityAsMap(getTrainedModelStats(modelWith2Deployments));
            List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");
            checkExpectedStats(
                List.of(new Tuple<>(modelWith2Deployments, forSearchDeployment), new Tuple<>(modelWith2Deployments, forIngestDeployment)),
                trainedModelStats
            );
        }
        {
            Map<String, Object> stats = entityAsMap(getTrainedModelStats(forIngestDeployment));
            List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");
            checkExpectedStats(List.of(new Tuple<>(modelWith2Deployments, forIngestDeployment)), trainedModelStats);
        }
        {
            // wildcard model id matching
            Map<String, Object> stats = entityAsMap(getTrainedModelStats("model-with-*"));
            List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");
            checkExpectedStats(
                List.of(
                    new Tuple<>(modelWith1Deployment, modelWith1Deployment),
                    new Tuple<>(modelWith2Deployments, forSearchDeployment),
                    new Tuple<>(modelWith2Deployments, forIngestDeployment)
                ),
                trainedModelStats
            );
        }
        {
            // wildcard deployment id matching
            Map<String, Object> stats = entityAsMap(getTrainedModelStats("for-*"));
            List<Map<String, Object>> trainedModelStats = (List<Map<String, Object>>) stats.get("trained_model_stats");
            checkExpectedStats(
                List.of(new Tuple<>(modelWith2Deployments, forSearchDeployment), new Tuple<>(modelWith2Deployments, forIngestDeployment)),
                trainedModelStats
            );
        }
    }

    private void checkExpectedStats(List<Tuple<String, String>> modelDeploymentPairs, List<Map<String, Object>> trainedModelStats) {
        checkExpectedStats(modelDeploymentPairs, trainedModelStats, false);
    }

    private void checkExpectedStats(
        List<Tuple<String, String>> modelDeploymentPairs,
        List<Map<String, Object>> trainedModelStats,
        boolean plusOneForLangIdent
    ) {
        var concatenatedIds = new HashSet<String>();
        modelDeploymentPairs.forEach(t -> concatenatedIds.add(t.v1() + t.v2()));

        int expectedSize = modelDeploymentPairs.size();
        if (plusOneForLangIdent) {
            expectedSize++;
        }
        assertEquals(trainedModelStats.toString(), trainedModelStats.size(), expectedSize);
        for (var tmStats : trainedModelStats) {
            String modelId = (String) tmStats.get("model_id");
            String deploymentId = (String) XContentMapValues.extractValue("deployment_stats.deployment_id", tmStats);
            concatenatedIds.remove(modelId + deploymentId);
        }

        assertThat("Missing stats for " + concatenatedIds, concatenatedIds, empty());
    }

    private void putAllModelParts(String modelId) throws IOException {
        createPassThroughModel(modelId);
        putModelDefinition(modelId);
        putVocabulary(List.of("these", "are", "my", "words"), modelId);
    }

    private void putModelDefinition(String modelId) throws IOException {
        putModelDefinition(modelId, PyTorchModelIT.BASE_64_ENCODED_MODEL, PyTorchModelIT.RAW_MODEL_SIZE);
    }
}
