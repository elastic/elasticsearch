/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class CreateFromDeploymentIT extends InferenceBaseRestTest {

    @SuppressWarnings("unchecked")
    public void testAttachToDeployment() throws IOException {
        var modelId = "attach_to_deployment";
        var deploymentId = "existing_deployment";

        CustomElandModelIT.createMlNodeTextExpansionModel(modelId, client());
        var response = startMlNodeDeploymemnt(modelId, deploymentId);
        assertStatusOkOrCreated(response);

        var inferenceId = "inference_on_existing_deployment";
        var putModel = putModel(inferenceId, endpointConfig(deploymentId), TaskType.SPARSE_EMBEDDING);
        var serviceSettings = putModel.get("service_settings");
        assertThat(
            putModel.toString(),
            serviceSettings,
            is(Map.of("num_allocations", 1, "num_threads", 1, "model_id", "attach_to_deployment", "deployment_id", "existing_deployment"))
        );

        var getModel = getModel(inferenceId);
        serviceSettings = getModel.get("service_settings");
        assertThat(
            getModel.toString(),
            serviceSettings,
            is(Map.of("num_allocations", 1, "num_threads", 1, "model_id", "attach_to_deployment", "deployment_id", "existing_deployment"))
        );

        var results = infer(inferenceId, List.of("washing machine"));
        assertNotNull(results.get(SparseEmbeddingResults.SPARSE_EMBEDDING));

        deleteModel(inferenceId);
        // assert deployment not stopped
        var stats = (List<Map<String, Object>>) getTrainedModelStats(modelId).get("trained_model_stats");
        var deploymentStats = stats.get(0).get("deployment_stats");
        assertNotNull(stats.toString(), deploymentStats);

        forceStopMlNodeDeployment(deploymentId);
    }

    public void testAttachWithModelId() throws IOException {
        var modelId = "attach_with_model_id";
        var deploymentId = "existing_deployment_with_model_id";

        CustomElandModelIT.createMlNodeTextExpansionModel(modelId, client());
        var response = startMlNodeDeploymemnt(modelId, deploymentId);
        assertStatusOkOrCreated(response);

        var inferenceId = "inference_on_existing_deployment";
        var putModel = putModel(inferenceId, endpointConfig(modelId, deploymentId), TaskType.SPARSE_EMBEDDING);
        var serviceSettings = putModel.get("service_settings");
        assertThat(
            putModel.toString(),
            serviceSettings,
            is(
                Map.of(
                    "num_allocations",
                    1,
                    "num_threads",
                    1,
                    "model_id",
                    "attach_with_model_id",
                    "deployment_id",
                    "existing_deployment_with_model_id"
                )
            )
        );

        var getModel = getModel(inferenceId);
        serviceSettings = getModel.get("service_settings");
        assertThat(
            getModel.toString(),
            serviceSettings,
            is(
                Map.of(
                    "num_allocations",
                    1,
                    "num_threads",
                    1,
                    "model_id",
                    "attach_with_model_id",
                    "deployment_id",
                    "existing_deployment_with_model_id"
                )
            )
        );

        var results = infer(inferenceId, List.of("washing machine"));
        assertNotNull(results.get(SparseEmbeddingResults.SPARSE_EMBEDDING));

        deleteModel(inferenceId);

        forceStopMlNodeDeployment(deploymentId);
    }

    public void testModelIdDoesNotMatch() throws IOException {
        var modelId = "attach_with_model_id";
        var deploymentId = "existing_deployment_with_model_id";
        var aDifferentModelId = "not_the_same_as_the_one_used_in_the_deployment";

        CustomElandModelIT.createMlNodeTextExpansionModel(modelId, client());
        var response = startMlNodeDeploymemnt(modelId, deploymentId);
        assertStatusOkOrCreated(response);

        var inferenceId = "inference_on_existing_deployment";
        var e = expectThrows(
            ResponseException.class,
            () -> putModel(inferenceId, endpointConfig(aDifferentModelId, deploymentId), TaskType.SPARSE_EMBEDDING)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "Deployment [existing_deployment_with_model_id] uses model [attach_with_model_id] "
                    + "which does not match the model [not_the_same_as_the_one_used_in_the_deployment] in the request."
            )
        );
    }

    public void testDeploymentDoesNotExist() {
        var deploymentId = "missing_deployment";

        var inferenceId = "inference_on_missing_deployment";
        var e = expectThrows(ResponseException.class, () -> putModel(inferenceId, endpointConfig(deploymentId), TaskType.SPARSE_EMBEDDING));
        assertThat(e.getMessage(), containsString("Cannot find deployment [missing_deployment]"));
    }

    public void testCreateInferenceUsingSameDeploymentId() throws IOException {
        var modelId = "conflicting_ids";
        var deploymentId = modelId;
        var inferenceId = modelId;

        CustomElandModelIT.createMlNodeTextExpansionModel(modelId, client());
        var response = startMlNodeDeploymemnt(modelId, deploymentId);
        assertStatusOkOrCreated(response);

        var responseException = assertThrows(
            ResponseException.class,
            () -> putModel(inferenceId, endpointConfig(deploymentId), TaskType.SPARSE_EMBEDDING)
        );
        assertThat(
            responseException.getMessage(),
            containsString(
                "Inference endpoint IDs must be unique. "
                    + "Requested inference endpoint ID [conflicting_ids] matches existing trained model ID(s) but must not."
            )
        );

        forceStopMlNodeDeployment(deploymentId);
    }

    public void testNumAllocationsIsUpdated() throws IOException {
        var modelId = "update_num_allocations";
        var deploymentId = modelId;

        CustomElandModelIT.createMlNodeTextExpansionModel(modelId, client());
        var response = startMlNodeDeploymemnt(modelId, deploymentId);
        assertStatusOkOrCreated(response);

        var inferenceId = "test_num_allocations_updated";
        var putModel = putModel(inferenceId, endpointConfig(deploymentId), TaskType.SPARSE_EMBEDDING);
        var serviceSettings = putModel.get("service_settings");
        assertThat(
            putModel.toString(),
            serviceSettings,
            is(
                Map.of(
                    "num_allocations",
                    1,
                    "num_threads",
                    1,
                    "model_id",
                    "update_num_allocations",
                    "deployment_id",
                    "update_num_allocations"
                )
            )
        );

        var responseException = assertThrows(ResponseException.class, () -> updateInference(inferenceId, TaskType.SPARSE_EMBEDDING, 2));
        assertThat(
            responseException.getMessage(),
            containsString(
                "Cannot update inference endpoint [test_num_allocations_updated] using model deployment [update_num_allocations]. "
                    + "The model deployment must be updated through the trained models API."
            )
        );

        updateMlNodeDeploymemnt(deploymentId, 2);

        var updatedServiceSettings = getModel(inferenceId).get("service_settings");
        assertThat(
            updatedServiceSettings.toString(),
            updatedServiceSettings,
            is(
                Map.of(
                    "num_allocations",
                    2,
                    "num_threads",
                    1,
                    "model_id",
                    "update_num_allocations",
                    "deployment_id",
                    "update_num_allocations"
                )
            )
        );

        deleteModel(inferenceId);
        forceStopMlNodeDeployment(deploymentId);
    }

    public void testUpdateWhenInferenceEndpointCreatesDeployment() throws IOException {
        var modelId = "update_num_allocations_from_created_endpoint";
        var inferenceId = "test_created_endpoint_from_model";
        var deploymentId = inferenceId;

        CustomElandModelIT.createMlNodeTextExpansionModel(modelId, client());

        var putModel = putModel(inferenceId, Strings.format("""
            {
              "service": "elasticsearch",
              "service_settings": {
                "num_allocations": %s,
                "num_threads": %s,
                "model_id": "%s"
              }
            }
            """, 1, 1, modelId), TaskType.SPARSE_EMBEDDING);
        var serviceSettings = putModel.get("service_settings");
        assertThat(putModel.toString(), serviceSettings, is(Map.of("num_allocations", 1, "num_threads", 1, "model_id", modelId)));

        updateInference(inferenceId, TaskType.SPARSE_EMBEDDING, 2);

        var responseException = assertThrows(ResponseException.class, () -> updateMlNodeDeploymemnt(deploymentId, 2));
        assertThat(
            responseException.getMessage(),
            containsString(
                "Cannot update deployment [test_created_endpoint_from_model] as it was created by inference endpoint "
                    + "[test_created_endpoint_from_model]. This model deployment must be updated through the inference API."
            )
        );

        var updatedServiceSettings = getModel(inferenceId).get("service_settings");
        assertThat(
            updatedServiceSettings.toString(),
            updatedServiceSettings,
            is(Map.of("num_allocations", 2, "num_threads", 1, "model_id", modelId))
        );

        deleteModel(inferenceId);
        forceStopMlNodeDeployment(deploymentId);
    }

    public void testCannotUpdateAnotherInferenceEndpointsCreatedDeployment() throws IOException {
        var modelId = "model_deployment_for_endpoint";
        var inferenceId = "first_endpoint_for_model_deployment";
        var deploymentId = inferenceId;

        CustomElandModelIT.createMlNodeTextExpansionModel(modelId, client());

        putModel(inferenceId, Strings.format("""
            {
              "service": "elasticsearch",
              "service_settings": {
                "num_allocations": %s,
                "num_threads": %s,
                "model_id": "%s"
              }
            }
            """, 1, 1, modelId), TaskType.SPARSE_EMBEDDING);

        var secondInferenceId = "second_endpoint_for_model_deployment";
        var putModel = putModel(secondInferenceId, endpointConfig(deploymentId), TaskType.SPARSE_EMBEDDING);
        var serviceSettings = putModel.get("service_settings");
        assertThat(
            putModel.toString(),
            serviceSettings,
            is(Map.of("num_allocations", 1, "num_threads", 1, "model_id", modelId, "deployment_id", deploymentId))
        );

        var responseException = assertThrows(
            ResponseException.class,
            () -> updateInference(secondInferenceId, TaskType.SPARSE_EMBEDDING, 2)
        );
        assertThat(
            responseException.getMessage(),
            containsString(
                "Cannot update inference endpoint [second_endpoint_for_model_deployment] for model deployment "
                    + "[first_endpoint_for_model_deployment] as it was created by another inference endpoint. "
                    + "The model can only be updated using inference endpoint id [first_endpoint_for_model_deployment]."
            )
        );

        deleteModel(inferenceId);
        deleteModel(secondInferenceId);
        forceStopMlNodeDeployment(deploymentId);
    }

    public void testStoppingDeploymentAttachedToInferenceEndpoint() throws IOException {
        var modelId = "try_stop_attach_to_deployment";
        var deploymentId = "test_stop_attach_to_deployment";

        CustomElandModelIT.createMlNodeTextExpansionModel(modelId, client());
        var response = startMlNodeDeploymemnt(modelId, deploymentId);
        assertStatusOkOrCreated(response);

        var inferenceId = "test_stop_inference_on_existing_deployment";
        putModel(inferenceId, endpointConfig(deploymentId), TaskType.SPARSE_EMBEDDING);

        var stopShouldNotSucceed = expectThrows(ResponseException.class, () -> stopMlNodeDeployment(deploymentId));
        assertThat(
            stopShouldNotSucceed.getMessage(),
            containsString(
                Strings.format("Cannot stop deployment [%s] as it is used by inference endpoint [%s]", deploymentId, inferenceId)
            )
        );

        deleteModel(inferenceId);
        // Force stop will stop the deployment
        forceStopMlNodeDeployment(deploymentId);
    }

    private String endpointConfig(String deploymentId) {
        return Strings.format("""
            {
              "service": "elasticsearch",
              "service_settings": {
                "deployment_id": "%s"
              }
            }
            """, deploymentId);
    }

    private String endpointConfig(String modelId, String deploymentId) {
        return Strings.format("""
            {
              "service": "elasticsearch",
              "service_settings": {
                "model_id": "%s",
                "deployment_id": "%s"
              }
            }
            """, modelId, deploymentId);
    }

    private Response startMlNodeDeploymemnt(String modelId, String deploymentId) throws IOException {
        String endPoint = "/_ml/trained_models/"
            + modelId
            + "/deployment/_start?timeout=10s&wait_for=started"
            + "&threads_per_allocation=1"
            + "&number_of_allocations=1";

        if (deploymentId != null) {
            endPoint = endPoint + "&deployment_id=" + deploymentId;
        }

        Request request = new Request("POST", endPoint);
        return client().performRequest(request);
    }

    private Response updateInference(String deploymentId, TaskType taskType, int numAllocations) throws IOException {
        String endPoint = Strings.format("/_inference/%s/%s/_update", taskType, deploymentId);

        var body = Strings.format("""
            {
              "service_settings": {
                "num_allocations": %d
              }
            }
            """, numAllocations);

        Request request = new Request("PUT", endPoint);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    private Response updateMlNodeDeploymemnt(String deploymentId, int numAllocations) throws IOException {
        String endPoint = "/_ml/trained_models/" + deploymentId + "/deployment/_update";

        var body = Strings.format("""
            {
              "number_of_allocations": %d
            }
            """, numAllocations);

        Request request = new Request("POST", endPoint);
        request.setJsonEntity(body);
        return client().performRequest(request);
    }

    protected void stopMlNodeDeployment(String deploymentId) throws IOException {
        String endpoint = "/_ml/trained_models/" + deploymentId + "/deployment/_stop";
        Request request = new Request("POST", endpoint);
        client().performRequest(request);
    }

    protected void forceStopMlNodeDeployment(String deploymentId) throws IOException {
        String endpoint = "/_ml/trained_models/" + deploymentId + "/deployment/_stop";
        Request request = new Request("POST", endpoint);
        request.addParameter("force", "true");
        client().performRequest(request);
    }

    protected Map<String, Object> getTrainedModelStats(String modelId) throws IOException {
        Request request = new Request("GET", "/_ml/trained_models/" + modelId + "/_stats");
        return entityAsMap(client().performRequest(request));
    }
}
