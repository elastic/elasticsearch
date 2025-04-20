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
        assertNotNull(results.get("sparse_embedding"));

        var updatedNumAllocations = randomIntBetween(1, 10);
        var updatedEndpointConfig = updateEndpoint(inferenceId, updatedEndpointConfig(updatedNumAllocations), TaskType.SPARSE_EMBEDDING);
        assertThat(
            updatedEndpointConfig.get("service_settings"),
            is(
                Map.of(
                    "num_allocations",
                    updatedNumAllocations,
                    "num_threads",
                    1,
                    "model_id",
                    "attach_to_deployment",
                    "deployment_id",
                    "existing_deployment"
                )
            )
        );

        deleteModel(inferenceId);
        // assert deployment not stopped
        var stats = (List<Map<String, Object>>) getTrainedModelStats(modelId).get("trained_model_stats");
        var deploymentStats = stats.get(0).get("deployment_stats");
        assertNotNull(stats.toString(), deploymentStats);

        stopMlNodeDeployment(deploymentId);
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
        assertNotNull(results.get("sparse_embedding"));

        var updatedNumAllocations = randomIntBetween(1, 10);
        var updatedEndpointConfig = updateEndpoint(inferenceId, updatedEndpointConfig(updatedNumAllocations), TaskType.SPARSE_EMBEDDING);
        assertThat(
            updatedEndpointConfig.get("service_settings"),
            is(
                Map.of(
                    "num_allocations",
                    updatedNumAllocations,
                    "num_threads",
                    1,
                    "model_id",
                    "attach_with_model_id",
                    "deployment_id",
                    "existing_deployment_with_model_id"
                )
            )
        );

        stopMlNodeDeployment(deploymentId);
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

        assertStatusOkOrCreated(updateMlNodeDeploymemnt(deploymentId, 2));

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

    private String updatedEndpointConfig(int numAllocations) {
        return Strings.format("""
            {
              "service_settings": {
                "num_allocations": %d
              }
            }
            """, numAllocations);
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
        request.addParameter("force", "true");
        client().performRequest(request);
    }

    protected Map<String, Object> getTrainedModelStats(String modelId) throws IOException {
        Request request = new Request("GET", "/_ml/trained_models/" + modelId + "/_stats");
        return entityAsMap(client().performRequest(request));
    }
}
