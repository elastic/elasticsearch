/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InferenceUpdateElasticsearchInternalServiceModelIT extends CustomElandModelIT {
    private final List<AdaptiveAllocationsSettings> ADAPTIVE_ALLOCATIONS_SETTINGS = List.of(
        new AdaptiveAllocationsSettings(randomBoolean(), null, null),
        new AdaptiveAllocationsSettings(null, randomIntBetween(1, 10), null),
        new AdaptiveAllocationsSettings(null, null, randomIntBetween(1, 10)),
        new AdaptiveAllocationsSettings(randomBoolean(), randomIntBetween(1, 10), randomIntBetween(11, 20))
    );

    public void testUpdateNumThreads() throws IOException {
        testUpdateElasticsearchInternalServiceEndpoint(
            Optional.of(randomIntBetween(2, 10)),
            Optional.empty(),
            Optional.empty(),
            Optional.empty()
        );
    }

    public void testUpdateAdaptiveAllocationsSettings() throws IOException {
        for (AdaptiveAllocationsSettings settings : ADAPTIVE_ALLOCATIONS_SETTINGS) {
            testUpdateElasticsearchInternalServiceEndpoint(
                Optional.empty(),
                Optional.ofNullable(settings.getEnabled()),
                Optional.ofNullable(settings.getMinNumberOfAllocations()),
                Optional.ofNullable(settings.getMaxNumberOfAllocations())
            );
        }
    }

    public void testUpdateNumAllocationsAndAdaptiveAllocationsSettings() throws IOException {
        testUpdateElasticsearchInternalServiceEndpoint(
            Optional.of(randomIntBetween(2, 10)),
            Optional.of(randomBoolean()),
            Optional.of(randomIntBetween(1, 10)),
            Optional.of(randomIntBetween(11, 20))
        );
    }

    private void testUpdateElasticsearchInternalServiceEndpoint(
        Optional<Integer> updatedNumAllocations,
        Optional<Boolean> updatedAdaptiveAllocationsEnabled,
        Optional<Integer> updatedMinNumberOfAllocations,
        Optional<Integer> updatedMaxNumberOfAllocations
    ) throws IOException {
        var inferenceId = "update-adaptive-allocations-inference";
        var originalEndpoint = setupInferenceEndpoint(inferenceId);
        verifyEndpointConfig(originalEndpoint, 1, Optional.empty(), Optional.empty(), Optional.empty());

        var updateConfig = generateUpdateConfig(
            updatedNumAllocations,
            updatedAdaptiveAllocationsEnabled,
            updatedMinNumberOfAllocations,
            updatedMaxNumberOfAllocations
        );
        var updatedEndpoint = updateEndpoint(inferenceId, updateConfig, TaskType.SPARSE_EMBEDDING);
        verifyEndpointConfig(
            updatedEndpoint,
            updatedNumAllocations.orElse(1),
            updatedAdaptiveAllocationsEnabled,
            updatedMinNumberOfAllocations,
            updatedMaxNumberOfAllocations
        );
    }

    private Map<String, Object> setupInferenceEndpoint(String inferenceId) throws IOException {
        String modelId = "custom-text-expansion-model";
        createMlNodeTextExpansionModel(modelId, client());

        var inferenceConfig = """
            {
              "service": "elasticsearch",
              "service_settings": {
                "model_id": "custom-text-expansion-model",
                "num_allocations": 1,
                "num_threads": 1
              }
            }
            """;

        return putModel(inferenceId, inferenceConfig, TaskType.SPARSE_EMBEDDING);
    }

    public static String generateUpdateConfig(
        Optional<Integer> numAllocations,
        Optional<Boolean> adaptiveAllocationsEnabled,
        Optional<Integer> minNumberOfAllocations,
        Optional<Integer> maxNumberOfAllocations
    ) {
        StringBuilder requestBodyBuilder = new StringBuilder();
        requestBodyBuilder.append("{ \"service_settings\": {");

        numAllocations.ifPresent(value -> requestBodyBuilder.append("\"num_allocations\": ").append(value).append(","));

        if (adaptiveAllocationsEnabled.isPresent() || minNumberOfAllocations.isPresent() || maxNumberOfAllocations.isPresent()) {
            requestBodyBuilder.append("\"adaptive_allocations\": {");
            adaptiveAllocationsEnabled.ifPresent(value -> requestBodyBuilder.append("\"enabled\": ").append(value).append(","));
            minNumberOfAllocations.ifPresent(
                value -> requestBodyBuilder.append("\"min_number_of_allocations\": ").append(value).append(",")
            );
            maxNumberOfAllocations.ifPresent(
                value -> requestBodyBuilder.append("\"max_number_of_allocations\": ").append(value).append(",")
            );

            if (requestBodyBuilder.charAt(requestBodyBuilder.length() - 1) == ',') {
                requestBodyBuilder.deleteCharAt(requestBodyBuilder.length() - 1);
            }
            requestBodyBuilder.append("},");
        }

        if (requestBodyBuilder.charAt(requestBodyBuilder.length() - 1) == ',') {
            requestBodyBuilder.deleteCharAt(requestBodyBuilder.length() - 1);
        }

        requestBodyBuilder.append("} }");
        return requestBodyBuilder.toString();
    }

    @SuppressWarnings("unchecked")
    private void verifyEndpointConfig(
        Map<String, Object> endpointConfig,
        int expectedNumAllocations,
        Optional<Boolean> adaptiveAllocationsEnabled,
        Optional<Integer> minNumberOfAllocations,
        Optional<Integer> maxNumberOfAllocations
    ) {
        var serviceSettings = (Map<String, Object>) endpointConfig.get("service_settings");

        assertEquals(expectedNumAllocations, serviceSettings.get("num_allocations"));
        if (adaptiveAllocationsEnabled.isPresent() || minNumberOfAllocations.isPresent() || maxNumberOfAllocations.isPresent()) {
            var adaptiveAllocations = (Map<String, Object>) serviceSettings.get("adaptive_allocations");
            adaptiveAllocationsEnabled.ifPresent(enabled -> assertEquals(enabled, adaptiveAllocations.get("enabled")));
            minNumberOfAllocations.ifPresent(min -> assertEquals(min, adaptiveAllocations.get("min_number_of_allocations")));
            maxNumberOfAllocations.ifPresent(max -> assertEquals(max, adaptiveAllocations.get("max_number_of_allocations")));
        }
    }
}
