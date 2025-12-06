/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.application.HuggingFaceServiceUpgradeIT.elserConfig;
import static org.elasticsearch.xpack.application.HuggingFaceServiceUpgradeIT.elserResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ModelRegistryUpgradeIT extends InferenceUpgradeTestCase {
    private static MockWebServer embeddingsServer;
    private static MockWebServer elserServer;

    @BeforeClass
    public static void startWebServer() throws IOException {
        embeddingsServer = new MockWebServer();
        embeddingsServer.start();

        elserServer = new MockWebServer();
        elserServer.start();
    }

    @AfterClass
    public static void shutdown() {
        embeddingsServer.close();
        elserServer.close();
    }

    public ModelRegistryUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testUpgradeModels() throws Exception {
        if (isOldCluster()) {
            int numModels = randomIntBetween(5, 10);
            for (int i = 0; i < numModels; i++) {
                var taskType = randomFrom(TaskType.TEXT_EMBEDDING, TaskType.SPARSE_EMBEDDING);
                if (taskType == TaskType.TEXT_EMBEDDING) {
                    int numDimensions = randomIntBetween(2, 50);
                    try {
                        embeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse(numDimensions)));
                        put("test-inference-" + i, embeddingConfig(getUrl(embeddingsServer)), taskType);
                    } finally {
                        embeddingsServer.clearRequests();
                    }
                } else {
                    try {
                        elserServer.enqueue(new MockResponse().setResponseCode(200).setBody(elserResponse()));
                        put("test-inference-" + i, elserConfig(getUrl(elserServer)), taskType);
                    } finally {
                        elserServer.clearRequests();
                    }
                }
            }
        } else if (isUpgradedCluster()) {
            // check upgraded model in the cluster state
            assertBusy(() -> assertMinimalModelsAreUpgraded());
            deleteAll();
        }
    }

    @SuppressWarnings("unchecked")
    private void assertMinimalModelsAreUpgraded() throws IOException {
        var fullModels = (List<Map<String, Object>>) get(TaskType.ANY, "*").get("endpoints");
        var minimalModels = getMinimalConfigs();
        assertMinimalModelsAreUpgraded(
            fullModels.stream().collect(Collectors.toMap(a -> (String) a.get("inference_id"), a -> a)),
            minimalModels
        );
    }

    @SuppressWarnings("unchecked")
    private void assertMinimalModelsAreUpgraded(
        Map<String, Map<String, Object>> fullModelsWithDefaults,
        Map<String, Map<String, Object>> minimalModels
    ) {
        // remove the default models as they are not stored in cluster state.
        var fullModels = fullModelsWithDefaults.entrySet()
            .stream()
            .filter(e -> e.getKey().startsWith(".") == false)
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        assertThat(fullModels.size(), greaterThan(0));
        assertThat(fullModels.size(), equalTo(minimalModels.size()));
        for (var entry : fullModels.entrySet()) {
            var fullModel = entry.getValue();
            var fullModelSettings = (Map<String, Object>) fullModel.get("service_settings");
            var minimalModelSettings = minimalModels.get(entry.getKey());
            assertNotNull(minimalModelSettings);

            assertThat(minimalModelSettings.get("service"), equalTo(fullModel.get("service")));
            assertThat(minimalModelSettings.get("task_type"), equalTo(fullModel.get("task_type")));
            var taskType = TaskType.fromString((String) minimalModelSettings.get("task_type"));
            if (taskType == TaskType.TEXT_EMBEDDING) {
                assertNotNull(minimalModelSettings.get("dimensions"));
                assertNotNull(minimalModelSettings.get("similarity"));
                // For default models, dimensions and similarity are not exposed since they are predefined.
                if (fullModelSettings.containsKey("dimensions")) {
                    assertThat(minimalModelSettings.get("dimensions"), equalTo(fullModelSettings.get("dimensions")));
                }
                if (fullModelSettings.containsKey("similarity")) {
                    assertThat(minimalModelSettings.get("similarity"), equalTo(fullModelSettings.get("similarity")));
                }
            }
        }
    }

    private String embeddingResponse(int numDimensions) {
        StringBuilder result = new StringBuilder();
        result.append("[[");
        for (int i = 0; i < numDimensions; i++) {
            if (i > 0) {
                result.append(", ");
            }
            result.append(randomFloat());
        }
        result.append("]]");
        return result.toString();
    }

    static String embeddingConfig(String url) {
        return Strings.format("""
            {
                "service": "hugging_face",
                "service_settings": {
                    "url": "%s",
                    "api_key": "XXXX"
                }
            }
            """, url, randomFrom(DenseVectorFieldMapper.ElementType.values()), randomFrom(SimilarityMeasure.values()));
    }
}
