/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.InferenceIndexDocTypeField;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.elasticsearch.xpack.inference.InferencePlugin;
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
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

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

    private final int upgradedNodes;

    public ModelRegistryUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
        this.upgradedNodes = upgradedNodes;
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
        } else if (isMixedCluster()) {
            // The core scenario of the mapping-update-on-write path: creating a NEW endpoint on an
            // upgraded node while old-version nodes (typically including the elected master) are
            // still in the cluster. The write must first upgrade the .inference mappings via the
            // origin-carrying put-mapping, which an old-version master is required to accept even
            // though the mappings are ahead of its own descriptor.
            // Node 0 is always the first node to be upgraded.
            try (
                RestClient upgradedNodeClient = buildClient(
                    restClientSettings(),
                    new HttpHost[] { HttpHost.create("http://" + getUpgradeCluster().getHttpAddress(0)) }
                )
            ) {
                String inferenceId = "test-inference-mixed-" + upgradedNodes;
                try {
                    embeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse(randomIntBetween(2, 50))));
                    var putRequest = new Request("PUT", "_inference/text_embedding/" + inferenceId);
                    putRequest.setJsonEntity(embeddingConfig(getUrl(embeddingsServer)));
                    assertOK(upgradedNodeClient.performRequest(putRequest));
                } finally {
                    embeddingsServer.clearRequests();
                }

                assertInferenceIndexMappingsAreCurrent(upgradedNodeClient);
                assertEndpointDocHasDocType(upgradedNodeClient, inferenceId);

                // Store the other document type that shares the .inference index and verify that
                // endpoint listings are not polluted by it.
                var putPolicyRequest = new Request("PUT", "_inference/_region_policy");
                putPolicyRequest.addParameter("force", "true");
                putPolicyRequest.setJsonEntity("{\"allowed_geos\": [\"us\"]}");
                assertOK(upgradedNodeClient.performRequest(putPolicyRequest));

                assertEndpointListingsContainOnlyEndpoints(upgradedNodeClient, inferenceId);
            }
        } else if (isUpgradedCluster()) {
            // check upgraded model in the cluster state
            assertBusy(() -> assertMinimalModelsAreUpgraded());
            deleteAll();
        }
    }

    /**
     * Asserts that {@code .inference}'s {@code _meta.managed_index_mappings_version} has reached this
     * node's latest descriptor version. Reaching it during the mixed phase proves the upgraded node
     * force-installed its latest mappings past the old-version master.
     */
    @SuppressWarnings("unchecked")
    private void assertInferenceIndexMappingsAreCurrent(RestClient upgradedNodeClient) throws IOException {
        var request = new Request("GET", "/.inference/_mapping");
        allowSystemIndexAccessWarnings(request);
        var response = entityAsMap(upgradedNodeClient.performRequest(request));
        // The response is keyed by the concrete index name backing ".inference".
        var indexMappings = (Map<String, Object>) response.values().iterator().next();
        var version = XContentMapValues.extractValue("mappings._meta.managed_index_mappings_version", indexMappings);
        assertNotNull("Expected a managed mappings version in the .inference _meta", version);
        int latestMappingsVersion = InferencePlugin.createInferenceIndexDescriptor(InferenceIndex.settings())
            .getMappingsVersion()
            .version();
        assertThat(((Number) version).intValue(), equalTo(latestMappingsVersion));
    }

    /** Asserts the endpoint document stored during the mixed phase carries the {@code doc_type} field. */
    @SuppressWarnings("unchecked")
    private void assertEndpointDocHasDocType(RestClient upgradedNodeClient, String inferenceId) throws IOException {
        var refreshRequest = new Request("POST", "/.inference/_refresh");
        allowSystemIndexAccessWarnings(refreshRequest);
        assertOK(upgradedNodeClient.performRequest(refreshRequest));

        var searchRequest = new Request("GET", "/.inference/_search");
        allowSystemIndexAccessWarnings(searchRequest);
        // Endpoint config documents store their inference id in the index-only "model_id" field.
        searchRequest.setJsonEntity(Strings.format("{\"query\":{\"term\":{\"model_id\":\"%s\"}}}", inferenceId));
        var response = entityAsMap(upgradedNodeClient.performRequest(searchRequest));
        var hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", response);
        assertThat("Expected exactly one config document for [" + inferenceId + "]", hits, hasSize(1));
        var source = (Map<String, Object>) hits.get(0).get("_source");
        assertThat(
            "Endpoint documents created after the mapping upgrade must be typed",
            source.get(InferenceIndexDocTypeField.DOC_TYPE_FIELD),
            equalTo(InferenceIndexDocTypeField.ENDPOINT_CONFIG_TYPE)
        );
    }

    /**
     * Asserts {@code GET _inference/_all} returns only endpoint documents: the endpoints created by
     * this test (old and mixed phases) plus preconfigured defaults — in particular, the region policy
     * document sharing the {@code .inference} index must not be classified as an endpoint.
     */
    @SuppressWarnings("unchecked")
    private void assertEndpointListingsContainOnlyEndpoints(RestClient upgradedNodeClient, String mixedPhaseInferenceId)
        throws IOException {
        var response = entityAsMap(upgradedNodeClient.performRequest(new Request("GET", "_inference/_all")));
        var endpoints = (List<Map<String, Object>>) response.get("endpoints");
        var nonDefaultIds = endpoints.stream()
            .map(endpoint -> (String) endpoint.get("inference_id"))
            .filter(id -> id.startsWith(".") == false)
            .toList();
        assertThat(nonDefaultIds, hasItem(mixedPhaseInferenceId));
        for (String id : nonDefaultIds) {
            assertThat("Only endpoints created by this test may be listed as endpoints", id, startsWith("test-inference-"));
        }
    }

    private static void allowSystemIndexAccessWarnings(Request request) {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
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
