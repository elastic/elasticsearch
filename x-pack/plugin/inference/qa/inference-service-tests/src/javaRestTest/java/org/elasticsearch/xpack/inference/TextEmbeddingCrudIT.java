/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.plugins.Platforms;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

// This test was previously disabled in CI due to the models being too large
// See "https://github.com/elastic/elasticsearch/issues/105198".
public class TextEmbeddingCrudIT extends InferenceBaseRestTest {

    public void testPutE5Small_withNoModelVariant() {
        {
            String inferenceEntityId = randomAlphaOfLength(10).toLowerCase();
            expectThrows(
                org.elasticsearch.client.ResponseException.class,
                () -> putTextEmbeddingModel(inferenceEntityId, noModelIdVariantJsonEntity())
            );
        }
    }

    public void testPutE5Small_withPlatformAgnosticVariant() throws IOException {
        String inferenceEntityId = randomAlphaOfLength(10).toLowerCase();
        putTextEmbeddingModel(inferenceEntityId, platformAgnosticModelVariantJsonEntity());
        var models = getTrainedModel("_all");
        assertThat(models.toString(), containsString("deployment_id=" + inferenceEntityId));

        Map<String, Object> results = inferOnMockService(
            inferenceEntityId,
            TaskType.TEXT_EMBEDDING,
            List.of("hello world", "this is the second document")
        );
        assertTrue(((List) ((Map) ((List) results.get("text_embedding")).get(0)).get("embedding")).size() > 1);
        // there exists embeddings
        assertTrue(((List) results.get("text_embedding")).size() == 2);
        // there are two sets of embeddings
        deleteTextEmbeddingModel(inferenceEntityId);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/105198")
    public void testPutE5Small_withPlatformSpecificVariant() throws IOException {
        String inferenceEntityId = randomAlphaOfLength(10).toLowerCase();
        if ("linux-x86_64".equals(Platforms.PLATFORM_NAME)) {
            putTextEmbeddingModel(inferenceEntityId, platformSpecificModelVariantJsonEntity());
            var models = getTrainedModel("_all");
            assertThat(models.toString(), containsString("deployment_id=" + inferenceEntityId));

            Map<String, Object> results = inferOnMockService(
                inferenceEntityId,
                TaskType.TEXT_EMBEDDING,
                List.of("hello world", "this is the second document")
            );
            assertTrue(((List) ((Map) ((List) results.get("text_embedding")).get(0)).get("embedding")).size() > 1);
            // there exists embeddings
            assertTrue(((List) results.get("text_embedding")).size() == 2);
            // there are two sets of embeddings
            deleteTextEmbeddingModel(inferenceEntityId);
        } else {
            expectThrows(
                org.elasticsearch.client.ResponseException.class,
                () -> putTextEmbeddingModel(inferenceEntityId, platformSpecificModelVariantJsonEntity())
            );
        }
    }

    public void testPutE5Small_withFakeModelVariant() {
        String inferenceEntityId = randomAlphaOfLength(10).toLowerCase();
        expectThrows(
            org.elasticsearch.client.ResponseException.class,
            () -> putTextEmbeddingModel(inferenceEntityId, fakeModelVariantJsonEntity())
        );
    }

    public void testPutE5WithTrainedModelAndInference() throws IOException {
        putE5TrainedModels();
        deployE5TrainedModels();
        putTextEmbeddingModel("an-e5-deployment", platformAgnosticModelVariantJsonEntity());
        getTrainedModel("an-e5-deployment");
    }

    private Map<String, Object> deleteTextEmbeddingModel(String inferenceEntityId) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", "text_embedding", inferenceEntityId);
        var request = new Request("DELETE", endpoint);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    private Map<String, Object> putTextEmbeddingModel(String inferenceEntityId, String jsonEntity) throws IOException {
        var endpoint = Strings.format("_inference/%s/%s", TaskType.TEXT_EMBEDDING, inferenceEntityId);
        var request = new Request("PUT", endpoint);

        request.setJsonEntity(jsonEntity);
        var response = client().performRequest(request);
        assertOkOrCreated(response);
        return entityAsMap(response);
    }

    private String noModelIdVariantJsonEntity() {
        return """
                {
                  "service": "text_embedding",
                  "service_settings": {
                    "num_allocations": 1,
                    "num_threads": 1
                  }
                }
            """;
    }

    private String platformAgnosticModelVariantJsonEntity() {
        return """
                {
                  "service": "elasticsearch",
                  "service_settings": {
                    "num_allocations": 1,
                    "num_threads": 1,
                    "model_id": ".multilingual-e5-small"
                    }
                }
            """;
    }

    private String platformSpecificModelVariantJsonEntity() {
        return """
                {
                  "service": "elasticsearch",
                  "service_settings": {
                    "num_allocations": 1,
                    "num_threads": 1,
                    "model_id": ".multilingual-e5-small_linux-x86_64"
                  }
                }
            """;
    }

    private String fakeModelVariantJsonEntity() {
        return """
                {
                  "service": "elasticsearch",
                  "service_settings": {
                    "num_allocations": 1,
                    "num_threads": 1,
                    "model_id": ".not-a-real-model-variant"
                  }
                }
            """;
    }
}
