/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.qa.mixed;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.qa.mixed.MixedClusterSpecIT.bwcVersion;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class AzureOpenAiServiceMixedIT extends BaseMixedIT {

    private static final String OPEN_AI_AZURE_EMBEDDINGS_ADDED = "8.14.0";

    private static MockWebServer openAiEmbeddingsServer;

    // @BeforeClass test mute
    public static void startWebServer() throws IOException {
        openAiEmbeddingsServer = new MockWebServer();
        openAiEmbeddingsServer.start();
    }

    // @AfterClass test mute
    public static void shutdown() {
        openAiEmbeddingsServer.close();
    }

    @SuppressWarnings("unchecked")
    @LuceneTestCase.AwaitsFix(bugUrl = "AzureOpenAI doesn't support webserver URL in tests")
    public void testOpenAiEmbeddings() throws IOException {
        var openAiEmbeddingsSupported = bwcVersion.onOrAfter(Version.fromString(OPEN_AI_AZURE_EMBEDDINGS_ADDED));
        assumeTrue("Azure OpenAI embedding service added in " + OPEN_AI_AZURE_EMBEDDINGS_ADDED, openAiEmbeddingsSupported);

        final String oldClusterId = "old-cluster-embeddings";

        openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
        put(oldClusterId, embeddingConfig(getUrl(openAiEmbeddingsServer)), TaskType.TEXT_EMBEDDING);

        var configs = (List<Map<String, Object>>) get(TaskType.TEXT_EMBEDDING, oldClusterId).get("endpoints");
        assertThat(configs, hasSize(1));
        assertEquals("azureopenai", configs.get(0).get("service"));
        assertEmbeddingInference(oldClusterId);
    }

    void assertEmbeddingInference(String inferenceId) throws IOException {
        openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
        var inferenceMap = inference(inferenceId, TaskType.TEXT_EMBEDDING, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    private String embeddingConfig(String url) {
        return Strings.format("""
            {
                "service": "azureopenai",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s",
                    "resource_name": "resource_name",
                    "deployment_id": "deployment_id",
                    "api_version": "2024-02-01"
                }
            }
            """, url);
    }

    static String embeddingResponse() {
        return """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.0123,
                          -0.0123
                      ]
                  }
              ],
              "model": "text-embedding-ada-002",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;
    }
}
