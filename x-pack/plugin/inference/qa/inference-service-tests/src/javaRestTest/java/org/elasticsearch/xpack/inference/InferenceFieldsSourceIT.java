/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.inference.TaskType;

import java.io.IOException;

public class InferenceFieldsSourceIT extends InferenceBaseRestTest {

    public void testGetSourceWithInferenceFields() throws IOException {
        // https://github.com/elastic/elasticsearch/issues/141075
        String inferenceEntityId = "test-get-source-inference-fields";
        String indexName = "test-index-inference-fields";

        putModel(inferenceEntityId, mockTextEmbeddingServiceModelConfig(), TaskType.TEXT_EMBEDDING);

        Request createIndex = new Request("PUT", indexName);
        createIndex.setJsonEntity("""
            {
              "mappings": {
                "properties": {
                  "content": {
                    "type": "semantic_text",
                    "inference_id": "%s"
                  }
                }
              }
            }""".formatted(inferenceEntityId));
        client().performRequest(createIndex);

        // Index a document
        Request indexDoc = new Request("PUT", indexName + "/_doc/1?refresh=true");
        indexDoc.setJsonEntity("""
            {"content": "test content"}""");

        client().performRequest(indexDoc);

        Request getSource = new Request("GET", indexName + "/_source/1");
        getSource.addParameter("_source_includes", "_inference_fields");
        Response response = client().performRequest(getSource);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
