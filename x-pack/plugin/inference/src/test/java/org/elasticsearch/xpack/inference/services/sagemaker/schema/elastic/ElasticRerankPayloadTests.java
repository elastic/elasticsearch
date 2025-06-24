/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.sagemaker.SageMakerInferenceRequest;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;

public class ElasticRerankPayloadTests extends ElasticPayloadTestCase<ElasticRerankPayload> {
    @Override
    protected ElasticRerankPayload payload() {
        return new ElasticRerankPayload();
    }

    @Override
    protected Set<TaskType> expectedSupportedTaskTypes() {
        return Set.of(TaskType.RERANK);
    }

    public void testRequestWithRequiredFields() throws Exception {
        var request = new SageMakerInferenceRequest("is this a greeting?", null, null, List.of("hello"), false, InputType.UNSPECIFIED);
        var sdkByes = payload.requestBytes(mockModel(), request);
        assertJsonSdkBytes(sdkByes, """
            {
                "input": "hello",
                "query": "is this a greeting?"
            }""");
    }

    // input_type is ignored for rerank
    public void testRequestWithInternalFields() throws Exception {
        var request = new SageMakerInferenceRequest("is this a greeting?", null, null, List.of("hello"), false, InputType.INTERNAL_SEARCH);
        var sdkByes = payload.requestBytes(mockModel(), request);
        assertJsonSdkBytes(sdkByes, """
            {
                "input": "hello",
                "query": "is this a greeting?"
            }""");
    }

    public void testRequestWithMultipleInput() throws Exception {
        var request = new SageMakerInferenceRequest(
            "is this a greeting?",
            null,
            null,
            List.of("hello", "there"),
            false,
            InputType.UNSPECIFIED
        );
        var sdkByes = payload.requestBytes(mockModel(), request);
        assertJsonSdkBytes(sdkByes, """
            {
                "input": [
                    "hello",
                    "there"
                ],
                "query": "is this a greeting?"
            }""");
    }

    public void testRequestWithOptionalFields() throws Exception {
        var request = new SageMakerInferenceRequest("is this a greeting?", true, 5, List.of("hello"), false, InputType.INGEST);
        var sdkByes = payload.requestBytes(mockModel(new SageMakerElasticTaskSettings(Map.of("more", "args"))), request);
        assertJsonSdkBytes(sdkByes, """
            {
                "input": "hello",
                "query": "is this a greeting?",
                "return_documents": true,
                "top_n": 5,
                "task_settings": {
                    "more": "args"
                }
            }""");
    }

    public void testResponse() throws Exception {
        var responseJson = """
            {
                "rerank": [
                    {
                        "index": 0,
                        "relevance_score": 1.0,
                        "text": "hello, world"
                    }
                ]
            }
            """;

        var rankedDocsResults = payload.responseBody(mockModel(), invokeEndpointResponse(responseJson));
        assertThat(rankedDocsResults.getRankedDocs().size(), is(1));
        var rankedDoc = rankedDocsResults.getRankedDocs().get(0);
        assertThat(rankedDoc.index(), is(0));
        assertThat(rankedDoc.relevanceScore(), is(1.0F));
        assertThat(rankedDoc.text(), is("hello, world"));
    }

}
