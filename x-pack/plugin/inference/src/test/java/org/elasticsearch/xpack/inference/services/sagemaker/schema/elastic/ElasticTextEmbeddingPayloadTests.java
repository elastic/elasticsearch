/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.services.sagemaker.model.SageMakerModel;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;

import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.Mockito.when;

public class ElasticTextEmbeddingPayloadTests extends ElasticPayloadTestCase<ElasticTextEmbeddingPayload> {
    @Override
    protected ElasticTextEmbeddingPayload payload() {
        return new ElasticTextEmbeddingPayload();
    }

    @Override
    protected Set<TaskType> expectedSupportedTaskTypes() {
        return Set.of(TaskType.TEXT_EMBEDDING);
    }

    @Override
    protected SageMakerStoredServiceSchema randomApiServiceSettings() {
        return SageMakerElasticTextEmbeddingServiceSettingsTests.randomInstance();
    }

    @Override
    protected SageMakerModel mockModel(SageMakerElasticTaskSettings taskSettings) {
        var model = super.mockModel(taskSettings);
        when(model.apiServiceSettings()).thenReturn(randomApiServiceSettings());
        return model;
    }

    protected SageMakerModel mockModel(DenseVectorFieldMapper.ElementType elementType) {
        var model = super.mockModel(SageMakerElasticTaskSettings.empty());
        when(model.apiServiceSettings()).thenReturn(SageMakerElasticTextEmbeddingServiceSettingsTests.randomInstance(elementType));
        return model;
    }

    public void testBitResponse() throws Exception {
        var responseJson = """
            {
                "text_embedding_bits": [
                    {
                        "embedding": [
                            23
                        ]
                    }
                ]
            }
            """;

        var bitResults = payload.responseBody(mockModel(DenseVectorFieldMapper.ElementType.BIT), invokeEndpointResponse(responseJson));

        assertThat(bitResults.embeddings().size(), is(1));
        var embedding = bitResults.embeddings().get(0);
        assertThat(embedding, isA(TextEmbeddingByteResults.Embedding.class));
        assertThat(((TextEmbeddingByteResults.Embedding) embedding).values(), is(new byte[] { 23 }));
    }

    public void testByteResponse() throws Exception {
        var responseJson = """
            {
                "text_embedding_bytes": [
                    {
                        "embedding": [
                            23
                        ]
                    }
                ]
            }
            """;

        var byteResults = payload.responseBody(mockModel(DenseVectorFieldMapper.ElementType.BYTE), invokeEndpointResponse(responseJson));

        assertThat(byteResults.embeddings().size(), is(1));
        var embedding = byteResults.embeddings().get(0);
        assertThat(embedding, isA(TextEmbeddingByteResults.Embedding.class));
        assertThat(((TextEmbeddingByteResults.Embedding) embedding).values(), is(new byte[] { 23 }));
    }

    public void testFloatResponse() throws Exception {
        var responseJson = """
            {
                "text_embedding": [
                    {
                        "embedding": [
                            0.1
                        ]
                    }
                ]
            }
            """;

        var byteResults = payload.responseBody(mockModel(DenseVectorFieldMapper.ElementType.FLOAT), invokeEndpointResponse(responseJson));

        assertThat(byteResults.embeddings().size(), is(1));
        var embedding = byteResults.embeddings().get(0);
        assertThat(embedding, isA(TextEmbeddingFloatResults.Embedding.class));
        assertThat(((TextEmbeddingFloatResults.Embedding) embedding).values(), is(new float[] { 0.1F }));
    }
}
