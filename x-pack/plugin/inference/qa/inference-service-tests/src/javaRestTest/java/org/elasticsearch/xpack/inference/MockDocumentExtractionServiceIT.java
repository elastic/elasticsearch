/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.inference.results.DocumentExtractionResults;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class MockDocumentExtractionServiceIT extends InferenceBaseRestTest {

    public void testMockService() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockDocumentExtractionServiceModelConfig(), TaskType.DOCUMENT_EXTRACTION);
        var model = getModels(inferenceEntityId, TaskType.DOCUMENT_EXTRACTION).get(0);

        for (var modelMap : List.of(putModel, model)) {
            assertEquals(inferenceEntityId, modelMap.get("inference_id"));
            assertEquals(TaskType.DOCUMENT_EXTRACTION, TaskType.fromString((String) modelMap.get("task_type")));
            assertEquals("test_document_extraction_service", modelMap.get("service"));
        }

        var input = List.of(randomPdfInput());
        var inference = documentExtractionInfer(inferenceEntityId, input);
        assertNonEmptyInferenceResults(inference, 1, TaskType.DOCUMENT_EXTRACTION);
        assertEquals(inference, documentExtractionInfer(inferenceEntityId, input));
        assertNotEquals(
            inference,
            documentExtractionInfer(
                inferenceEntityId,
                List.of(randomValueOtherThan(input.getFirst(), MockDocumentExtractionServiceIT::randomPdfInput))
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testMockServiceReturnsExpectedResultFields() throws IOException {
        String inferenceEntityId = "test-mock-result-fields";
        putModel(inferenceEntityId, mockDocumentExtractionServiceModelConfig(), TaskType.DOCUMENT_EXTRACTION);

        var inference = documentExtractionInfer(inferenceEntityId, List.of(randomPdfInput(), randomImageInput()));

        var results = (List<Map<String, Object>>) inference.get(DocumentExtractionResults.DOCUMENT_EXTRACTION);
        assertThat(results, hasSize(2));

        var pdfResult = results.get(0);
        assertThat((String) pdfResult.get(DocumentExtractionResults.Result.CONTENT), startsWith("extracted content"));
        assertThat(pdfResult.get(DocumentExtractionResults.Result.FORMAT), is("markdown"));
        assertThat(pdfResult.get(DocumentExtractionResults.Result.METADATA), is(Map.of("media_type", "application/pdf")));

        var imageResult = results.get(1);
        assertThat((String) imageResult.get(DocumentExtractionResults.Result.CONTENT), startsWith("extracted content"));
        assertThat(imageResult.get(DocumentExtractionResults.Result.FORMAT), is("markdown"));
        assertThat(imageResult.get(DocumentExtractionResults.Result.METADATA), is(Map.of("media_type", "image/png")));

        assertThat(
            pdfResult.get(DocumentExtractionResults.Result.CONTENT),
            is(not(imageResult.get(DocumentExtractionResults.Result.CONTENT)))
        );
    }

    @SuppressWarnings("unchecked")
    public void testMockServiceAppliesRequestTaskSettings() throws IOException {
        String inferenceEntityId = "test-mock-task-settings";
        putModel(inferenceEntityId, mockDocumentExtractionServiceModelConfig(), TaskType.DOCUMENT_EXTRACTION);

        var inference = documentExtractionInfer(inferenceEntityId, List.of(randomPdfInput()), "text");

        var results = (List<Map<String, Object>>) inference.get(DocumentExtractionResults.DOCUMENT_EXTRACTION);
        assertThat(results, hasSize(1));
        assertThat(results.getFirst().get(DocumentExtractionResults.Result.FORMAT), is("text"));
    }

    @SuppressWarnings("unchecked")
    public void testMockService_DoesNotReturnSecretsInGetResponse() throws IOException {
        String inferenceEntityId = "test-mock";
        var putModel = putModel(inferenceEntityId, mockDocumentExtractionServiceModelConfig(), TaskType.DOCUMENT_EXTRACTION);
        var model = getModels(inferenceEntityId, TaskType.DOCUMENT_EXTRACTION).get(0);

        var serviceSettings = (Map<String, Object>) model.get("service_settings");
        assertNull(serviceSettings.get("api_key"));
        assertNotNull(serviceSettings.get("model_id"));

        var putServiceSettings = (Map<String, Object>) putModel.get("service_settings");
        assertNull(putServiceSettings.get("api_key"));
        assertNotNull(putServiceSettings.get("model_id"));
    }

    private static InferenceString randomPdfInput() {
        return new InferenceString(DataType.PDF, DataFormat.BASE64, "data:application/pdf;base64," + randomAlphanumericOfLength(16));
    }

    private static InferenceString randomImageInput() {
        return new InferenceString(DataType.IMAGE, DataFormat.BASE64, "data:image/png;base64," + randomAlphanumericOfLength(16));
    }
}
