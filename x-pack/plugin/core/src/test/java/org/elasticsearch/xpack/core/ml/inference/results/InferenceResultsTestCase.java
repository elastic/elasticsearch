/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

abstract class InferenceResultsTestCase<T extends InferenceResults> extends AbstractWireSerializingTestCase<T> {

    public void testWriteToIngestDoc() throws IOException {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; ++i) {
            T inferenceResult = createTestInstance();
            if (randomBoolean()) {
                inferenceResult = copyInstance(inferenceResult, TransportVersion.current());
            }
            IngestDocument document = TestIngestDocument.emptyIngestDocument();
            String parentField = randomAlphaOfLength(10);
            String modelId = randomAlphaOfLength(10);
            boolean alreadyHasResult = randomBoolean();
            if (alreadyHasResult) {
                document.setFieldValue(parentField, Map.of());
            }
            InferenceResults.writeResult(inferenceResult, document, parentField, modelId);

            String expectedOutputPath = alreadyHasResult ? parentField + ".1." : parentField + ".";

            assertThat(
                document.getFieldValue(expectedOutputPath + InferenceResults.MODEL_ID_RESULTS_FIELD, String.class),
                equalTo(modelId)
            );
            if (inferenceResult instanceof NlpInferenceResults nlpInferenceResults && nlpInferenceResults.isTruncated()) {
                assertTrue(document.getFieldValue(expectedOutputPath + "is_truncated", Boolean.class));
            }

            assertFieldValues(inferenceResult, document, expectedOutputPath, inferenceResult.getResultsField());
        }
    }

    private void testWriteToIngestDocField() throws IOException {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; ++i) {
            T inferenceResult = createTestInstance();
            if (randomBoolean()) {
                inferenceResult = copyInstance(inferenceResult, TransportVersion.current());
            }
            IngestDocument document = TestIngestDocument.emptyIngestDocument();
            String outputField = randomAlphaOfLength(10);
            String modelId = randomAlphaOfLength(10);
            String parentField = randomBoolean() ? null : randomAlphaOfLength(10);
            boolean writeModelId = randomBoolean();

            boolean alreadyHasResult = randomBoolean();
            if (alreadyHasResult && parentField != null) {
                document.setFieldValue(parentField, Map.of());
            }
            InferenceResults.writeResultToField(inferenceResult, document, parentField, outputField, modelId, writeModelId);

            String expectedOutputPath = parentField == null ? "" : parentField + ".";
            if (alreadyHasResult && parentField != null) {
                expectedOutputPath = expectedOutputPath + "1.";
            }

            if (writeModelId) {
                String modelIdPath = expectedOutputPath + InferenceResults.MODEL_ID_RESULTS_FIELD;
                assertThat(document.getFieldValue(modelIdPath, String.class), equalTo(modelId));
            }
            if (inferenceResult instanceof NlpInferenceResults nlpInferenceResults && nlpInferenceResults.isTruncated()) {
                assertTrue(document.getFieldValue(expectedOutputPath + "is_truncated", Boolean.class));
            }

            assertFieldValues(inferenceResult, document, expectedOutputPath, outputField);
        }
    }

    abstract void assertFieldValues(T createdInstance, IngestDocument document, String parentField, String resultsField);

    public void testWriteToDocAndSerialize() throws IOException {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; ++i) {
            T inferenceResult = createTestInstance();
            if (randomBoolean()) {
                inferenceResult = copyInstance(inferenceResult, TransportVersion.current());
            }
            IngestDocument document = TestIngestDocument.emptyIngestDocument();
            String parentField = randomAlphaOfLength(10);
            String modelId = randomAlphaOfLength(10);
            boolean alreadyHasResult = randomBoolean();
            if (alreadyHasResult) {
                document.setFieldValue(parentField, Map.of());
            }
            InferenceResults.writeResult(inferenceResult, document, parentField, modelId);
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                org.elasticsearch.script.Metadata metadata = document.getMetadata();
                for (String key : metadata.keySet()) {
                    Object value = metadata.get(key);
                    if (value != null) {
                        builder.field(key, value.toString());
                    }
                }
                Map<String, Object> source = IngestDocument.deepCopyMap(document.getSourceAndMetadata());
                metadata.keySet().forEach(source::remove);
                builder.field("_source", source);
                builder.field("_ingest", document.getIngestMetadata());
                builder.endObject();
            }
        }
    }
}
