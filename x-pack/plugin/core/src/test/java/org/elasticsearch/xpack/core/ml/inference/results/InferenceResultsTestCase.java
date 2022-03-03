/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.Version;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

abstract class InferenceResultsTestCase<T extends InferenceResults> extends AbstractWireSerializingTestCase<T> {

    public void testWriteToIngestDoc() throws IOException {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; ++i) {
            T inferenceResult = createTestInstance();
            if (randomBoolean()) {
                inferenceResult = copyInstance(inferenceResult, Version.CURRENT);
            }
            IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
            String parentField = randomAlphaOfLength(10);
            String modelId = randomAlphaOfLength(10);
            boolean alreadyHasResult = randomBoolean();
            if (alreadyHasResult) {
                document.setFieldValue(parentField, Map.of());
            }
            InferenceResults.writeResult(inferenceResult, document, parentField, modelId);
            assertFieldValues(inferenceResult, document, alreadyHasResult ? parentField + ".1" : parentField);
        }
    }

    abstract void assertFieldValues(T createdInstance, IngestDocument document, String resultsField);

    public void testWriteToDocAndSerialize() throws IOException {
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; ++i) {
            T inferenceResult = createTestInstance();
            if (randomBoolean()) {
                inferenceResult = copyInstance(inferenceResult, Version.CURRENT);
            }
            IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
            String parentField = randomAlphaOfLength(10);
            String modelId = randomAlphaOfLength(10);
            boolean alreadyHasResult = randomBoolean();
            if (alreadyHasResult) {
                document.setFieldValue(parentField, Map.of());
            }
            InferenceResults.writeResult(inferenceResult, document, parentField, modelId);
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject();
                Map<IngestDocument.Metadata, Object> metadataMap = document.getMetadata();
                for (Map.Entry<IngestDocument.Metadata, Object> metadata : metadataMap.entrySet()) {
                    if (metadata.getValue() != null) {
                        builder.field(metadata.getKey().getFieldName(), metadata.getValue().toString());
                    }
                }
                Map<String, Object> source = IngestDocument.deepCopyMap(document.getSourceAndMetadata());
                metadataMap.keySet().forEach(mD -> source.remove(mD.getFieldName()));
                builder.field("_source", source);
                builder.field("_ingest", document.getIngestMetadata());
                builder.endObject();
            }
        }
    }
}
