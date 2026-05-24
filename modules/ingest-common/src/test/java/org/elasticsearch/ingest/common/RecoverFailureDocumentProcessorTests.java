/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.action.bulk.FailureStoreDocumentConverter;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class RecoverFailureDocumentProcessorTests extends ESTestCase {

    public void testExecute() throws Exception {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("field1", "value1");
        originalSource.put("field2", "value2");

        IngestDocument failureDoc = createFailureDoc("routing-value", null, originalSource, true);
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IngestDocument result = processor.execute(failureDoc);

        // Verify the original source is completely copied over
        assertThat(result.getSource().size(), equalTo(2));
        assertThat(result.getFieldValue("field1", String.class), equalTo("value1"));
        assertThat(result.getFieldValue("field2", String.class), equalTo("value2"));

        // Verify error and document fields are removed
        assertThat(result.getSource(), not(hasKey(RecoverFailureDocumentProcessor.ERROR_FIELD)));
        assertThat(result.getSource(), not(hasKey(RecoverFailureDocumentProcessor.DOCUMENT_FIELD)));

        // Verify metadata is restored
        assertThat(result.getFieldValue(IngestDocument.Metadata.INDEX.getFieldName(), String.class), equalTo("original-index"));
        assertThat(result.getFieldValue(IngestDocument.Metadata.ROUTING.getFieldName(), String.class), equalTo("routing-value"));

        // Verify pre-recovery data is stored in the ingest-metadata
        assertThat(result.getIngestMetadata(), hasKey(RecoverFailureDocumentProcessor.PRE_RECOVERY_FIELD));
        @SuppressWarnings("unchecked")
        Map<String, Object> preRecoveryData = (Map<String, Object>) result.getIngestMetadata()
            .get(RecoverFailureDocumentProcessor.PRE_RECOVERY_FIELD);
        assertThat(preRecoveryData, notNullValue());

        // Verify pre-recovery contains the original failure state
        assertThat(preRecoveryData, hasKey(IngestDocument.Metadata.INDEX.getFieldName()));
        assertThat(preRecoveryData.get(IngestDocument.Metadata.INDEX.getFieldName()), equalTo("current-index"));
        assertThat(preRecoveryData, hasKey(RecoverFailureDocumentProcessor.ERROR_FIELD));
        @SuppressWarnings("unchecked")
        Map<String, Object> errorMap = (Map<String, Object>) preRecoveryData.get(RecoverFailureDocumentProcessor.ERROR_FIELD);
        assertThat(errorMap, notNullValue());
        assertThat(errorMap.get("type"), equalTo("exception"));
        assertThat(errorMap.get("message"), equalTo("simulated failure"));
        assertThat(preRecoveryData, hasKey(RecoverFailureDocumentProcessor.DOCUMENT_FIELD));

        // Verify document field in pre-recovery has source removed
        @SuppressWarnings("unchecked")
        Map<String, Object> preRecoveryDocument = (Map<String, Object>) preRecoveryData.get(RecoverFailureDocumentProcessor.DOCUMENT_FIELD);
        assertThat(preRecoveryDocument, not(hasKey(RecoverFailureDocumentProcessor.SOURCE_FIELD)));
        assertThat(preRecoveryDocument, hasKey(RecoverFailureDocumentProcessor.INDEX_FIELD));
        assertThat(preRecoveryDocument, hasKey(RecoverFailureDocumentProcessor.ROUTING_FIELD));
    }

    public void testExecuteWithOriginalId() throws Exception {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("data", "test");

        IngestDocument failureDoc = createFailureDoc("routing-value", "original-doc-id", originalSource, true);
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IngestDocument result = processor.execute(failureDoc);

        // Verify the original ID is copied over
        assertThat(result.getFieldValue(IngestDocument.Metadata.ID.getFieldName(), String.class), equalTo("original-doc-id"));

        // Verify pre-recovery data contains the original current ID
        @SuppressWarnings("unchecked")
        Map<String, Object> preRecoveryData = (Map<String, Object>) result.getIngestMetadata()
            .get(RecoverFailureDocumentProcessor.PRE_RECOVERY_FIELD);
        assertThat(preRecoveryData.get(IngestDocument.Metadata.ID.getFieldName()), equalTo("test-id"));
    }

    public void testExecuteWithMissingErrorField() throws Exception {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("field1", "value1");

        IngestDocument docWithoutError = createFailureDoc(null, null, originalSource, false);
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(docWithoutError));
        assertThat(exception.getMessage(), equalTo(RecoverFailureDocumentProcessor.MISSING_ERROR_ERROR_MSG));
    }

    public void testExecuteWithMissingDocumentField() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IngestDocument.Metadata.INDEX.getFieldName(), "current-index");
        sourceAndMetadata.put(RecoverFailureDocumentProcessor.ERROR_FIELD, "simulated failure");
        sourceAndMetadata.put(IngestDocument.Metadata.VERSION.getFieldName(), 1L);
        // Missing document field

        IngestDocument doc = new IngestDocument(sourceAndMetadata, new HashMap<>());
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(exception.getMessage(), equalTo(RecoverFailureDocumentProcessor.MISSING_DOCUMENT_ERROR_MSG));
    }

    public void testExecuteWithMissingSourceField() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IngestDocument.Metadata.INDEX.getFieldName(), "current-index");
        sourceAndMetadata.put(RecoverFailureDocumentProcessor.ERROR_FIELD, "simulated failure");

        // Document without a source field
        Map<String, Object> documentWrapper = new HashMap<>();
        documentWrapper.put(RecoverFailureDocumentProcessor.INDEX_FIELD, "original-index");
        sourceAndMetadata.put(RecoverFailureDocumentProcessor.DOCUMENT_FIELD, documentWrapper);
        sourceAndMetadata.put(IngestDocument.Metadata.VERSION.getFieldName(), 1L);

        IngestDocument doc = new IngestDocument(sourceAndMetadata, new HashMap<>());
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(exception.getMessage(), equalTo(RecoverFailureDocumentProcessor.MISSING_SOURCE_ERROR_MSG));
    }

    public void testExecuteWithNullMetadataFields() throws Exception {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("field1", "value1");

        IngestDocument failureDoc = createFailureDocWithNullMetadata(originalSource);
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IngestDocument result = processor.execute(failureDoc);

        // Verify source is recovered
        assertThat(result.getFieldValue("field1", String.class), equalTo("value1"));

        // Verify metadata fields are not overwritten by null values (existing behavior should be preserved)
        // The processor only updates fields when the failure source fields are not null
        assertThat(result.getFieldValue(IngestDocument.Metadata.INDEX.getFieldName(), String.class), equalTo("current-index"));
        assertThat(result.getFieldValue(IngestDocument.Metadata.ID.getFieldName(), String.class), equalTo("test-id"));
    }

    public void testExecuteCompletelyReplacesSource() throws Exception {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("original_field", "original_value");

        // Create a failure doc with extra fields in the current source
        IngestDocument failureDoc = createFailureDoc(null, null, originalSource, true);
        failureDoc.setFieldValue("extra_field", "should_be_removed");
        failureDoc.setFieldValue("another_field", "also_removed");

        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IngestDocument result = processor.execute(failureDoc);

        // Verify only original source fields remain
        assertThat(result.getSource().size(), equalTo(1));
        assertThat(result.getFieldValue("original_field", String.class), equalTo("original_value"));

        // Verify extra fields are completely removed
        assertThat(result.getSource(), not(hasKey("extra_field")));
        assertThat(result.getSource(), not(hasKey("another_field")));
        assertThat(result.getSource(), not(hasKey(RecoverFailureDocumentProcessor.ERROR_FIELD)));
        assertThat(result.getSource(), not(hasKey(RecoverFailureDocumentProcessor.DOCUMENT_FIELD)));

        // Verify pre-recovery data contains the extra fields
        @SuppressWarnings("unchecked")
        Map<String, Object> preRecoveryData = (Map<String, Object>) result.getIngestMetadata()
            .get(RecoverFailureDocumentProcessor.PRE_RECOVERY_FIELD);
        assertThat(preRecoveryData, hasKey("extra_field"));
        assertThat(preRecoveryData, hasKey("another_field"));
    }

    public void testGetType() {
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");
        assertThat(processor.getType(), equalTo(RecoverFailureDocumentProcessor.TYPE));
    }

    private static IngestDocument createFailureDoc(
        String routing,
        String originalId,
        Map<String, Object> originalSource,
        boolean containsError
    ) throws IOException {
        String currentIndex = "current-index";
        String originalIndex = "original-index";
        // Build the original IndexRequest
        IndexRequest originalRequest = new IndexRequest(originalIndex).source(originalSource);
        if (routing != null) {
            originalRequest.routing(routing);
        }
        if (originalId != null) {
            originalRequest.id(originalId);
        }
        // Simulate a failure if requested
        Exception failure = containsError ? new Exception("simulated failure") : null;
        FailureStoreDocumentConverter converter = new FailureStoreDocumentConverter();
        IndexRequest failureRequest;
        if (failure == null) {
            // If no error, manually create a doc with missing error field for negative test
            Map<String, Object> sourceAndMetadata = new HashMap<>();
            sourceAndMetadata.put(IngestDocument.Metadata.INDEX.getFieldName(), currentIndex);
            Map<String, Object> docMap = new HashMap<>();
            docMap.put(RecoverFailureDocumentProcessor.INDEX_FIELD, originalIndex);
            if (routing != null) docMap.put(RecoverFailureDocumentProcessor.ROUTING_FIELD, routing);
            if (originalId != null) docMap.put(RecoverFailureDocumentProcessor.ID_FIELD, originalId);
            docMap.put(RecoverFailureDocumentProcessor.SOURCE_FIELD, originalSource);
            sourceAndMetadata.put(RecoverFailureDocumentProcessor.DOCUMENT_FIELD, docMap);
            sourceAndMetadata.put(IngestDocument.Metadata.VERSION.getFieldName(), 1L);
            return new IngestDocument(sourceAndMetadata, new HashMap<>());
        }
        failureRequest = converter.transformFailedRequest(originalRequest, failure, currentIndex);
        // Extract the source map from the failureRequest
        Map<String, Object> failureSource = XContentHelper.convertToMap(failureRequest.source(), false, failureRequest.getContentType())
            .v2();
        // Add metadata fields
        failureSource.put(IngestDocument.Metadata.INDEX.getFieldName(), currentIndex);
        failureSource.put(IngestDocument.Metadata.ID.getFieldName(), "test-id");
        failureSource.put(IngestDocument.Metadata.VERSION.getFieldName(), 1L);
        return new IngestDocument(failureSource, new HashMap<>());
    }

    private static IngestDocument createFailureDocWithNullMetadata(Map<String, Object> originalSource) throws IOException {
        String currentIndex = "current-index";

        // Build the original IndexRequest with null metadata fields
        IndexRequest originalRequest = new IndexRequest((String) null).source(originalSource);
        // Simulate a failure
        Exception failure = new Exception("simulated failure");
        FailureStoreDocumentConverter converter = new FailureStoreDocumentConverter();
        IndexRequest failureRequest = converter.transformFailedRequest(originalRequest, failure, currentIndex);
        // Extract the source map from the failureRequest
        Map<String, Object> failureSource = XContentHelper.convertToMap(failureRequest.source(), false, failureRequest.getContentType())
            .v2();
        // Add metadata fields
        failureSource.put(IngestDocument.Metadata.INDEX.getFieldName(), currentIndex);
        failureSource.put(IngestDocument.Metadata.ID.getFieldName(), "test-id");
        failureSource.put(IngestDocument.Metadata.VERSION.getFieldName(), 1L);
        return new IngestDocument(failureSource, new HashMap<>());
    }
}
