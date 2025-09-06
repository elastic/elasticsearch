/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

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

        IngestDocument failureDoc = createFailureDoc("current-index", "original-index", "routing-value", null, originalSource, true);
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IngestDocument result = processor.execute(failureDoc);

        // Verify the original source is completely copied over
        assertThat(result.getSource().size(), equalTo(2));
        assertThat(result.getFieldValue("field1", String.class), equalTo("value1"));
        assertThat(result.getFieldValue("field2", String.class), equalTo("value2"));

        // Verify error and document fields are removed
        assertThat(result.getSource(), not(hasKey("error")));
        assertThat(result.getSource(), not(hasKey("document")));

        // Verify metadata is restored
        assertThat(result.getFieldValue("_index", String.class), equalTo("original-index"));
        assertThat(result.getFieldValue("_routing", String.class), equalTo("routing-value"));

        // Verify pre-recovery data is stored in the ingest-metadata
        assertThat(result.getIngestMetadata(), hasKey("pre_recovery"));
        @SuppressWarnings("unchecked")
        Map<String, Object> preRecoveryData = (Map<String, Object>) result.getIngestMetadata().get("pre_recovery");
        assertThat(preRecoveryData, notNullValue());

        // Verify pre-recovery contains the original failure state
        assertThat(preRecoveryData, hasKey("_index"));
        assertThat(preRecoveryData.get("_index"), equalTo("current-index"));
        assertThat(preRecoveryData, hasKey("error"));
        assertThat(preRecoveryData.get("error"), equalTo("simulated failure"));
        assertThat(preRecoveryData, hasKey("document"));

        // Verify document field in pre-recovery has source removed
        @SuppressWarnings("unchecked")
        Map<String, Object> preRecoveryDocument = (Map<String, Object>) preRecoveryData.get("document");
        assertThat(preRecoveryDocument, not(hasKey("source")));
        assertThat(preRecoveryDocument, hasKey("index"));
        assertThat(preRecoveryDocument, hasKey("routing"));
    }

    public void testExecuteWithOriginalId() throws Exception {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("data", "test");

        IngestDocument failureDoc = createFailureDoc(
            "current-index",
            "original-index",
            "routing-value",
            "original-doc-id",
            originalSource,
            true
        );
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IngestDocument result = processor.execute(failureDoc);

        // Verify the original ID is copied over
        assertThat(result.getFieldValue("_id", String.class), equalTo("original-doc-id"));

        // Verify pre-recovery data contains the original current ID
        @SuppressWarnings("unchecked")
        Map<String, Object> preRecoveryData = (Map<String, Object>) result.getIngestMetadata().get("pre_recovery");
        assertThat(preRecoveryData.get("_id"), equalTo("test-id"));
    }

    public void testExecuteWithMissingErrorField() {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("field1", "value1");

        IngestDocument docWithoutError = createFailureDoc("current-index", "original-index", null, null, originalSource, false);
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(docWithoutError));
        assertThat(exception.getMessage(), equalTo("field [error] not present as part of path [error]"));
    }

    public void testExecuteWithMissingDocumentField() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("_index", "current-index");
        sourceAndMetadata.put("error", "simulated failure");
        sourceAndMetadata.put("_version", 1L);
        // Missing document field

        IngestDocument doc = new IngestDocument(sourceAndMetadata, new HashMap<>());
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(exception.getMessage(), equalTo("field [document] not present as part of path [document]"));
    }

    public void testExecuteWithMissingSourceField() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("_index", "current-index");
        sourceAndMetadata.put("error", "simulated failure");

        // Document without a source field
        Map<String, Object> documentWrapper = new HashMap<>();
        documentWrapper.put("index", "original-index");
        sourceAndMetadata.put("document", documentWrapper);
        sourceAndMetadata.put("_version", 1L);

        IngestDocument doc = new IngestDocument(sourceAndMetadata, new HashMap<>());
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(doc));
        assertThat(exception.getMessage(), equalTo("field [source] not present as part of path [document.source]"));
    }

    public void testExecuteWithNullMetadataFields() throws Exception {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("field1", "value1");

        IngestDocument failureDoc = createFailureDocWithNullMetadata("current-index", originalSource);
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");

        IngestDocument result = processor.execute(failureDoc);

        // Verify source is recovered
        assertThat(result.getFieldValue("field1", String.class), equalTo("value1"));

        // Verify null metadata fields are not set (existing behavior should be preserved)
        // The processor only sets fields when they are not null
    }

    public void testExecuteCompletelyReplacesSource() throws Exception {
        Map<String, Object> originalSource = new HashMap<>();
        originalSource.put("original_field", "original_value");

        // Create a failure doc with extra fields in the current source
        IngestDocument failureDoc = createFailureDoc("current-index", "original-index", null, null, originalSource, true);
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
        assertThat(result.getSource(), not(hasKey("error")));
        assertThat(result.getSource(), not(hasKey("document")));

        // Verify pre-recovery data contains the extra fields
        @SuppressWarnings("unchecked")
        Map<String, Object> preRecoveryData = (Map<String, Object>) result.getIngestMetadata().get("pre_recovery");
        assertThat(preRecoveryData, hasKey("extra_field"));
        assertThat(preRecoveryData, hasKey("another_field"));
    }

    public void testGetType() {
        RecoverFailureDocumentProcessor processor = new RecoverFailureDocumentProcessor("test", "test");
        assertThat(processor.getType(), equalTo(RecoverFailureDocumentProcessor.TYPE));
    }

    private static IngestDocument createFailureDoc(
        String currentIndex,
        String originalIndex,
        String routing,
        String originalId,
        Map<String, Object> originalSource,
        boolean containsError
    ) {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        // current metadata
        sourceAndMetadata.put("_index", currentIndex);
        sourceAndMetadata.put("_id", "test-id");
        sourceAndMetadata.put("_version", 1L);

        // failure wrapper
        Map<String, Object> documentWrapper = new HashMap<>();
        documentWrapper.put("index", originalIndex);
        if (routing != null) {
            documentWrapper.put("routing", routing);
        }
        if (originalId != null) {
            documentWrapper.put("id", originalId);
        }
        documentWrapper.put("source", originalSource);

        if (containsError) {
            sourceAndMetadata.put("error", "simulated failure");
        }

        sourceAndMetadata.put("document", documentWrapper);

        // no special ingest-metadata
        Map<String, Object> ingestMetadata = new HashMap<>();

        return new IngestDocument(sourceAndMetadata, ingestMetadata);
    }

    private static IngestDocument createFailureDocWithNullMetadata(String currentIndex, Map<String, Object> originalSource) {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        // current metadata
        sourceAndMetadata.put("_index", currentIndex);
        sourceAndMetadata.put("_id", "test-id");
        sourceAndMetadata.put("_version", 1L);

        // failure wrapper with null metadata
        Map<String, Object> documentWrapper = new HashMap<>();
        documentWrapper.put("index", null);
        documentWrapper.put("routing", null);
        documentWrapper.put("id", null);
        documentWrapper.put("source", originalSource);

        sourceAndMetadata.put("error", "simulated failure");
        sourceAndMetadata.put("document", documentWrapper);

        // no special ingest-metadata
        Map<String, Object> ingestMetadata = new HashMap<>();

        return new IngestDocument(sourceAndMetadata, ingestMetadata);
    }
}
