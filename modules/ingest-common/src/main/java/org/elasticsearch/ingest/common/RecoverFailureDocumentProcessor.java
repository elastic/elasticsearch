/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.HashMap;
import java.util.Map;

/**
 * Processor that remediates a failure document by:
 * - Copying the original index (and routing if present) from ctx.document.* into the document metadata
 * - Extracting the original source from ctx.document.source
 * - Removing the "error" and "document" fields from the root
 * - Restoring the original source fields at the root of the document
 */
public final class RecoverFailureDocumentProcessor extends AbstractProcessor {

    public static final String PRE_RECOVERY_FIELD = "pre_recovery";
    public static final String DOCUMENT_FIELD = "document";
    public static final String SOURCE_FIELD = "source";
    public static final String SOURCE_FIELD_PATH = DOCUMENT_FIELD + "." + SOURCE_FIELD;
    public static final String ID_FIELD = "id";
    public static final String INDEX_FIELD = "index";
    public static final String ROUTING_FIELD = "routing";
    public static final String ERROR_FIELD = "error";

    public static final String MISSING_DOCUMENT_ERROR_MSG =
        "failure store document has unexpected structure, missing required [document] field";
    public static final String MISSING_SOURCE_ERROR_MSG =
        "failure store document has unexpected structure, missing required [document.source] field";
    public static final String MISSING_ERROR_ERROR_MSG = "failure store document has unexpected structure, missing required [error] field";

    public static final String TYPE = "recover_failure_document";

    RecoverFailureDocumentProcessor(String tag, String description) {
        super(tag, description);
    }

    @Override
    @SuppressWarnings("unchecked")
    public IngestDocument execute(IngestDocument document) throws Exception {
        if (document.hasField(DOCUMENT_FIELD) == false) {
            throw new IllegalArgumentException(MISSING_DOCUMENT_ERROR_MSG);
        }

        if (document.hasField(SOURCE_FIELD_PATH) == false) {
            throw new IllegalArgumentException(MISSING_SOURCE_ERROR_MSG);
        }

        if (document.hasField(ERROR_FIELD) == false) {
            throw new IllegalArgumentException(MISSING_ERROR_ERROR_MSG);
        }

        // store pre-recovery data in ingest metadata
        storePreRecoveryData(document);

        // Get the nested 'document' field, which holds the original document and metadata.
        Map<String, Object> failedDocument = (Map<String, Object>) document.getFieldValue(DOCUMENT_FIELD, Map.class);

        // Copy the original index, routing, and id back to the document's metadata.
        String originalIndex = (String) failedDocument.get(INDEX_FIELD);
        if (originalIndex != null) {
            document.setFieldValue(IngestDocument.Metadata.INDEX.getFieldName(), originalIndex);
        }

        String originalRouting = (String) failedDocument.get(ROUTING_FIELD);
        if (originalRouting != null) {
            document.setFieldValue(IngestDocument.Metadata.ROUTING.getFieldName(), originalRouting);
        }

        String originalId = (String) failedDocument.get(ID_FIELD);
        if (originalId != null) {
            document.setFieldValue(IngestDocument.Metadata.ID.getFieldName(), originalId);
        }

        // Get the original document's source.
        Map<String, Object> originalSource = (Map<String, Object>) failedDocument.get(SOURCE_FIELD);

        // Overwrite the _source with original source contents.
        Map<String, Object> source = document.getSource();
        source.clear();
        source.putAll(originalSource);

        // Return the modified document.
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public RecoverFailureDocumentProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            return new RecoverFailureDocumentProcessor(processorTag, description);
        }
    }

    private static void storePreRecoveryData(IngestDocument document) {
        Map<String, Object> sourceAndMetadataMap = document.getSourceAndMetadata();

        // Create the pre_recovery data structure
        Map<String, Object> preRecoveryData = new HashMap<>();

        // Copy everything from the current document
        sourceAndMetadataMap.forEach((key, value) -> {
            if (DOCUMENT_FIELD.equals(key) && value instanceof Map) {
                // For the document field, copy everything except source
                @SuppressWarnings("unchecked")
                Map<String, Object> docMap = (Map<String, Object>) value;
                Map<String, Object> docCopy = new HashMap<>(docMap);
                docCopy.remove(SOURCE_FIELD);
                preRecoveryData.put(key, docCopy);
            } else {
                // Copy all other fields as-is
                preRecoveryData.put(key, value);
            }
        });

        // Store directly in ingest metadata
        document.getIngestMetadata().put(PRE_RECOVERY_FIELD, preRecoveryData);
    }
}
