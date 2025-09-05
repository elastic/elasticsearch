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

    public static final String TYPE = "recover_failure_document";

    RecoverFailureDocumentProcessor(String tag, String description) {
        super(tag, description);
    }

    @Override
    @SuppressWarnings("unchecked")
    public IngestDocument execute(IngestDocument document) throws Exception {
        if (document.hasField("document") == false) {
            throw new IllegalArgumentException("field [document] not present as part of path [document]");
        }

        if (document.hasField("document.source") == false) {
            throw new IllegalArgumentException("field [source] not present as part of path [document.source]");
        }

        if (document.hasField("error") == false) {
            throw new IllegalArgumentException("field [error] not present as part of path [error]");
        }

        // store pre-recovery data in ingest metadata
        storePreRecoveryData(document);

        // Get the nested 'document' field, which holds the original document and metadata.
        Map<String, Object> failedDocument = (Map<String, Object>) document.getFieldValue("document", Map.class);

        // Copy the original index, routing, and id back to the document's metadata.
        String originalIndex = (String) failedDocument.get("index");
        if (originalIndex != null) {
            document.setFieldValue("_index", originalIndex);
        }

        String originalRouting = (String) failedDocument.get("routing");
        if (originalRouting != null) {
            document.setFieldValue("_routing", originalRouting);
        }

        String originalId = (String) failedDocument.get("id");
        if (originalId != null) {
            document.setFieldValue("_id", originalId);
        }

        // Get the original document's source.
        Map<String, Object> originalSource = (Map<String, Object>) failedDocument.get("source");

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
            if ("document".equals(key) && value instanceof Map) {
                // For the document field, copy everything except source
                @SuppressWarnings("unchecked")
                Map<String, Object> docMap = (Map<String, Object>) value;
                Map<String, Object> docCopy = new HashMap<>(docMap);
                docCopy.remove("source");
                preRecoveryData.put(key, docCopy);
            } else {
                // Copy all other fields as-is
                preRecoveryData.put(key, value);
            }
        });

        // Store directly in ingest metadata
        document.getIngestMetadata().put("pre_recovery", preRecoveryData);
    }
}
