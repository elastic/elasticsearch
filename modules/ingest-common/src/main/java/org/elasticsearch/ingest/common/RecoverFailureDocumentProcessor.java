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
        // Get the nested 'document' field, which holds the original document and metadata.
        if (document.hasField("document") == false) {
            throw new IllegalArgumentException("field [document] not present as part of path [document]");
        }
        Map<String, Object> failedDocument = (Map<String, Object>) document.getFieldValue("document", Map.class);

        // Copy the original index and routing back to the document's metadata.
        String originalIndex = (String) failedDocument.get("index");
        if (originalIndex != null) {
            document.setFieldValue("_index", originalIndex);
        }

        String originalRouting = (String) failedDocument.get("routing");
        if (originalRouting != null) {
            document.setFieldValue("_routing", originalRouting);
        }

        // Get the original document's source.
        Map<String, Object> originalSource = (Map<String, Object>) failedDocument.get("source");
        if (originalSource == null) {
            throw new IllegalArgumentException("field [source] not present as part of path [document.source]");
        }

        // Remove the 'error' and 'document' fields from the top-level document.
        document.removeField("error");
        document.removeField("document");

        // Extract all fields from the original source back to the root of the document.
        for (Map.Entry<String, Object> entry : originalSource.entrySet()) {
            document.setFieldValue(entry.getKey(), entry.getValue());
        }

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
}
