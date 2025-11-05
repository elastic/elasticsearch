/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

/**
 * Plugin that provides the attachment ingest processor for parsing and extracting document content.
 * <p>
 * This plugin integrates Apache Tika to extract text and metadata from binary documents including
 * PDFs, Microsoft Office documents, HTML, plain text, and various other formats. The extracted
 * content and metadata are added to the ingest document for indexing.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Plugin registers the "attachment" processor type
 * // Use in an ingest pipeline:
 * PUT _ingest/pipeline/attachment
 * {
 *   "description": "Extract attachment information",
 *   "processors": [
 *     {
 *       "attachment": {
 *         "field": "data",
 *         "target_field": "attachment"
 *       }
 *     }
 *   ]
 * }
 * }</pre>
 */
public class IngestAttachmentPlugin extends Plugin implements IngestPlugin {

    /**
     * Returns a map of ingest processors provided by this plugin.
     * <p>
     * This method registers the attachment processor factory which creates processors
     * for parsing and extracting content from binary documents using Apache Tika.
     * </p>
     *
     * @param parameters the processor parameters (unused in this implementation)
     * @return an immutable map containing the "attachment" processor type and its factory
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called automatically by Elasticsearch during plugin initialization
     * Map<String, Processor.Factory> processors = plugin.getProcessors(parameters);
     * Processor.Factory attachmentFactory = processors.get("attachment");
     * }</pre>
     */
    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(AttachmentProcessor.TYPE, new AttachmentProcessor.Factory());
    }
}
