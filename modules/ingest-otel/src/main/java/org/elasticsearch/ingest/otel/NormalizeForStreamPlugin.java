/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.otel;

import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Map;

/**
 * Plugin that provides the normalize_for_stream ingest processor for OpenTelemetry compatibility.
 * <p>
 * This plugin registers a processor that transforms non-OpenTelemetry-compliant documents into
 * a namespaced flavor of Elastic Common Schema (ECS) that is compatible with OpenTelemetry.
 * It renames specific ECS fields, namespaces attributes, and restructures resource attributes
 * to align with OpenTelemetry semantic conventions.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Plugin registers the "normalize_for_stream" processor type
 * // Use in an ingest pipeline:
 * PUT _ingest/pipeline/normalize_otel
 * {
 *   "description": "Normalize documents for OpenTelemetry compatibility",
 *   "processors": [
 *     {
 *       "normalize_for_stream": {}
 *     }
 *   ]
 * }
 * }</pre>
 */
public class NormalizeForStreamPlugin extends Plugin implements IngestPlugin {

    /**
     * Returns a map of ingest processors provided by this plugin.
     * <p>
     * This method registers the normalize_for_stream processor factory which creates
     * processors for transforming documents into OpenTelemetry-compatible format.
     * </p>
     *
     * @param parameters the processor parameters (unused in this implementation)
     * @return an immutable map containing the "normalize_for_stream" processor type and its factory
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called automatically by Elasticsearch during plugin initialization
     * Map<String, Processor.Factory> processors = plugin.getProcessors(parameters);
     * Processor.Factory factory = processors.get("normalize_for_stream");
     * }</pre>
     */
    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of(NormalizeForStreamProcessor.TYPE, new NormalizeForStreamProcessor.Factory());
    }
}
