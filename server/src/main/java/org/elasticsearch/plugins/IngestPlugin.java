/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.ingest.Pipeline;
import org.elasticsearch.ingest.Processor;

import java.util.Map;
import java.util.Optional;

/**
 * An extension point for {@link Plugin} implementations to add custom ingest processors
 */
public interface IngestPlugin {

    /**
     * Returns additional ingest processor types added by this plugin.
     *
     * The key of the returned {@link Map} is the unique name for the processor which is specified
     * in pipeline configurations, and the value is a {@link org.elasticsearch.ingest.Processor.Factory}
     * to create the processor from a given pipeline configuration.
     */
    default Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Map.of();
    }

    default Optional<Pipeline> getIngestPipeline(IndexMetadata indexMetadata, Processor.Parameters parameters) {
        return Optional.empty();
    }
}
