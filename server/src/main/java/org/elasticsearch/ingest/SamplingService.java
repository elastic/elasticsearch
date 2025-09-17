/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentParseException;

import java.util.Map;
import java.util.function.Supplier;

public class SamplingService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(SamplingService.class);
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    public SamplingService(ScriptService scriptService, ClusterService clusterService) {
        this.scriptService = scriptService;
        this.clusterService = clusterService;
    }

    /**
     * Potentially samples the given indexRequest, depending on the existing sampling configuration.
     * @param projectMetadata Used to get the sampling configuration
     * @param indexRequest The raw request to potentially sample
     */
    public void maybeSample(ProjectMetadata projectMetadata, IndexRequest indexRequest) {
        maybeSample(projectMetadata, indexRequest.index(), () -> indexRequest, () -> {
            Map<String, Object> sourceAsMap;
            try {
                sourceAsMap = indexRequest.sourceAsMap();
            } catch (XContentParseException e) {
                sourceAsMap = Map.of();
                logger.trace("Invalid index request source, attempting to sample anyway");
            }
            return new IngestDocument(
                indexRequest.index(),
                indexRequest.id(),
                indexRequest.version(),
                indexRequest.routing(),
                indexRequest.versionType(),
                sourceAsMap
            );
        });
    }

    /**
     *
     * @param projectMetadata Used to get the sampling configuration
     * @param indexRequestSupplier A supplier for the raw request to potentially sample
     * @param ingestDocument The IngestDocument used for evaluating any conditionals that are part of the sample configuration
     */
    public void maybeSample(
        ProjectMetadata projectMetadata,
        String indexName,
        Supplier<IndexRequest> indexRequestSupplier,
        IngestDocument ingestDocument
    ) {
        maybeSample(projectMetadata, indexName, indexRequestSupplier, () -> ingestDocument);
    }

    private void maybeSample(
        ProjectMetadata projectMetadata,
        String indexName,
        Supplier<IndexRequest> indexRequest,
        Supplier<IngestDocument> ingestDocumentSupplier
    ) {
        // TODO Sampling logic to go here in the near future
    }

    public boolean atLeastOneSampleConfigured() {
        return false; // TODO Return true if there is at least one sample in the cluster state
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO: React to sampling config changes
    }

}
