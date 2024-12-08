/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file was contributed to by a Generative AI
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.transport.Transports;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SemanticTextInfoExtractor {
    private static final Logger logger = LogManager.getLogger(SemanticTextInfoExtractor.class);

    public static Set<String> extractIndexesReferencingInferenceEndpoints(Metadata metadata, Set<String> endpointIds) {
        assert Transports.assertNotTransportThread("non-trivial nested loops over cluster state structures");
        assert endpointIds.isEmpty() == false;
        assert metadata != null;

        Set<String> referenceIndices = new HashSet<>();

        Map<String, IndexMetadata> indices = metadata.indices();

        indices.forEach((indexName, indexMetadata) -> {
            Map<String, InferenceFieldMetadata> inferenceFields = indexMetadata.getInferenceFields();
            if (inferenceFields.values()
                .stream()
                .anyMatch(im -> endpointIds.contains(im.getInferenceId()) || endpointIds.contains(im.getSearchInferenceId()))) {
                referenceIndices.add(indexName);
            }
        });

        return referenceIndices;
    }
}
