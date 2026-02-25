/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.inference.mapper.SemanticTextField;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SemanticTextInfoExtractor {
    public static Set<String> extractIndexesReferencingInferenceEndpoints(Metadata metadata, Set<String> endpointIds) {
        assert Transports.assertNotTransportThread("non-trivial nested loops over cluster state structures");
        assert endpointIds.isEmpty() == false;
        assert metadata != null;

        Set<String> referenceIndices = new HashSet<>();

        Map<String, IndexMetadata> indices = metadata.getProject().indices();

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

    public static Map<String, MinimalServiceSettings> getModelSettingsForIndicesReferencingInferenceEndpoints(
        Metadata metadata,
        Set<String> endpointIds
    ) {
        assert Transports.assertNotTransportThread("non-trivial nested loops over cluster state structures");
        assert endpointIds.isEmpty() == false;
        assert metadata != null;

        Map<String, MinimalServiceSettings> serviceSettingsMap = new HashMap<>();

        metadata.getProject().indices().forEach((indexName, indexMetadata) -> {
            indexMetadata.getInferenceFields()
                .values()
                .stream()
                .filter(field -> endpointIds.contains(field.getInferenceId()) || endpointIds.contains(field.getSearchInferenceId()))
                .findFirst() // Assume that the model settings are the same for all fields using the inference endpoint
                .ifPresent(field -> {
                    MappingMetadata mapping = indexMetadata.mapping();
                    if (mapping != null) {
                        String[] pathArray = { ElasticsearchMappings.PROPERTIES, field.getName(), SemanticTextField.MODEL_SETTINGS_FIELD };
                        Object modelSettings = ObjectPath.eval(pathArray, mapping.sourceAsMap());
                        serviceSettingsMap.put(indexName, SemanticTextField.parseModelSettingsFromMap(modelSettings));
                    }
                });
        });

        return serviceSettingsMap;
    }
}
