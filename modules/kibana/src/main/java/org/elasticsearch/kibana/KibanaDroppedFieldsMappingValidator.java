/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana;

import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Rejects regular put-mapping requests that introduce net-new fields (fields absent from the current live mapping) on
 * Kibana saved-objects system indices. New fields must go through
 * {@link TransportReplaceKibanaIndexMappingAction}, which runs a pre-flight check against the index's Lucene
 * FieldInfos to detect retired field names before the cluster-state update is committed.
 * <p>
 * Ordinary put-mapping is still permitted for updates to fields that already exist in the current mapping (adding
 * parameters such as {@code eager_global_ordinals} or changing analyser settings where allowed by the merge rules).
 */
public class KibanaDroppedFieldsMappingValidator implements RequestValidators.RequestValidator<PutMappingRequest> {

    @Override
    public Optional<Exception> validateRequest(PutMappingRequest request, ProjectMetadata projectMetadata, Index[] indices) {
        if (indices == null) {
            return Optional.empty();
        }
        for (Index index : indices) {
            if (KibanaPlugin.KIBANA_INDEX_DESCRIPTOR.matchesIndexPattern(index.getName()) == false) {
                continue;
            }
            IndexMetadata indexMetadata = projectMetadata.index(index);
            if (indexMetadata == null) {
                continue;
            }
            Set<String> currentFields = currentMappedFields(indexMetadata.mapping());
            Set<String> requestedFields = leafFieldPaths(request.source());
            for (String field : requestedFields) {
                if (currentFields.contains(field) == false) {
                    return Optional.of(
                        new IllegalArgumentException(
                            "field ["
                                + field
                                + "] is not present in the current mapping of ["
                                + index.getName()
                                + "]; use the Kibana replace-mappings API to introduce new fields, "
                                + "which performs a compatibility check against the index's Lucene field history"
                        )
                    );
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the set of all leaf field paths declared in the current live mapping. An absent or empty mapping
     * returns an empty set, causing every incoming field to be treated as net-new and therefore rejected.
     */
    private static Set<String> currentMappedFields(MappingMetadata mappingMetadata) {
        if (mappingMetadata == null) {
            return Set.of();
        }
        return leafFieldPaths(mappingMetadata.source().string());
    }

    /**
     * Extracts the flattened leaf field paths declared in a put-mapping source by walking its {@code properties}
     * tree (including multi-fields under {@code fields}). This deliberately avoids building mapper objects: for
     * the net-new check only the declared names are needed, not the resulting Lucene shapes.
     */
    static Set<String> leafFieldPaths(String mappingSource) {
        Set<String> paths = new HashSet<>();
        if (mappingSource == null) {
            return paths;
        }
        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, mappingSource, false);
        if (parsed.size() == 1
            && parsed.containsKey(MapperService.SINGLE_MAPPING_NAME)
            && parsed.get(MapperService.SINGLE_MAPPING_NAME) instanceof Map<?, ?> doc) {
            parsed = castMap(doc);
        }
        if (parsed.get("properties") instanceof Map<?, ?> properties) {
            collectLeafPaths("", castMap(properties), paths);
        }
        return paths;
    }

    private static void collectLeafPaths(String prefix, Map<String, Object> properties, Set<String> paths) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?> rawDefinition) {
                Map<String, Object> definition = castMap(rawDefinition);
                String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                if (definition.get("properties") instanceof Map<?, ?> subProperties) {
                    collectLeafPaths(path, castMap(subProperties), paths);
                } else {
                    paths.add(path);
                }
                if (definition.get("fields") instanceof Map<?, ?> multiFields) {
                    for (String multiField : castMap(multiFields).keySet()) {
                        paths.add(path + "." + multiField);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked") // XContent maps are always Map<String, Object>
    private static Map<String, Object> castMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }
}
