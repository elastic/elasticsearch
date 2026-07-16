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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Rejects regular put-mapping requests that touch a field recorded in the
 * {@link TransportReplaceKibanaIndexMappingAction#DROPPED_FIELDS_METADATA_KEY} tombstones of a Kibana system index.
 * Without this, the ordinary additive put-mapping API would bypass the tombstone guardrail entirely: it sees no
 * mapping entry for a dropped field and happily re-adds it under any type, re-creating the shard-level Lucene shape
 * conflict the tombstones exist to prevent. Re-introductions (which are legal when the type is unchanged) must go
 * through the replace-mappings action, which can validate the type and clear the tombstone atomically.
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
            Map<String, String> tombstones = indexMetadata.getCustomData(
                TransportReplaceKibanaIndexMappingAction.DROPPED_FIELDS_METADATA_KEY
            );
            if (tombstones == null || tombstones.isEmpty()) {
                continue;
            }
            Set<String> requestedFields = leafFieldPaths(request.source());
            for (String field : requestedFields) {
                String droppedType = tombstones.get(field);
                if (droppedType != null) {
                    return Optional.of(
                        new IllegalArgumentException(
                            "field ["
                                + field
                                + "] of ["
                                + index.getName()
                                + "] was previously dropped (as type ["
                                + droppedType
                                + "]) and cannot be modified via the put mapping API; re-introduce it with the same type"
                                + " via the Kibana replace mappings API, or use a new (versioned) field name"
                        )
                    );
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Extracts the flattened leaf field paths declared in a put-mapping source by walking its {@code properties}
     * tree (including multi-fields under {@code fields}). This deliberately avoids building mapper objects: for
     * tombstone matching only the declared names are needed, not the resulting Lucene shapes.
     */
    static Set<String> leafFieldPaths(String mappingSource) {
        Set<String> paths = new HashSet<>();
        if (mappingSource == null) {
            return paths;
        }
        Map<String, Object> parsed = XContentHelper.convertToMap(JsonXContent.jsonXContent, mappingSource, false);
        if (parsed.size() == 1 && parsed.containsKey("_doc") && parsed.get("_doc") instanceof Map<?, ?> doc) {
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
