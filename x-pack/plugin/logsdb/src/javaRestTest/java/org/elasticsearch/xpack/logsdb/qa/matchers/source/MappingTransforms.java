/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa.matchers.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MappingTransforms {
    /**
     * Container for mapping of a field. Contains field mapping parameters and mapping parameters of parent fields (if present)
     * in order of increasing distance (direct parent first).
     * This is needed because some parent mapping parameters influence how source of the field is stored (e.g. `enabled: false`).
     * @param mappingParameters
     * @param parentMappingParameters
     */
    record FieldMapping(Map<String, Object> mappingParameters, List<Map<String, Object>> parentMappingParameters) {}

    /**
     * Normalize mapping to have the same structure as normalized source and enable field mapping lookup.
     * Similar to {@link SourceTransforms#normalize(Map)} but needs to get rid of intermediate nodes
     * and collect results into a different data structure.
     *
     * @param map raw mapping document converted to map
     * @return map from normalized field name (like a.b.c) to a map of mapping parameters (like type)
     */
    public static Map<String, FieldMapping> normalizeMapping(Map<String, Object> map) {
        var flattened = new HashMap<String, FieldMapping>();

        descend(null, map, flattened);

        return flattened;
    }

    @SuppressWarnings("unchecked")
    private static void descend(String pathFromRoot, Map<String, Object> currentLevel, Map<String, FieldMapping> flattened) {
        for (var entry : currentLevel.entrySet()) {
            if (entry.getKey().equals("_doc") || entry.getKey().equals("properties")) {
                descend(pathFromRoot, (Map<String, Object>) entry.getValue(), flattened);
            } else {
                if (entry.getValue() instanceof Map<?, ?> map) {
                    var pathToField = pathFromRoot == null ? entry.getKey() : pathFromRoot + "." + entry.getKey();

                    // Descending to subobject, we need to remember parent mapping
                    if (pathFromRoot != null) {
                        var parentMapping = flattened.computeIfAbsent(
                            pathFromRoot,
                            k -> new FieldMapping(new HashMap<>(), new ArrayList<>())
                        );
                        var childMapping = flattened.computeIfAbsent(
                            pathToField,
                            k -> new FieldMapping(new HashMap<>(), new ArrayList<>())
                        );
                        childMapping.parentMappingParameters.add(parentMapping.mappingParameters);
                        childMapping.parentMappingParameters.addAll(parentMapping.parentMappingParameters);
                    }

                    descend(pathToField, (Map<String, Object>) map, flattened);
                } else {
                    var pathToField = pathFromRoot == null ? "_doc" : pathFromRoot;
                    // We are either at the lowest level of mapping or it's a leaf field of top level object
                    flattened.computeIfAbsent(pathToField, k -> new FieldMapping(new HashMap<>(), new ArrayList<>())).mappingParameters.put(
                        entry.getKey(),
                        entry.getValue()
                    );
                }
            }
        }
    }
}
