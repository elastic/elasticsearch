/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.logsdb.qa.matchers.source;

import java.util.HashMap;
import java.util.Map;

class MappingTransforms {
    /**
     * Normalize mapping to have the same structure as normalized source and enable field mapping lookup.
     * Similar to {@link SourceTransforms#normalize(Map)} but needs to get rid of intermediate nodes
     * and collect results into a different data structure.
     *
     * @param map raw mapping document converted to map
     * @return map from normalized field name (like a.b.c) to a map of mapping parameters (like type)
     */
    public static Map<String, Map<String, Object>> normalizeMapping(Map<String, Object> map) {
        var flattened = new HashMap<String, Map<String, Object>>();

        descend(null, map, flattened);

        return flattened;
    }

    @SuppressWarnings("unchecked")
    private static void descend(String pathFromRoot, Map<String, Object> currentLevel, Map<String, Map<String, Object>> flattened) {
        for (var entry : currentLevel.entrySet()) {
            if (entry.getKey().equals("_doc") || entry.getKey().equals("properties")) {
                descend(pathFromRoot, (Map<String, Object>) entry.getValue(), flattened);
            } else {
                if (entry.getValue() instanceof Map<?, ?> map) {
                    var pathToField = pathFromRoot == null ? entry.getKey() : pathFromRoot + "." + entry.getKey();
                    descend(pathToField, (Map<String, Object>) map, flattened);
                } else {
                    if (pathFromRoot == null) {
                        // Ignore top level mapping parameters for now
                        continue;
                    }

                    flattened.computeIfAbsent(pathFromRoot, k -> new HashMap<>()).put(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}
