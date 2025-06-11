/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.matchers.source;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

class SourceTransforms {
    /**
     * This preprocessing step makes it easier to match the document using a unified structure.
     * It performs following modifications:
     * <ul>
     * <li> Flattens all nested maps into top level map with full field path as key (e.g. "a.b.c.d") </li>
     * <li> Transforms all field values to arrays of length >= 1 </li>
     * </ul>
     * <p>
     * It also makes it possible to work with subobjects: false/auto settings.
     *
     * @return flattened map
     */
    public static Map<String, List<Object>> normalize(Map<String, Object> documentMap, Map<String, Map<String, Object>> mappingLookup) {
        var flattened = new TreeMap<String, List<Object>>();

        descend(null, documentMap, flattened, mappingLookup);

        return flattened;
    }

    public static <T> List<T> normalizeValues(List<T> values) {
        if (values == null) {
            return Collections.emptyList();
        }

        return normalizeValues(values, Function.identity());
    }

    public static <T, U> List<U> normalizeValues(List<T> values, Function<T, U> transform) {
        if (values == null) {
            return Collections.emptyList();
        }

        // Synthetic source modifications:
        // * null values are not present
        // * duplicates are removed
        return values.stream()
            .filter(v -> v != null && Objects.equals(v, "null") == false)
            .map(transform)
            .distinct()
            .collect(Collectors.toList());
    }

    private static void descend(
        String pathFromRoot,
        Map<String, Object> currentLevel,
        Map<String, List<Object>> flattened,
        Map<String, Map<String, Object>> mappingLookup
    ) {
        for (var entry : currentLevel.entrySet()) {
            var pathToCurrentField = pathFromRoot == null ? entry.getKey() : pathFromRoot + "." + entry.getKey();
            if (entry.getValue() instanceof List<?> list) {
                for (var fieldValue : list) {
                    handleField(pathToCurrentField, fieldValue, flattened, mappingLookup);
                }
            } else {
                handleField(pathToCurrentField, entry.getValue(), flattened, mappingLookup);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void handleField(
        String pathToCurrentField,
        Object currentField,
        Map<String, List<Object>> flattened,
        Map<String, Map<String, Object>> mappingLookup
    ) {
        var mapping = mappingLookup.get(pathToCurrentField);
        // Values of some fields are complex objects so we need to double-check that this is actually and object or a nested field
        // we can descend into.
        if (currentField instanceof Map<?, ?> map
            && (mapping == null || mapping.get("type").equals("object") || mapping.get("type").equals("nested"))) {
            descend(pathToCurrentField, (Map<String, Object>) map, flattened, mappingLookup);
        } else {
            flattened.computeIfAbsent(pathToCurrentField, k -> new ArrayList<>()).add(currentField);
        }
    }
}
