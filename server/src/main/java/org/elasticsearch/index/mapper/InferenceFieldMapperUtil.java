/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InferenceFieldMapperUtil {
    private InferenceFieldMapperUtil() {}

    /**
     * <p>
     * Insert or replace the path's value in the map with the provided new value. The map will be modified in-place.
     * If the complete path does not exist in the map, it will be added to the deepest (sub-)map possible.
     * </p>
     * <p>
     * For example, given the map:
     * </p>
     * <pre>
     * {
     *   "path1": {
     *     "path2": {
     *       "key1": "value1"
     *     }
     *   }
     * }
     * </pre>
     * <p>
     * And the caller wanted to insert {@code "path1.path2.path3.key2": "value2"}, the method would emit the modified map:
     * </p>
     * <pre>
     * {
     *   "path1": {
     *     "path2": {
     *       "key1": "value1",
     *       "path3.key2": "value2"
     *     }
     *   }
     * }
     * </pre>
     *
     * @param path the value's path in the map.
     * @param map the map to search and modify in-place.
     * @param newValue the new value to assign to the path.
     *
     * @throws IllegalArgumentException If either the path cannot be fully traversed or there is ambiguity about where to insert the new
     *                                  value.
     */
    public static void insertValue(String path, Map<?, ?> map, Object newValue) {
        String[] pathElements = path.split("\\.");
        if (pathElements.length == 0) {
            return;
        }

        List<SuffixMap> suffixMaps = extractSuffixMaps(pathElements, 0, map);
        if (suffixMaps.isEmpty()) {
            // This should never happen. Throw in case it does for some reason.
            throw new IllegalStateException("extractSuffixMaps returned an empty suffix map list");
        } else if (suffixMaps.size() == 1) {
            SuffixMap suffixMap = suffixMaps.getFirst();
            suffixMap.map().put(suffixMap.suffix(), newValue);
        } else {
            throw new IllegalArgumentException(
                "Path [" + path + "] could be inserted in " + suffixMaps.size() + " distinct ways, it is ambiguous which one to use"
            );
        }
    }

    private record SuffixMap(String suffix, Map<String, Object> map) {}

    private static List<SuffixMap> extractSuffixMaps(String[] pathElements, int index, Object currentValue) {
        if (currentValue instanceof List<?> valueList) {
            List<SuffixMap> suffixMaps = new ArrayList<>(valueList.size());
            for (Object o : valueList) {
                suffixMaps.addAll(extractSuffixMaps(pathElements, index, o));
            }

            return suffixMaps;
        } else if (currentValue instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) currentValue;
            List<SuffixMap> suffixMaps = new ArrayList<>(map.size());

            String key = pathElements[index];
            while (index < pathElements.length) {
                if (map.containsKey(key)) {
                    if (index + 1 == pathElements.length) {
                        // We found the complete path
                        suffixMaps.add(new SuffixMap(key, map));
                    } else {
                        // We've matched that path partially, keep traversing to try to match it fully
                        suffixMaps.addAll(extractSuffixMaps(pathElements, index + 1, map.get(key)));
                    }
                }

                if (++index < pathElements.length) {
                    key += "." + pathElements[index];
                }
            }

            if (suffixMaps.isEmpty()) {
                // We checked for all remaining elements in the path, and they do not exist. This means we found a leaf map that we should
                // add the value to.
                suffixMaps.add(new SuffixMap(key, map));
            }

            return suffixMaps;
        } else {
            throw new IllegalArgumentException(
                "Path ["
                    + String.join(".", Arrays.copyOfRange(pathElements, 0, index))
                    + "] has value ["
                    + currentValue
                    + "] of type ["
                    + currentValue.getClass().getSimpleName()
                    + "], which cannot be traversed into further"
            );
        }
    }
}
