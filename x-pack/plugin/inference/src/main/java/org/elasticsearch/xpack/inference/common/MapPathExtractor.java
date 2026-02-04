/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Extracts fields from a {@link Map}.
 *
 * Uses a subset of the JSONPath schema to extract fields from a map.
 * For more information <a href="https://en.wikipedia.org/wiki/JSONPath">see here</a>.
 *
 * This implementation differs in how it handles lists in that JSONPath will flatten inner lists. This implementation
 * preserves inner lists.
 *
 * Examples of the schema:
 *
 * <pre>
 * {@code
 * $.field1.array[*].field2
 * $.field1.field2
 * }
 * </pre>
 *
 * Given the map
 * <pre>
 * {@code
 * {
 *     "request_id": "B4AB89C8-B135-xxxx-A6F8-2BAB801A2CE4",
 *     "latency": 38,
 *     "usage": {
 *         "token_count": 3072
 *     },
 *     "result": {
 *         "embeddings": [
 *             {
 *                 "index": 0,
 *                 "embedding": [
 *                     2,
 *                     4
 *                 ]
 *             },
 *             {
 *                 "index": 1,
 *                 "embedding": [
 *                     1,
 *                     2
 *                 ]
 *             }
 *         ]
 *     }
 * }
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * var embeddings = MapPathExtractor.extract(map, "$.result.embeddings[*].embedding");
 * }
 * </pre>
 *
 * Will result in:
 *
 * <pre>
 * {@code
 * [
 *   [2, 4],
 *   [1, 2]
 * ]
 * }
 *
 * The array field names would be {@code ["embeddings", "embedding"}
 * </pre>
 *
 * This implementation differs from JSONPath when handling a list of maps. JSONPath will flatten the result and return a single array.
 * this implementation will preserve each nested list while gathering the results.
 *
 * For example
 *
 * <pre>
 * {@code
 * {
 *   "result": [
 *     {
 *       "key": [
 *         {
 *           "a": 1.1
 *         },
 *         {
 *           "a": 2.2
 *         }
 *       ]
 *     },
 *     {
 *       "key": [
 *         {
 *           "a": 3.3
 *         },
 *         {
 *           "a": 4.4
 *         }
 *       ]
 *     }
 *   ]
 * }
 * }
 * {@code var embeddings = MapPathExtractor.extract(map, "$.result[*].key[*].a");}
 *
 * JSONPath: {@code [1.1, 2.2, 3.3, 4.4]}
 * This implementation: {@code [[1.1, 2.2], [3.3, 4.4]]}
 * </pre>
 */
public class MapPathExtractor {

    private static final String DOLLAR = "$";

    // default for testing
    static final Pattern DOT_FIELD_PATTERN = Pattern.compile("^\\.([^.\\[]+)(.*)");
    static final Pattern ARRAY_WILDCARD_PATTERN = Pattern.compile("^\\[\\*\\](.*)");
    public static final String UNKNOWN_FIELD_NAME = "unknown";

    /**
     * A result object that tries to match up the field names parsed from the passed in path and the result
     * extracted from the passed in map.
     * @param extractedObject represents the extracted result from the map
     * @param traversedFields a list of field names in order as they're encountered while navigating through the nested objects
     */
    public record Result(Object extractedObject, List<String> traversedFields) {
        public String getArrayFieldName(int index) {
            // if the index is out of bounds we'll return a default value
            if (traversedFields.size() <= index || index < 0) {
                return UNKNOWN_FIELD_NAME;
            }

            return traversedFields.get(index);
        }
    }

    public static Result extract(Map<String, Object> data, String path) {
        if (data == null || data.isEmpty() || path == null || path.trim().isEmpty()) {
            return null;
        }

        var cleanedPath = path.trim();

        if (cleanedPath.startsWith(DOLLAR)) {
            cleanedPath = cleanedPath.substring(DOLLAR.length());
        } else {
            throw new IllegalArgumentException(Strings.format("Path [%s] must start with a dollar sign ($)", cleanedPath));
        }

        var fieldNames = new LinkedHashSet<String>();

        return new Result(navigate(data, cleanedPath, new FieldNameInfo("", "", fieldNames)), fieldNames.stream().toList());
    }

    private record FieldNameInfo(String currentPath, String fieldName, Set<String> traversedFields) {
        void addTraversedField(String fieldName) {
            traversedFields.add(createPath(fieldName));
        }

        void addCurrentField() {
            traversedFields.add(currentPath);
        }

        FieldNameInfo descend(String newFieldName) {
            var newLocation = createPath(newFieldName);
            return new FieldNameInfo(newLocation, newFieldName, traversedFields);
        }

        private String createPath(String newFieldName) {
            if (Strings.isNullOrEmpty(currentPath)) {
                return newFieldName;
            } else {
                return currentPath + "." + newFieldName;
            }
        }
    }

    private static Object navigate(Object current, String remainingPath, FieldNameInfo fieldNameInfo) {
        if (current == null || Strings.isNullOrEmpty(remainingPath)) {
            return current;
        }

        var dotFieldMatcher = DOT_FIELD_PATTERN.matcher(remainingPath);
        var arrayWildcardMatcher = ARRAY_WILDCARD_PATTERN.matcher(remainingPath);

        if (dotFieldMatcher.matches()) {
            String field = dotFieldMatcher.group(1);
            if (field == null || field.isEmpty()) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Unable to extract field from remaining path [%s]. Fields must be delimited by a dot character.",
                        remainingPath
                    )
                );
            }

            String nextPath = dotFieldMatcher.group(2);
            if (current instanceof Map<?, ?> currentMap) {
                var fieldFromMap = currentMap.get(field);
                if (fieldFromMap == null) {
                    throw new IllegalArgumentException(Strings.format("Unable to find field [%s] in map", field));
                }

                // Handle the case where the path was $.result.text or $.result[*].key
                if (Strings.isNullOrEmpty(nextPath)) {
                    fieldNameInfo.addTraversedField(field);
                }

                return navigate(currentMap.get(field), nextPath, fieldNameInfo.descend(field));
            } else {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Current path [%s] matched the dot field pattern but the current object is not a map, "
                            + "found invalid type [%s] instead.",
                        remainingPath,
                        current.getClass().getSimpleName()
                    )
                );
            }
        } else if (arrayWildcardMatcher.matches()) {
            String nextPath = arrayWildcardMatcher.group(1);
            if (current instanceof List<?> list) {
                fieldNameInfo.addCurrentField();

                List<Object> results = new ArrayList<>();

                for (Object item : list) {
                    Object result = navigate(item, nextPath, fieldNameInfo);
                    if (result != null) {
                        results.add(result);
                    }
                }

                return results;
            } else {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Current path [%s] matched the array field pattern but the current object is not a list, "
                            + "found invalid type [%s] instead.",
                        remainingPath,
                        current.getClass().getSimpleName()
                    )
                );
            }
        }

        throw new IllegalArgumentException(Strings.format("Invalid path received [%s], unable to extract a field name.", remainingPath));
    }
}
