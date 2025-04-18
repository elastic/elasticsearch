/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    static final Pattern dotFieldPattern = Pattern.compile("^\\.([^.\\[]+)(.*)");
    static final Pattern arrayWildcardPattern = Pattern.compile("^\\[\\*\\](.*)");

    public static Object extract(Map<String, Object> data, String path) {
        if (data == null || data.isEmpty() || path == null || path.trim().isEmpty()) {
            return null;
        }

        var cleanedPath = path.trim();

        if (cleanedPath.startsWith(DOLLAR)) {
            cleanedPath = cleanedPath.substring(DOLLAR.length());
        } else {
            throw new IllegalArgumentException(Strings.format("Path [%s] must start with a dollar sign ($)", cleanedPath));
        }

        return navigate(data, cleanedPath);
    }

    private static Object navigate(Object current, String remainingPath) {
        if (current == null || remainingPath == null || remainingPath.isEmpty()) {
            return current;
        }

        var dotFieldMatcher = dotFieldPattern.matcher(remainingPath);
        var arrayWildcardMatcher = arrayWildcardPattern.matcher(remainingPath);

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

                return navigate(currentMap.get(field), nextPath);
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
                List<Object> results = new ArrayList<>();

                for (Object item : list) {
                    Object result = navigate(item, nextPath);
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
