/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.Nullable;

import java.util.Arrays;
import java.util.Map;

public final class MapHelper {

    private MapHelper() {}

    /**
     * This eagerly digs through the map by tokenizing the provided path on '.'.
     *
     * Only fully nested or fully collapsed fields are searched.
     * Examples
     *
     * {
     *     "a.b.c.d" : 2
     * }
     * {
     *     "a" :{"b": {"c": {"d" : 2}}}
     * }
     *
     * It is possible for ES _source docs to have "mixed" path formats.
     *
     * Meaning the following _sources (along with the above) are indexed the same:
     * {
     *     "a": {"b.c": {"d": 2}}
     * }
     * {
     *     "a.b.c": {"d": 2}
     * }
     *
     * To exhaustively explore all these paths would result in 2^n-1 total possible paths, where {@code n = path.split("\\.").length}
     *
     * This would result in an exponential runtime algorithm.
     *
     * NOTE: The default maximum field depth is 20. Meaning 524288 different ways of
     * nesting the same fields.
     *
     *
     * @param path Dot delimited path containing the field desired
     * @param map The {@link Map} map to dig
     * @return The found object. Returns {@code null} if not found
     */
    @Nullable
    public static Object dig(String path, Map<String, Object> map) {
        // short cut before search
        if (map.keySet().contains(path)) {
            return map.get(path);
        }
        String[] fields = path.split("\\.");
        if (Arrays.stream(fields).anyMatch(String::isEmpty)) {
            throw new IllegalArgumentException("Empty path detected. Invalid field name");
        }
        return explore(fields, map);
    }

    @SuppressWarnings("unchecked")
    private static Object explore(String[] path, Map<String, Object> map) {
        for(int i = 0; i < path.length - 1; i++) {
            if (map.get(path[i]) instanceof Map<?, ?> == false) {
                return null;
            }
            map = (Map<String, Object>) map.get(path[i]);
        }
        return map.get(path[path.length - 1]);
    }
}
