/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.common.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Stack;

public final class MapHelper {

    private MapHelper() {}

    /**
     * This eagerly digs (depth first search, longer keys first) through the map by tokenizing the provided path on '.'.
     *
     * It is possible for ES _source docs to have "mixed" path formats. So, we should search all potential paths
     * given the current knowledge of the map.
     *
     * Examples:
     *
     * The following maps would return `2` given the path "a.b.c.d"
     *
     * {
     *     "a.b.c.d" : 2
     * }
     * {
     *     "a" :{"b": {"c": {"d" : 2}}}
     * }
     * {
     *     "a" :{"b.c": {"d" : 2}}}
     * }
     * {
     *     "a" :{"b": {"c": {"d" : 2}}},
     *     "a.b" :{"c": {"d" : 5}} // we choose the first one found, we go down longer keys first
     * }
     * {
     *     "a" :{"b": {"c": {"NOT_d" : 2, "d": 2}}}
     * }
     *
     * Conceptual "Worse case" 5 potential paths explored for "a.b.c.d" until 2 is finally returned
     * {
     *     "a.b.c": {"not_d": 2},
     *     "a.b": {"c": {"not_d": 2}},
     *     "a": {"b.c": {"not_d": 2}},
     *     "a": {"b" :{ "c.not_d": 2}},
     *     "a" :{"b": {"c": {"not_d" : 2}}},
     *     "a" :{"b": {"c": {"d" : 2}}},
     * }
     *
     * We don't exhaustively create all potential paths.
     * If we did, this would result in 2^n-1 total possible paths, where {@code n = path.split("\\.").length}.
     *
     * Instead we lazily create potential paths once we know that they are possibilities.
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
        Stack<PotentialPath> pathStack = new Stack<>();
        pathStack.push(new PotentialPath(map, 0));
        return explore(fields, pathStack);
    }

    @SuppressWarnings("unchecked")
    private static Object explore(String[] path, Stack<PotentialPath> pathStack) {
        while (pathStack.empty() == false) {
            PotentialPath potentialPath = pathStack.pop();
            int endPos = potentialPath.pathPosition + 1;
            int startPos = potentialPath.pathPosition;
            Map<String, Object> map = potentialPath.map;
            String candidateKey = null;
            while(endPos <= path.length) {
                candidateKey = mergePath(path, startPos, endPos);
                Object next = map.get(candidateKey);
                if (endPos == path.length && next != null) { // exit early, we reached the full path and found something
                    return next;
                }
                if (next instanceof Map<?, ?>) { // we found another map, continue exploring down this path
                    pathStack.push(new PotentialPath((Map<String, Object>)next, endPos));
                }
                endPos++;
            }
            if (candidateKey != null && map.containsKey(candidateKey)) { //exit early
                return map.get(candidateKey);
            }
        }

        return null;
    }

    private static String mergePath(String[] path, int start, int end) {
        if (start + 1 == end) { // early exit, no need to create sb
            return path[start];
        }

        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end - 1; i++) {
            sb.append(path[i]);
            sb.append(".");
        }
        sb.append(path[end - 1]);
        return sb.toString();
    }

    private static class PotentialPath {

        // Pointer to where to start exploring
        private final Map<String, Object> map;
        // Where in the requested path are we
        private final int pathPosition;

        private PotentialPath(Map<String, Object> map, int pathPosition) {
            this.map = map;
            this.pathPosition = pathPosition;
        }

    }
}
