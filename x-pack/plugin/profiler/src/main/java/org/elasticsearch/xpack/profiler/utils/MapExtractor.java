/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler.utils;

import java.util.Map;

public final class MapExtractor {
    private MapExtractor() {}

    @SuppressWarnings("unchecked")
    public static <T> T read(Map<String, Object> source, String... path) {
        Object value = null;
        if (source == null) {
            return null;
        }
        for (String pathElem : path) {
            if (source.containsKey(pathElem) == false) {
                return null;
            }
            value = source.get(pathElem);
            if (value == null) {
                return null;
            } else if (value instanceof Map) {
                source = (Map<String, Object>) value;
            }
        }
        return (T) value;
    }
}
