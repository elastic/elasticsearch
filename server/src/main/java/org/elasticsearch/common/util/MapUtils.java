/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.util.Map;

public class MapUtils {
    public static void recursiveMerge(Map<String, Object> target, Map<String, Object> from) {
        for (String key : from.keySet()) {
            if (target.containsKey(key)) {
                Object targetValue = target.get(key);
                Object fromValue = from.get(key);
                if (targetValue instanceof Map && fromValue instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> targetMap = (Map<String, Object>) targetValue;
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fromMap = (Map<String, Object>) fromValue;
                    recursiveMerge(targetMap, fromMap);
                } else {
                    target.put(key, fromValue);
                }
            } else {
                target.put(key, from.get(key));
            }
        }
    }
}
