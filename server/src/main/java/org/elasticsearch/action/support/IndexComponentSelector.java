/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum IndexComponentSelector {
    DATA("data"),
    FAILURES("failures");

    private final String key;

    IndexComponentSelector(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    private static final Map<String, IndexComponentSelector> REGISTRY;

    static {
        Map<String, IndexComponentSelector> registry = new HashMap<>(IndexComponentSelector.values().length);
        for (IndexComponentSelector value : IndexComponentSelector.values()) {
            registry.put(value.getKey(), value);
        }
        REGISTRY = Collections.unmodifiableMap(registry);
    }

    public static IndexComponentSelector getByKey(String key) {
        return REGISTRY.get(key);
    }
}
