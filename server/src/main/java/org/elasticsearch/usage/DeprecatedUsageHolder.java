/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.usage;

import org.elasticsearch.common.util.Maps;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public final class DeprecatedUsageHolder {
    DeprecatedUsageHolder() {}

    private final Map<String, LongAdder> usage = new ConcurrentHashMap<>();

    public void incrementDeprecationUsage(String deprecation_key) {
        usage.computeIfAbsent(deprecation_key, key -> new LongAdder()).increment();
    }

    public Map<String, Long> getDeprecatedUsageStats() {
        Map<String, Long> stats = Maps.newMapWithExpectedSize(usage.size());

        usage.forEach((deprecationKey, adder) -> stats.put(deprecationKey, adder.longValue()));
        return stats;
    }
}
