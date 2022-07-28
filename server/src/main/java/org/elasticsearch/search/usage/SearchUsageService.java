/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.usage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class SearchUsageService {
    private final Map<String, LongAdder> queriesUsage;

    public static class Builder {
        private final Map<String, LongAdder> queriesUsage;

        public Builder() {
            queriesUsage = new HashMap<>();
        }

        public void registerQuery(String queryName) {
            if (queriesUsage.put(queryName, new LongAdder()) != null) {
                throw new IllegalArgumentException("stats for the query type [" + queryName + "] already registered!");
            }
        }

        public SearchUsageService build() {
            return new SearchUsageService(this);
        }
    }

    private SearchUsageService(Builder builder) {
        this.queriesUsage = Collections.unmodifiableMap(builder.queriesUsage);
    }

    public void incrementQueryUsage(String query) {
        LongAdder adder = queriesUsage.get(query);
        // Not all queries register their usage at the moment
        if (adder != null) {
            adder.increment();
        }
    }

    public Map<String, Long> getUsageStats() {
        Map<String, Long> queriesUsageMap = new HashMap<>();
        queriesUsage.forEach((query, adder) -> {
            long usageCount = adder.longValue();
            if (usageCount > 0) {
                queriesUsageMap.put(query, usageCount);
            }
        });
        return queriesUsageMap;
    }

}
