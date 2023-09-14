/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.aggregation;

import org.elasticsearch.search.profile.AbstractProfileBreakdown;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * {@linkplain AbstractProfileBreakdown} customized to work with aggregations.
 */
public class AggregationProfileBreakdown extends AbstractProfileBreakdown<AggregationTimingType> {
    private final Map<String, Object> extra = Collections.synchronizedMap(new HashMap<>());

    public AggregationProfileBreakdown() {
        super(AggregationTimingType.class);
    }

    /**
     * Add extra debugging information about the aggregation.
     */
    public synchronized void addDebugInfo(String key, Object value) {
        Object old = extra.put(key, value);
        if (old != null && value instanceof Integer) {
            extra.put(key, ((Integer) value + (Integer) old));
        }
        //assert old == null : "debug info duplicate key [" + key + "] was [" + old + "] is [" + value + "]";
    }

    @Override
    protected Map<String, Object> toDebugMap() {
        return unmodifiableMap(extra);
    }
}
