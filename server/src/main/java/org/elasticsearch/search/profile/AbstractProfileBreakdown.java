/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 */
public abstract class AbstractProfileBreakdown<T extends Enum<T>> {

    /**
     * The accumulated timings for this query node
     */
    private final Timer[] timings;
    private final T[] timingTypes;

    /** Sole constructor. */
    public AbstractProfileBreakdown(Class<T> clazz) {
        this.timingTypes = clazz.getEnumConstants();
        timings = new Timer[timingTypes.length];
        for (int i = 0; i < timings.length; ++i) {
            timings[i] = new Timer();
        }
    }

    public Timer getTimer(T timing) {
        return timings[timing.ordinal()];
    }

    public void setTimer(T timing, Timer timer) {
        timings[timing.ordinal()] = timer;
    }

    /**
     * Build a timing count breakdown.
     */
    public final Map<String, Long> toBreakdownMap() {
        Map<String, Long> map = new HashMap<>(timings.length * 2);
        for (T timingType : timingTypes) {
            map.put(timingType.toString(), timings[timingType.ordinal()].getApproximateTiming());
            map.put(timingType.toString() + "_count", timings[timingType.ordinal()].getCount());
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Fetch extra debugging information.
     */
    protected Map<String, Object> toDebugMap() {
        return emptyMap();
    }

    public final long toNodeTime() {
        long total = 0;
        for (T timingType : timingTypes) {
            total += timings[timingType.ordinal()].getApproximateTiming();
        }
        return total;
    }
}
