/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.util.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
    private final Map<Integer, List<Timer>> timings;
    private final T[] timingTypes;

    /** Sole constructor. */
    public AbstractProfileBreakdown(Class<T> clazz) {
        this.timingTypes = clazz.getEnumConstants();
        timings = new HashMap<>(this.timingTypes.length);
        for (int i = 0; i < this.timingTypes.length; ++i) {
            ArrayList<Timer> listOfTimers = new ArrayList<>();
            listOfTimers.add(new Timer());
            timings.put(i, listOfTimers);
        }
    }

    /**
     * @param timingType the timing type to create a new {@link Timer} for
     * @return a new {@link Timer} instance
     */
    public Timer getNewTimer(T timingType) {
        Timer timer = new Timer();
        timings.get(timingType.ordinal()).add(timer);
        return timer;
    }

    /**
     * Build a timing count breakdown.
     * If multiple timers where requested from different locations or threads from this profile breakdown,
     * the approximation will contain the sum of each timers approximate time and count.
     */
    public final Map<String, Long> toBreakdownMap() {
        Map<String, Long> map = Maps.newMapWithExpectedSize(timings.keySet().size() * 2);
        for (T timingType : this.timingTypes) {
            map.put(
                timingType.toString(),
                timings.get(timingType.ordinal()).stream().map(Timer::getApproximateTiming).mapToLong(Long::valueOf).sum()
            );
            map.put(
                timingType.toString() + "_count",
                timings.get(timingType.ordinal()).stream().map(Timer::getCount).mapToLong(Long::valueOf).sum()
            );
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Fetch extra debugging information.
     */
    protected Map<String, Object> toDebugMap() {
        return emptyMap();
    }

    /**
     * @return the total sum of timers approximate times across all timing types
     */
    public final long toNodeTime() {
        long total = 0;
        for (T timingType : timingTypes) {
            total += timings.get(timingType.ordinal()).stream().map(Timer::getApproximateTiming).mapToLong(Long::valueOf).sum();
        }
        return total;
    }
}
