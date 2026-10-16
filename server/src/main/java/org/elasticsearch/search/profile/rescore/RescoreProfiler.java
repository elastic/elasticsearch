/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.rescore;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.profile.AbstractProfileBreakdown;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.Timer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Profiles a single rescorer. There is one instance per
 * {@link org.elasticsearch.search.rescore.RescoreContext} of a search request.
 * <p>
 * Queries executed by the rescorer are still recorded by the profiler of the main query, and tagged with the position of the rescorer.
 * Rescorers that do work the query profiler can't see, such as running a model, can report the time of each step in a breakdown of
 * their own with {@link #setBreakdown}, and other details with {@link #addDebugInfo}.
 */
public final class RescoreProfiler {

    private final String type;
    private final String description;
    private final int windowSize;
    private final Timer timer = new Timer();
    private final Map<String, Object> rescorerDebugInfo = new HashMap<>();
    @Nullable
    private AbstractProfileBreakdown<?> breakdown;
    private boolean timedOut;

    /**
     * @param type        name of the rescorer, for example {@code query}
     * @param description description of what the rescorer runs, for example its query
     * @param windowSize  the window size the rescorer was configured with
     */
    public RescoreProfiler(String type, String description, int windowSize) {
        this.type = type;
        this.description = description;
        this.windowSize = windowSize;
    }

    /**
     * Start timing the rescorer. The caller is responsible for stopping the returned timer.
     */
    public Timer startRescoreTimer() {
        timer.start();
        return timer;
    }

    /**
     * Record that the search timed out while the rescorer was running, so it did not return any top docs.
     */
    public void setTimedOut() {
        this.timedOut = true;
    }

    /**
     * Report the steps of the rescorer in the {@code breakdown} section of its profile. The steps should run while the rescorer is
     * timed, so that their times are part of the rescorer's time. Rescorers that don't set a breakdown report an empty one.
     */
    public void setBreakdown(AbstractProfileBreakdown<?> breakdown) {
        this.breakdown = breakdown;
    }

    /**
     * Add rescorer specific information to the {@code debug} section of this rescorer's profile. Values must be serializable with
     * {@link org.elasticsearch.common.io.stream.StreamOutput#writeGenericValue}.
     */
    public void addDebugInfo(String key, Object value) {
        rescorerDebugInfo.put(key, value);
    }

    Map<String, Object> toDebugMap() {
        Map<String, Object> debug = new HashMap<>(rescorerDebugInfo);
        debug.put("window_size", windowSize);
        if (timedOut) {
            debug.put("timed_out", true);
        }
        return Collections.unmodifiableMap(debug);
    }

    public ProfileResult buildResult() {
        Map<String, Long> breakdownMap = breakdown == null ? Map.of() : breakdown.toBreakdownMap();
        return new ProfileResult(type, description, breakdownMap, toDebugMap(), timer.getApproximateTiming(), List.of());
    }
}
