/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.rescore;

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
 * Queries executed by the rescorer are still recorded by the profiler of the main query. Rescorers that do work the query profiler
 * can't see, such as running a model, can report it with {@link #addDebugInfo}.
 */
public final class RescoreProfiler extends AbstractProfileBreakdown<RescoreTimingType> {

    private final String type;
    private final int windowSize;
    private final Map<String, Object> rescorerDebugInfo = new HashMap<>();
    private int docsBeforeRescore;
    private int docsAfterRescore;
    private boolean timedOut;

    /**
     * @param type       name of the rescorer, for example {@code query}
     * @param windowSize the window size the rescorer was configured with
     */
    public RescoreProfiler(String type, int windowSize) {
        super(RescoreTimingType.class);
        this.type = type;
        this.windowSize = windowSize;
    }

    /**
     * Start timing the rescorer. The caller is responsible for stopping the returned timer.
     */
    public Timer startRescoreTimer() {
        Timer timer = getNewTimer(RescoreTimingType.RESCORE);
        timer.start();
        return timer;
    }

    /**
     * Record the number of top docs handed to the rescorer.
     */
    public void setDocsBeforeRescore(int docsBeforeRescore) {
        this.docsBeforeRescore = docsBeforeRescore;
    }

    /**
     * Record the number of top docs returned by the rescorer.
     */
    public void setDocsAfterRescore(int docsAfterRescore) {
        this.docsAfterRescore = docsAfterRescore;
    }

    /**
     * Record that the search timed out while the rescorer was running, so it did not return any top docs.
     */
    public void setTimedOut() {
        this.timedOut = true;
    }

    /**
     * Add rescorer specific information to the {@code debug} section of this rescorer's profile, for example the time spent in each
     * step of the rescorer. Values must be serializable with
     * {@link org.elasticsearch.common.io.stream.StreamOutput#writeGenericValue}.
     */
    public void addDebugInfo(String key, Object value) {
        rescorerDebugInfo.put(key, value);
    }

    @Override
    protected Map<String, Object> toDebugMap() {
        Map<String, Object> debug = new HashMap<>(rescorerDebugInfo);
        debug.put("window_size", windowSize);
        debug.put("docs_before_rescore", docsBeforeRescore);
        if (timedOut) {
            debug.put("timed_out", true);
        } else {
            debug.put("docs_after_rescore", docsAfterRescore);
        }
        return Collections.unmodifiableMap(debug);
    }

    public ProfileResult buildResult() {
        return new ProfileResult(type, "rescore top documents", toBreakdownMap(), toDebugMap(), toNodeTime(), List.of());
    }
}
