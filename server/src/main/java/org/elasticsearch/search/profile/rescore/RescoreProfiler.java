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
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Profiles a single rescorer. There is one instance per
 * {@link org.elasticsearch.search.rescore.RescoreContext} of a search request.
 * <p>
 * Each instance owns a dedicated {@link QueryProfiler}. While the rescorer runs, that profiler is installed on the shard's searcher so
 * that queries executed by the rescorer are reported as children of the rescore node instead of leaking, unlabeled, into the query tree
 * of the main query.
 */
public final class RescoreProfiler extends AbstractProfileBreakdown<RescoreTimingType> {

    private final String type;
    private final int windowSize;
    private final QueryProfiler queryProfiler = new QueryProfiler();
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
     * The profiler that must be set on the searcher while the rescorer runs.
     */
    public QueryProfiler getQueryProfiler() {
        return queryProfiler;
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

    @Override
    protected Map<String, Object> toDebugMap() {
        Map<String, Object> debug = new HashMap<>();
        debug.put("window_size", windowSize);
        debug.put("docs_before_rescore", docsBeforeRescore);
        if (timedOut) {
            debug.put("timed_out", true);
        } else {
            debug.put("docs_after_rescore", docsAfterRescore);
        }
        debug.put("rewrite_time", queryProfiler.getRewriteTime());
        return Collections.unmodifiableMap(debug);
    }

    /**
     * Build the result for this rescorer. Queries run by the rescorer are the children of the returned node.
     */
    public ProfileResult buildResult() {
        return new ProfileResult(type, "window_size=" + windowSize, toBreakdownMap(), toDebugMap(), toNodeTime(), queryProfiler.getTree());
    }
}
