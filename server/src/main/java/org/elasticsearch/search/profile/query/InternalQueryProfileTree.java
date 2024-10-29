/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.search.profile.AbstractInternalProfileTree;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.Timer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class tracks the dependency tree for queries (scoring and rewriting) and
 * generates {@link QueryProfileBreakdown} for each node in the tree.  It also finalizes the tree
 * and returns a list of {@link ProfileResult} that can be serialized back to the client
 */
final class InternalQueryProfileTree extends AbstractInternalProfileTree<QueryProfileBreakdown, Query> {

    /** Rewrite time */
    private final AtomicLong rewriteTime = new AtomicLong(0L);

    @Override
    protected QueryProfileBreakdown createProfileBreakdown() {
        return new QueryProfileBreakdown();
    }

    @Override
    protected String getTypeFromElement(Query query) {
        // Anonymous classes won't have a name,
        // we need to get the super class
        if (query.getClass().getSimpleName().isEmpty()) {
            return query.getClass().getSuperclass().getSimpleName();
        }
        return query.getClass().getSimpleName();
    }

    @Override
    protected String getDescriptionFromElement(Query query) {
        return query.toString();
    }

    /**
     * Begin timing a query for a specific Timing context and return the running timer
     */
    public Timer startRewriteTime() {
        Timer timer = new Timer();
        timer.start();
        return timer;
    }

    /**
     * Halt the timing process and add the elapsed rewriting time.
     * startRewriteTime() must be called for a particular context prior to calling
     * stopAndAddRewriteTime(), otherwise the elapsed time will be negative and
     * nonsensical
     *
     * @return          The elapsed time
     */
    public long stopAndAddRewriteTime(Timer timer) {
        timer.stop();
        assert timer.getCount() == 1L : "stopAndAddRewriteTime() called without a matching startRewriteTime()";
        long time = Math.max(1, timer.getApproximateTiming());
        return rewriteTime.addAndGet(time);
    }

    public long getRewriteTime() {
        return rewriteTime.get();
    }

}
